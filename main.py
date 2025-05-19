#!/usr/bin/env python3
import argparse
from flask import Flask, request, jsonify, Response
from flask_restx import Api, Resource, fields
from flask_caching import Cache
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_compress import Compress
from prometheus_client import make_wsgi_app
from werkzeug.middleware.dispatcher import DispatcherMiddleware
import structlog
from urllib.parse import unquote, urlencode
from typing import List, Dict, Any
import os
from io import StringIO
import tempfile
from datetime import datetime

from crawler import PrecatoriosCrawler
from entity_mapping_crawler import EntityMappingCrawler
from config import config, field_config
from logger import configure_logging, get_logger
from models import Precatorio, PrecatorioResponse, EntityMapping, EntidadeResponse, HealthCheckResponse, FetchPrecatoriosQuery
from metrics import track_time
from entity_mapping import get_api_entity_name, get_entity_slug, validate_entity_slug
from pinata_uploader import upload_and_get_pinata_url

# Configuração do logging
configure_logging()
logger = get_logger(__name__)

# Configuração da API
app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False  # Permite caracteres UTF-8 no JSON
app.config["RESTX_JSON"] = {
    "ensure_ascii": False
}  # Configura o flask-restx para não escapar caracteres UTF-8
app.config["CACHE_TYPE"] = "SimpleCache"
app.config["CACHE_DEFAULT_TIMEOUT"] = config.cache_default_timeout
app.config["RATELIMIT_DEFAULT"] = config.rate_limit_default
app.config["RATELIMIT_STRATEGY"] = "fixed-window"

# Configuração do rate limiting
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://",
)

# Configuração da compressão
compress = Compress()
compress.init_app(app)

api = Api(
    app,
    version="1.0",
    title="API de Precatórios",
    description="API para busca de precatórios do TJCE",
    doc='/api/docs',
)

# Criação do namespace
ns = api.namespace("api", description="Operações da API de Precatórios")

# Configuração do cache
cache = Cache(app)

# Configuração do Swagger UI para Flask-RESTX
authorizations = {
    'apikey': {
        'type': 'apiKey',
        'in': 'header',
        'name': 'X-API-KEY'
    }
}

# Modelos Flask-RESTX para documentação e marshalling
health_model_fields = api.model('Health', {
    'status': fields.String(required=True, description='Status da saúde da aplicação.'),
    'message': fields.String(required=True, description='Mensagem sobre a saúde da aplicação.')
})

entity_mapping_item_fields = api.model('EntityMappingItem', {
    'official_name': fields.String(required=True, description='Nome oficial da entidade como retornado pela fonte de dados.'),
    'slug': fields.String(required=True, description='Slug normalizado da entidade para uso na API.')
})

entity_model_response_fields = api.model('EntidadeResponse', {
    'status': fields.String(required=True, description='Status da resposta.'),
    'message': fields.String(required=True, description='Mensagem descritiva da resposta.'),
    'data': fields.List(fields.Nested(entity_mapping_item_fields), description='Lista de mapeamentos de entidades.', allow_null=True),
    'pinata_url': fields.String(description='URL do arquivo CSV de entidades no Pinata, se o upload for bem-sucedido.', allow_null=True)
})

fetch_query_model_fields = api.model('FetchPrecatoriosQuery', {
    'entity': fields.String(required=True, description='Slug da entidade para buscar precatórios. Ex: municipio-de-fortaleza', example='municipio-de-fortaleza'),
    'count': fields.Integer(description='Número de registros a serem retornados. Se não fornecido, busca todos.', example=10, allow_null=True)
})

precatorio_item_fields = api.model('PrecatorioItem', {
    'ordem': fields.Integer(required=True, description='Número de ordem do precatório.'),
    'processo': fields.String(required=True, description='Número do processo.'),
    'comarca': fields.String(description='Comarca do precatório.', default='-'),
    'ano_orcamento': fields.Integer(required=True, description='Ano do orçamento.'),
    'natureza': fields.String(description='Natureza do precatório (ex: ALIMENTAR).', default='-'),
    'data_cadastro': fields.DateTime(description='Data de cadastro do precatório.', dt_format='iso8601', allow_null=True),
    'tipo_classificacao': fields.String(description='Tipo de classificação do precatório.', default='-'),
    'valor_original': fields.String(description='Valor original do precatório formatado como string.'), # Usando String para Decimals formatados
    'valor_atual': fields.String(description='Valor atualizado do precatório formatado como string.'), # Usando String para Decimals formatados
    'situacao': fields.String(description='Situação atual do precatório.', default='-')
})

precatorio_response_model_fields = api.model('PrecatorioResponse', {
    'status': fields.String(required=True, description='Status da resposta.'),
    'message': fields.String(required=True, description='Mensagem descritiva da resposta.'),
    'data': fields.List(fields.Nested(precatorio_item_fields), description='Lista de precatórios encontrados.', allow_null=True),
    'pinata_url': fields.String(description='URL do arquivo CSV de precatórios no Pinata, se o upload for bem-sucedido.', allow_null=True)
})

# Instância do crawler
crawler = PrecatoriosCrawler()

def apply_filters(rows: List[Dict[str, Any]], **filters) -> List[Dict[str, Any]]:
    """Aplica filtros aos dados"""
    filtered = rows.copy()

    # Filtro por ano
    if filters.get("ano_min"):
        filtered = [
            r for r in filtered if r["ano_orcamento"] >= int(filters["ano_min"])
        ]
    if filters.get("ano_max"):
        filtered = [
            r for r in filtered if r["ano_orcamento"] <= int(filters["ano_max"])
        ]

    # Filtro por valor
    if filters.get("valor_min"):
        filtered = [
            r for r in filtered if r["valor_atual"] >= float(filters["valor_min"])
        ]
    if filters.get("valor_max"):
        filtered = [
            r for r in filtered if r["valor_atual"] <= float(filters["valor_max"])
        ]

    # Filtro por natureza
    if filters.get("natureza"):
        filtered = [
            r for r in filtered if r["natureza"].lower() == filters["natureza"].lower()
        ]

    return filtered

def sort_rows(
    rows: List[Dict[str, Any]], sort_by: str = None, order: str = "asc"
) -> List[Dict[str, Any]]:
    """Ordena os dados"""
    if not sort_by:
        return rows

    reverse = order.lower() == "desc"
    return sorted(rows, key=lambda x: x[sort_by], reverse=reverse)

@ns.route("/health")
class HealthCheck(Resource):
    @api.marshal_with(health_model_fields)
    def get(self):
        """Verifica a saúde da aplicação."""
        logger.info("Health check endpoint chamado.")
        return HealthCheckResponse(status="OK", message="API está operacional")

@ns.route("/entities")
class Entidades(Resource):
    @cache.cached(timeout=config.cache_timeout_entities)
    @limiter.limit(config.rate_limit_entities)
    @api.marshal_with(entity_model_response_fields)
    def get(self):
        """Lista todas as entidades disponíveis para consulta de precatórios."""
        logger.info("Endpoint /entities chamado")
        crawler = EntityMappingCrawler()
        output_filename = config.entities_output_filename
        output_dir = os.path.join(os.getcwd(), 'data') # Salva na pasta 'data'
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, output_filename)

        try:
            logger.info(f"Buscando entidades e salvando em {output_path}")
            # A lógica de buscar e salvar entidades já deve existir em EntityMappingCrawler
            # Vamos assumir que ele cria o arquivo CSV em output_path
            entities_data = crawler.get_and_save_entities(output_path)

            if not entities_data:
                logger.warning("Nenhuma entidade encontrada.")
                # Corrigido para retornar dicionário
                return EntidadeResponse(status="warning", message="Nenhuma entidade encontrada.", data=[]).dict(), 200
            
            pinata_url = None
            logger.info(f"Verificando condições para upload no Pinata (Entidades): JWT_EXISTS={bool(config.pinata_api_jwt)}, FILE_EXISTS={os.path.exists(output_path)}")
            if config.pinata_api_jwt and os.path.exists(output_path):
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                pinata_file_name = f"entidades_tjce_{timestamp}.csv"
                logger.info(f"Tentando upload de {output_path} para Pinata como {pinata_file_name}")
                pinata_url = upload_and_get_pinata_url(
                    local_file_path=output_path,
                    file_name_for_pinata=pinata_file_name,
                    pinata_jwt=config.pinata_api_jwt,
                    pinata_metadata={"type": "entities_mapping"}
                )
                if pinata_url:
                    logger.info(f"Upload para Pinata bem-sucedido: {pinata_url}")
                else:
                    logger.warning("Falha no upload do arquivo de entidades para o Pinata.")

            response_data = EntidadeResponse(
                status="success", 
                message="Entidades listadas com sucesso.", 
                data=entities_data,
                pinata_url=pinata_url
            )
            return response_data.dict(), 200 # Corrigido para retornar dicionário
        
        except Exception as e:
            logger.error(f"Erro ao buscar entidades: {e}", exc_info=True)
            # Corrigido para retornar dicionário
            return EntidadeResponse(status="error", message=f"Erro interno: {str(e)}", data=[]).dict(), 500

@ns.route("/fetch")
class FetchPrecatorios(Resource):
    @limiter.limit(config.rate_limit_fetch)
    @api.expect(fetch_query_model_fields)
    @api.marshal_with(precatorio_response_model_fields) 
    def get(self):
        """Busca e normaliza precatórios para uma entidade específica."""
        args = request.args
        entity_slug = args.get('entity')
        count_str = args.get('count')
        
        count = None
        if count_str:
            try:
                count = int(count_str)
                if count <= 0:
                    logger.warning("Parâmetro 'count' inválido, deve ser um inteiro positivo.")
                    # Corrigido para usar .dict()
                    return PrecatorioResponse(status="error", message="Parâmetro 'count' inválido.", data=[]).dict(), 400
            except ValueError:
                logger.warning("Parâmetro 'count' inválido, deve ser um inteiro.")
                # Corrigido para usar .dict()
                return PrecatorioResponse(status="error", message="Parâmetro 'count' inválido.", data=[]).dict(), 400

        if not entity_slug:
            logger.warning("Parâmetro 'entity' é obrigatório.")
            # Corrigido para usar .dict()
            return PrecatorioResponse(status="error", message="Parâmetro 'entity' é obrigatório.", data=[]).dict(), 400

        logger.info(f"Endpoint /fetch chamado para entidade: {entity_slug}, count: {count}")
        crawler = PrecatoriosCrawler()
        
        # Usar um arquivo temporário para o CSV
        temp_dir = tempfile.mkdtemp()
        timestamp_file = datetime.now().strftime("%Y%m%d%H%M%S")
        # Sanitizar entity_slug para nome de arquivo
        safe_entity_slug = entity_slug.replace(" ", "_").replace("/", "_")
        temp_csv_filename = f"precatorios_{safe_entity_slug}_{timestamp_file}.csv"
        temp_csv_path = os.path.join(temp_dir, temp_csv_filename)
        
        logger.info(f"Arquivo CSV temporário será salvo em: {temp_csv_path}")

        try:
            raw_data = crawler.fetch_data(entity_slug, count=count) # Passa o count para fetch_data
            if not raw_data:
                logger.warning(f"Nenhum dado bruto encontrado para {entity_slug}")
                # Limpa o diretório temporário
                try:
                    os.remove(temp_csv_path) # Remove o arquivo se existir (improvável)
                    os.rmdir(temp_dir) 
                except OSError as e_clean:
                    logger.warning(f"Erro ao limpar diretório temporário {temp_dir}: {e_clean}")
                # Corrigido para usar .dict() e remover count
                return PrecatorioResponse(status="warning", message="Nenhum dado encontrado para a entidade.", data=[]).dict(), 200

            # raw_data é um único dicionário da resposta da API.
            # normalize_to_rows espera uma lista de dicionários (um por página de resposta)
            rows = crawler.normalize_to_rows([raw_data]) # Envolver raw_data em uma lista
            
            if not rows:
                logger.warning(f"Nenhum dado normalizado para {entity_slug}. O arquivo CSV não será gerado ou estará vazio.")
                crawler.write_csv([], temp_csv_path) # Escreve CSV vazio com cabeçalhos
            else:
                crawler.write_csv(rows, temp_csv_path)
                logger.info(f"{len(rows)} registros normalizados e salvos em {temp_csv_path} para {entity_slug}")

            pinata_url = None
            logger.info(f"Verificando condições para upload no Pinata (Precatórios): JWT_EXISTS={bool(config.pinata_api_jwt)}, FILE_EXISTS={os.path.exists(temp_csv_path)}")
            if config.pinata_api_jwt and os.path.exists(temp_csv_path) and (rows or not rows): # Tenta upload mesmo se vazio (com cabeçalhos)
                logger.info(f"Tentando upload de {temp_csv_path} para Pinata como {temp_csv_filename}")
                pinata_url = upload_and_get_pinata_url(
                    local_file_path=temp_csv_path,
                    file_name_for_pinata=temp_csv_filename, # Usa o nome do arquivo temp que já tem timestamp
                    pinata_jwt=config.pinata_api_jwt,
                    pinata_metadata={"entity_slug": entity_slug, "type": "precatorios"}
                )
                if pinata_url:
                    logger.info(f"Upload para Pinata bem-sucedido: {pinata_url}")
                else:
                    logger.warning(f"Falha no upload do arquivo de precatórios para o Pinata: {temp_csv_path}")
            
            response_payload = PrecatorioResponse(
                status="success",
                message=f"{len(rows) if rows else 0} precatórios encontrados e normalizados para {entity_slug}.",
                data=rows if rows else [],
                pinata_url=pinata_url
            )
            return response_payload.dict(), 200

        except Exception as e:
            logger.error(f"Erro ao processar precatórios para {entity_slug}: {e}", exc_info=True)
            # Corrigido para usar .dict() e remover count
            return PrecatorioResponse(status="error", message=f"Erro interno: {str(e)}", data=[]).dict(), 500
        finally:
            # Limpar o arquivo e diretório temporário
            try:
                if os.path.exists(temp_csv_path):
                    os.remove(temp_csv_path)
                os.rmdir(temp_dir) 
                logger.info(f"Diretório temporário {temp_dir} limpo.")
            except OSError as e_clean:
                logger.warning(f"Erro ao limpar diretório temporário {temp_dir}: {e_clean}")

def cli():
    """Interface de linha de comando para o crawler"""
    parser = argparse.ArgumentParser(description="Crawler de precatórios do TJCE")
    parser.add_argument(
        "--entity",
        type=str,
        default=config.default_entity,
        help=f"Entidade para buscar precatórios (default: {config.default_entity})",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="precatorios.csv",
        help="Arquivo de saída CSV (default: precatorios.csv)",
    )

    args = parser.parse_args()

    try:
        logger = get_logger(__name__).bind(entity=args.entity)
        logger.info("iniciando_cli", output=args.output)
        crawler.crawl(args.entity, args.output)
        return 0
    except Exception as e:
        logger.error("erro_cli", error=str(e), exc_info=True)
        return 1

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        sys.exit(cli())
    else:
        logger.info("Iniciando servidor Flask.")
        app.run(debug=config.flask_debug_mode, host='0.0.0.0', port=config.flask_port)
