#!/usr/bin/env python3
import argparse
from flask import Flask, request, jsonify, Response, render_template
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
import requests

from crawler import PrecatoriosCrawler
from entity_mapping_crawler import EntityMappingCrawler
from edital_crawler import EditalCrawler
from config import config, field_config
from logger import configure_logging, get_logger
from models import (
    Precatorio,
    PrecatorioResponse,
    EntityMapping,
    EntidadeResponse,
    Edital,
    HealthCheckResponse,
    FetchPrecatoriosQuery,
)
from metrics import track_time
from entity_mapping import (
    get_api_entity_name,
    get_entity_slug,
    validate_entity_slug,
    ENTITY_MAPPING,
)
from pinata_uploader import upload_and_get_pinata_url

# Configuração do logging
configure_logging()
logger = get_logger(__name__)

# Configuração da API
app = Flask(__name__)


# Rota para a página inicial (definida ANTES da inicialização do Flask-RESTX Api)
@app.route("/")
def home():
    """Serve a nova página inicial com seleção de tipos de dados."""
    logger.info("Servindo página inicial (home.html)")
    return render_template("home.html")


# Rota para a página de ordem cronológica (consulta de precatórios)
@app.route("/cronologica")
def cronologica():
    """Serve a página de consulta de precatórios em ordem cronológica."""
    logger.info("Servindo página de ordem cronológica (index.html)")
    # Preparar a lista de entidades a partir do ENTITY_MAPPING
    # Formato: [{'slug': '...', 'official_name': '...'}]
    entities_list_for_frontend = sorted(
        [
            {"slug": slug, "official_name": name}
            for slug, name in ENTITY_MAPPING.items()
        ],
        key=lambda x: x["official_name"],  # Ordenar pelo nome oficial
    )
    return render_template("index.html", entities_list=entities_list_for_frontend)


# Rota para a página de editais (placeholder - será implementada quando API for fornecida)
@app.route("/edital")
def edital():
    """Serve a página de consulta de editais."""
    logger.info("Servindo página de editais (edital.html)")
    return render_template("edital.html")


# Rota para a página de pagamentos realizados (placeholder - será implementada quando API for fornecida)
@app.route("/pagamentos")
def pagamentos():
    """Serve a página de consulta de pagamentos realizados."""
    logger.info("Servindo página de pagamentos (pagamentos.html)")
    return render_template("pagamentos.html")




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
    title="Crawler TJCE - Precatorios",
    description=(
        "API para consulta e extração de dados de precatórios do Tribunal de Justiça "
        "do Estado do Ceará (TJCE). Permite listar entidades, buscar precatórios por "
        "entidade e obter os dados em formato CSV via Pinata."
    ),
    doc="/docs",
)

# Configuração do cache
cache = Cache(app)

# Criação do namespace
ns = api.namespace("api", description="Operações da API de Precatórios do TJCE")


# Rota para buscar editais via API
@ns.route("/editais")
class Editais(Resource):
    @cache.cached(timeout=config.cache_timeout_entities)
    @limiter.limit(config.rate_limit_entities)
    def get(self):
        """Lista todos os editais disponíveis e fornece um CSV com os dados via Pinata."""
        logger.info("Endpoint /editais chamado")
        output_filename = "editais_tjce.csv"
        output_dir = os.path.join(os.getcwd(), "data")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, output_filename)

        try:
            logger.info(f"Buscando editais e salvando em {output_path}")
            editais_data = edital_crawler.get_and_save_editais(output_path)

            if not editais_data:
                logger.warning("Nenhum edital encontrado.")
                return {
                    "status": "warning",
                    "message": "Nenhum edital encontrado.",
                    "data": [],
                }, 200

            pinata_url = None
            logger.info(
                f"Verificando condições para upload no Pinata (Editais): JWT_EXISTS={bool(config.pinata_api_jwt)}, FILE_EXISTS={os.path.exists(output_path)}"
            )
            if config.pinata_api_jwt and os.path.exists(output_path):
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                pinata_file_name = f"editais_tjce_{timestamp}.csv"
                logger.info(
                    f"Tentando upload de {output_path} para Pinata como {pinata_file_name}"
                )
                pinata_url = upload_and_get_pinata_url(
                    local_file_path=output_path,
                    file_name_for_pinata=pinata_file_name,
                    pinata_jwt=config.pinata_api_jwt,
                    pinata_metadata={"type": "editais"},
                )
                if pinata_url:
                    logger.info(f"Upload para Pinata bem-sucedido: {pinata_url}")
                else:
                    logger.warning(
                        "Falha no upload do arquivo de editais para o Pinata."
                    )

            # Converter dados para formato serializável
            serialized_data = []
            for edital in editais_data:
                # Trata o valor que pode vir como Decimal ou string
                valor = edital.get("valor", 0.0)
                if isinstance(valor, str):
                    if valor == "-" or not valor.strip():
                        valor = 0.0
                    else:
                        try:
                            valor = float(valor)
                        except (ValueError, TypeError):
                            valor = 0.0
                elif hasattr(valor, '__float__'):
                    valor = float(valor)

                serialized_edital = {
                    "ordem": edital.get("ordem", 0),
                    "ano_orcamento": edital.get("ano_orcamento", 0),
                    "natureza": edital.get("natureza", "-"),
                    "data_cadastro": edital.get("data_cadastro", "-"),
                    "precatorio": edital.get("precatorio", "-"),
                    "status": edital.get("status", "-"),
                    "valor": valor,
                }
                serialized_data.append(serialized_edital)

            return {
                "status": "success",
                "message": "Editais listados com sucesso.",
                "data": serialized_data,
                "pinata_url": pinata_url,
                "num_editais_found": len(editais_data),
            }, 200

        except Exception as e:
            logger.error(f"Erro ao buscar editais: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Erro interno: {str(e)}",
                "data": []
            }, 500

# Modelo para os argumentos de query da rota /fetch, para documentação e validação
fetch_precatorios_parser = ns.parser()
fetch_precatorios_parser.add_argument(
    "entity",
    type=str,
    required=True,
    help="Slug da entidade para buscar precatórios. Ex: municipio-de-fortaleza",
    location="args",
)
fetch_precatorios_parser.add_argument(
    "count",
    type=int,
    help=(
        "Número de registros a serem retornados por página durante a busca paginada no backend. "
        "Se não fornecido, o crawler usará um valor padrão (ex: 500). "
        "A API sempre tentará buscar TODOS os registros da entidade, paginando no backend."
    ),
    location="args",
    default=500,  # Mantém o default que o frontend já espera
)
fetch_precatorios_parser.add_argument(
    "year",
    type=int,
    help="Filtra os precatórios por ano de orçamento específico.",
    location="args",
)

# Configuração do Swagger UI para Flask-RESTX
authorizations = {"apikey": {"type": "apiKey", "in": "header", "name": "X-API-KEY"}}

# Modelos Flask-RESTX para documentação e marshalling
health_model_fields = api.model(
    "Health",
    {
        "status": fields.String(
            required=True, description="Status da saúde da aplicação."
        ),
        "message": fields.String(
            required=True, description="Mensagem sobre a saúde da aplicação."
        ),
    },
)

entity_mapping_item_fields = api.model(
    "EntityMappingItem",
    {
        "official_name": fields.String(
            required=True,
            description="Nome oficial da entidade como retornado pela fonte de dados.",
        ),
        "slug": fields.String(
            required=True, description="Slug normalizado da entidade para uso na API."
        ),
    },
)

entity_model_response_fields = api.model(
    "EntidadeResponse",
    {
        "status": fields.String(required=True, description="Status da resposta."),
        "message": fields.String(
            required=True, description="Mensagem descritiva da resposta."
        ),
        "data": fields.List(
            fields.Nested(entity_mapping_item_fields),
            description="Lista de mapeamentos de entidades.",
            allow_null=True,
        ),
        "pinata_url": fields.String(
            description="URL do arquivo CSV de entidades no Pinata, se o upload for bem-sucedido.",
            allow_null=True,
        ),
    },
)

fetch_query_model_fields = api.model(
    "FetchPrecatoriosQuery",
    {
        "entity": fields.String(
            required=True,
            description="Slug da entidade para buscar precatórios. Ex: municipio-de-fortaleza",
            example="municipio-de-fortaleza",
        ),
        "count": fields.Integer(
            description="Número de registros a serem retornados. Se não fornecido, busca todos.",
            example=10,
            allow_null=True,
        ),
    },
)

precatorio_item_fields = api.model(
    "PrecatorioItem",
    {
        "ordem": fields.Integer(
            required=True, description="Número de ordem do precatório."
        ),
        "processo": fields.String(required=True, description="Número do processo."),
        "comarca": fields.String(description="Comarca do precatório.", default="-"),
        "ano_orcamento": fields.Integer(required=True, description="Ano do orçamento."),
        "natureza": fields.String(
            description="Natureza do precatório (ex: ALIMENTAR).", default="-"
        ),
        "data_cadastro": fields.DateTime(
            description="Data de cadastro do precatório.",
            dt_format="iso8601",
            allow_null=True,
        ),
        "tipo_classificacao": fields.String(
            description="Tipo de classificação do precatório.", default="-"
        ),
        "valor_original": fields.String(
            description="Valor original do precatório formatado como string."
        ),  # Usando String para Decimals formatados
        "valor_atual": fields.String(
            description="Valor atualizado do precatório formatado como string."
        ),  # Usando String para Decimals formatados
        "situacao": fields.String(
            description="Situação atual do precatório.", default="-"
        ),
    },
)

precatorio_response_model_fields = api.model(
    "PrecatorioResponse",
    {
        "status": fields.String(required=True, description="Status da resposta."),
        "message": fields.String(
            required=True, description="Mensagem descritiva da resposta."
        ),
        "data": fields.List(
            fields.Nested(precatorio_item_fields),
            description="Lista de precatórios encontrados.",
            allow_null=True,
        ),
        "pinata_url": fields.String(
            description="URL do arquivo CSV de precatórios no Pinata, se o upload for bem-sucedido.",
            allow_null=True,
        ),
        "num_precatorios_found": fields.Integer(
            description="Número total de precatórios encontrados para a consulta antes do CSV.",
            required=False,
            allow_null=True,
        ),
    },
)

# Instâncias dos crawlers
crawler = PrecatoriosCrawler()
edital_crawler = EditalCrawler()


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
        """Verifica a saúde e disponibilidade da API."""
        logger.info("Health check endpoint chamado.")
        return HealthCheckResponse(status="OK", message="API está operacional")


@ns.route("/entities")
class Entidades(Resource):
    @cache.cached(timeout=config.cache_timeout_entities)
    @limiter.limit(config.rate_limit_entities)
    @api.marshal_with(entity_model_response_fields)
    def get(self):
        """Lista todas as entidades devedoras disponíveis para consulta de precatórios e fornece um CSV com os dados via Pinata."""
        logger.info("Endpoint /entities chamado")
        crawler = EntityMappingCrawler()
        output_filename = config.entities_output_filename
        output_dir = os.path.join(os.getcwd(), "data")  # Salva na pasta 'data'
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
                return (
                    EntidadeResponse(
                        status="warning",
                        message="Nenhuma entidade encontrada.",
                        data=[],
                    ).dict(),
                    200,
                )

            pinata_url = None
            logger.info(
                f"Verificando condições para upload no Pinata (Entidades): JWT_EXISTS={bool(config.pinata_api_jwt)}, FILE_EXISTS={os.path.exists(output_path)}"
            )
            if config.pinata_api_jwt and os.path.exists(output_path):
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                pinata_file_name = f"entidades_tjce_{timestamp}.csv"
                logger.info(
                    f"Tentando upload de {output_path} para Pinata como {pinata_file_name}"
                )
                pinata_url = upload_and_get_pinata_url(
                    local_file_path=output_path,
                    file_name_for_pinata=pinata_file_name,
                    pinata_jwt=config.pinata_api_jwt,
                    pinata_metadata={"type": "entities_mapping"},
                )
                if pinata_url:
                    logger.info(f"Upload para Pinata bem-sucedido: {pinata_url}")
                else:
                    logger.warning(
                        "Falha no upload do arquivo de entidades para o Pinata."
                    )

            response_data = EntidadeResponse(
                status="success",
                message="Entidades listadas com sucesso.",
                data=entities_data,
                pinata_url=pinata_url,
            )
            return response_data.dict(), 200  # Corrigido para retornar dicionário

        except Exception as e:
            logger.error(f"Erro ao buscar entidades: {e}", exc_info=True)
            # Corrigido para retornar dicionário
            return (
                EntidadeResponse(
                    status="error", message=f"Erro interno: {str(e)}", data=[]
                ).dict(),
                500,
            )


@ns.route("/fetch")
class FetchPrecatorios(Resource):
    """Busca e retorna precatórios para uma entidade específica."""

    @limiter.limit(config.rate_limit_fetch)
    @ns.expect(fetch_precatorios_parser)  # Usa o parser para os argumentos GET
    @ns.marshal_with(precatorio_response_model_fields)
    def get(self):
        """Busca precatórios com base no slug da entidade e opcionalmente no ano.

        A API agora busca todos os precatórios da entidade, lidando com paginação internamente.
        O parâmetro 'count' influencia o tamanho da página durante essa busca interna.
        """
        args = (
            fetch_precatorios_parser.parse_args()
        )  # Obtém os argumentos da query string
        entity_slug = args.get("entity")
        # O parâmetro 'count' da requisição será usado como 'count_per_page' no crawler.
        # Se 'count' não for fornecido na URL, o parser define default 500.
        # Se o usuário explicitamente passar count=0 ou um valor inválido,
        # o crawler.fetch_all_precatorios_data usará seu próprio default_batch_size.
        count_per_page_for_crawler = args.get("count")
        year_filter = args.get("year")

        logger.info(
            "fetch_precatorios_request_received",
            entity_slug=entity_slug,
            requested_batch_size=count_per_page_for_crawler,
            year_filter=year_filter,
        )

        if not entity_slug:
            logger.warning("fetch_api_called_without_entity_slug")
            message = "O parâmetro 'entity' (slug da entidade) é obrigatório."
            api.abort(400, message)

        if not validate_entity_slug(entity_slug):
            logger.warning("invalid_entity_slug_provided", slug=entity_slug)
            # Tenta encontrar um slug válido se um nome oficial foi passado por engano
            possible_slug = get_entity_slug(
                entity_slug
            )  # Tenta converter nome para slug
            if possible_slug and validate_entity_slug(possible_slug):
                logger.info(
                    f"Nome '{entity_slug}' convertido para slug válido: '{possible_slug}'"
                )
                entity_slug = possible_slug
            else:
                logger.error(
                    f"Slug de entidade inválido ou não encontrado: {entity_slug}"
                )
                message = f"Slug de entidade inválido ou não encontrado: {entity_slug}"
                api.abort(400, message)

        official_entity_name = get_api_entity_name(entity_slug)
        if not official_entity_name:
            logger.error(f"Nome oficial não encontrado para o slug: {entity_slug}")
            message = f"Nome oficial não encontrado para o slug: {entity_slug}"
            api.abort(404, message)

        try:
            # Chama fetch_all_precatorios_data para buscar todos os dados com paginação interna
            # O count_per_page_for_crawler (vindo do 'count' da URL) define o tamanho do batch no crawler
            all_rows = crawler.fetch_all_precatorios_data(
                entity_slug_or_official_name=official_entity_name,
                count_per_page=count_per_page_for_crawler,
                year=year_filter,
            )

            if not all_rows:
                logger.info(
                    "no_precatorios_found_for_entity",
                    entity_slug=entity_slug,
                    year=year_filter,
                )
                year_msg = f"ano {year_filter if year_filter else 'não aplicado'}"
                message = f"Nenhum precatório para '{official_entity_name}' ({entity_slug}) {year_msg}."
                return {
                    "status": "success",
                    "message": message,
                    "data": [],
                    "pinata_url": None,
                    "num_precatorios_found": 0,
                }, 200

            logger.info(
                f"{len(all_rows)} precatórios encontrados para '{official_entity_name}' "
                f"(slug: {entity_slug})."
            )

            # Gera o nome do arquivo CSV e o caminho temporário
            timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
            normalized_entity_slug_for_filename = entity_slug.replace("-", "_")
            csv_filename = (
                f"precatorios_{normalized_entity_slug_for_filename}_{timestamp_str}.csv"
            )

            # Cria um diretório temporário seguro para o arquivo CSV
            with tempfile.TemporaryDirectory() as tmpdir:
                csv_temp_path = os.path.join(tmpdir, csv_filename)

                logger.info(f"Escrevendo CSV em: {csv_temp_path}")
                crawler.write_csv(all_rows, csv_temp_path)

                logger.info(f"Upload do CSV para Pinata: {csv_filename}")
                pinata_url = upload_and_get_pinata_url(
                    local_file_path=csv_temp_path,
                    file_name_for_pinata=csv_filename,
                    pinata_jwt=config.pinata_api_jwt,
                )

            if pinata_url:
                logger.info("CSV no Pinata com sucesso", pinata_url=pinata_url)
                message = f"Precatórios para '{official_entity_name}' ({entity_slug}) recuperados. CSV disponível."
                return {
                    "status": "success",
                    "message": message,
                    "data": all_rows,  # Retorna os dados completos na resposta JSON também
                    "pinata_url": pinata_url,
                    "num_precatorios_found": len(all_rows),
                }, 200
            else:
                logger.error("Falha no upload para Pinata. Servindo apenas dados.")
                message = f"Precatórios para '{official_entity_name}' ({entity_slug}) recuperados. Upload Pinata falhou."
                return {
                    "status": "warning",
                    "message": message,
                    "data": all_rows,
                    "pinata_url": None,
                    "num_precatorios_found": len(all_rows),
                }, 200

        except ValueError as ve:
            logger.error(
                f"value_error_fetching_precatorios: {ve}",
                entity_slug=entity_slug,
                exc_info=True,
            )
            api.abort(400, str(ve))
        except requests.exceptions.RequestException as re:
            logger.error(
                f"request_exception_fetching_precatorios: {re}",
                entity_slug=entity_slug,
                exc_info=True,
            )
            error_message = f"Erro de comunicação com a API externa: {re}"
            api.abort(503, error_message)
        except Exception as e:
            logger.error(
                f"unexpected_error_fetching_precatorios: {e}",
                entity_slug=entity_slug,
                exc_info=True,
            )
            error_message = f"Erro inesperado ao processar a solicitação: {e}"
            api.abort(500, error_message)


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
        app.run(debug=config.flask_debug_mode, host="0.0.0.0", port=config.flask_port)
