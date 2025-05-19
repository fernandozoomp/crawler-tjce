#!/usr/bin/env python3
import argparse
from flask import Flask, request
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

from crawler import PrecatoriosCrawler
from config import config
from logger import configure_logging, get_logger
from models import PrecatorioResponse, EntidadeResponse
from metrics import track_time
from entity_mapping import get_api_entity_name, get_entity_slug, validate_entity_slug

# Configuração do logging
configure_logging()
logger = get_logger(__name__)

# Configuração da API
app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False  # Permite caracteres UTF-8 no JSON
app.config["RESTX_JSON"] = {
    "ensure_ascii": False
}  # Configura o flask-restx para não escapar caracteres UTF-8

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
)

# Criação do namespace
ns = api.namespace("api", description="Operações da API de Precatórios")

# Configuração do cache
cache = Cache(app, config={"CACHE_TYPE": "simple"})


# Função para gerar chave de cache
def make_cache_key():
    """Gera uma chave de cache baseada nos parâmetros da requisição"""
    args = request.args.copy()
    args.pop("output", None)  # Remove output do cache key
    return f"{request.path}?{urlencode(sorted(args.items()))}"


# Adiciona endpoint do Prometheus
app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {"/metrics": make_wsgi_app()})


# Endpoint de healthcheck
@app.route("/health")
def health_check():
    return {"status": "healthy", "version": "1.0.0"}


# Modelos da API
entity_mapping_model = api.model(
    "EntityMapping",
    {
        "official_name": fields.String(
            required=True, description="Nome oficial da entidade no TJCE"
        ),
        "slug": fields.String(required=True, description="Slug para uso na API"),
    },
)

entities_response_model = api.model(
    "EntitiesResponse",
    {
        "status": fields.String(required=True, description="Status da requisição"),
        "message": fields.String(required=True, description="Mensagem"),
        "data": fields.List(
            fields.Nested(entity_mapping_model),
            description="Lista de entidades e seus slugs",
        ),
    },
)

precatorio_model = api.model(
    "Precatorio",
    {
        "ordem": fields.Integer(required=True, description="Número de ordem"),
        "processo": fields.String(required=True, description="Número do processo"),
        "comarca": fields.String(required=True, description="Comarca"),
        "ano_orcamento": fields.Integer(required=True, description="Ano do orçamento"),
        "natureza": fields.String(required=True, description="Natureza"),
        "data_cadastro": fields.DateTime(required=True, description="Data de cadastro"),
        "tipo_classificacao": fields.String(
            required=True, description="Tipo de classificação"
        ),
        "valor_original": fields.Float(required=True, description="Valor original"),
        "valor_atual": fields.Float(required=True, description="Valor atual"),
        "situacao": fields.String(required=True, description="Situação"),
    },
)

response_model = api.model(
    "Response",
    {
        "status": fields.String(required=True, description="Status da requisição"),
        "message": fields.String(required=True, description="Mensagem"),
        "data": fields.List(
            fields.Nested(precatorio_model), description="Dados dos precatórios"
        ),
        "total": fields.Integer(description="Total de registros"),
        "page": fields.Integer(description="Página atual"),
        "total_pages": fields.Integer(description="Total de páginas"),
    },
)

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


@ns.route("/fetch")
class PrecatoriosResource(Resource):
    @api.doc(
        params={
            "entity": "Slug da entidade (ex: municipio-de-fortaleza)",
            "page": "Número da página (default: 1)",
            "per_page": "Itens por página (default: 50)",
            "sort_by": "Campo para ordenação (ex: valor_atual)",
            "order": "Direção da ordenação (asc/desc)",
            "ano_min": "Ano mínimo do orçamento",
            "ano_max": "Ano máximo do orçamento",
            "valor_min": "Valor mínimo",
            "valor_max": "Valor máximo",
            "natureza": "Natureza do precatório",
            "output": "Nome do arquivo CSV para salvar os dados",
            "count": "Número de registros para buscar da API PowerBI (default: crawler decide)",
        }
    )
    @api.response(200, "Success", response_model)
    @api.response(400, "Parâmetros inválidos")
    @api.response(429, "Muitas requisições")
    @api.response(500, "Internal Server Error")
    @limiter.limit("1 per second")  # Rate limiting por IP
    @cache.cached(timeout=3600, key_prefix=make_cache_key)  # Cache por 1 hora
    @track_time(entity=lambda: request.args.get("entity", config.default_entity))
    def get(self):
        """Busca precatórios para uma entidade específica"""
        try:
            # Obtém o slug da entidade
            entity_slug = request.args.get("entity", "municipio-de-fortaleza")

            # Valida a entidade
            if not validate_entity_slug(entity_slug):
                return (
                    PrecatorioResponse(
                        status="error",
                        message=f"Entidade inválida: {entity_slug}",
                        data=None,
                    ).dict(),
                    400,
                )

            # Converte para o formato esperado pela API do TJCE
            api_entity = get_api_entity_name(entity_slug)

            # Definir o logger aqui para que esteja disponível para os blocos try/except abaixo
            logger = get_logger(__name__).bind(entity_slug=entity_slug)

            # Parâmetros de paginação
            try:
                page = int(request.args.get("page", 1))
                per_page = min(
                    int(request.args.get("per_page", 50)), 100
                )  # Limita a 100 itens por página
                if page < 1 or per_page < 1:
                    raise ValueError("Página e itens por página devem ser positivos")
            except ValueError as e:
                return (
                    PrecatorioResponse(
                        status="error",
                        message=str(e),
                        data=None,
                    ).dict(),
                    400,
                )

            # Novo parâmetro para contagem de registros
            try:
                requested_count_str = request.args.get("count")
                requested_count = (
                    int(requested_count_str)
                    if requested_count_str is not None and requested_count_str.isdigit()
                    else None
                )
                if requested_count is not None and requested_count < 1:
                    logger.warning(
                        "contagem_invalida_ignorado", count_fornecido=requested_count
                    )
                    requested_count = None  # Ignora se for < 1, deixa o crawler decidir
            except (
                ValueError
            ):  # Em caso de falha na conversão para int, não não numérico.
                logger.warning(
                    "contagem_nao_numerica_ignorado",
                    count_fornecido=request.args.get("count"),
                )
                requested_count = None

            output = request.args.get("output", "precatorios.csv")

            # Atualiza o logger com todos os parâmetros finais
            logger = logger.bind(
                page=page, per_page=per_page, requested_count=requested_count
            )
            logger.info("iniciando_busca")

            # Busca os dados usando o nome formatado para a API do TJCE e o count solicitado
            raw_data = crawler.fetch_data(entity=api_entity, count=requested_count)
            if not raw_data:
                logger.warning("nenhum_dado_encontrado")
                return PrecatorioResponse(
                    status="error",
                    message="Nenhum dado encontrado para a entidade",
                    data=None,
                ).dict()

            # Normaliza os dados
            rows = crawler.normalize_to_rows(raw_data)
            if not rows:
                logger.warning("erro_normalizacao")
                return PrecatorioResponse(
                    status="error",
                    message="Erro ao normalizar os dados da entidade",
                    data=None,
                ).dict()

            # Aplica filtros
            filtered_rows = apply_filters(
                rows,
                ano_min=request.args.get("ano_min"),
                ano_max=request.args.get("ano_max"),
                valor_min=request.args.get("valor_min"),
                valor_max=request.args.get("valor_max"),
                natureza=request.args.get("natureza"),
            )

            # Ordena os resultados
            sorted_rows = sort_rows(
                filtered_rows,
                sort_by=request.args.get("sort_by"),
                order=request.args.get("order", "asc"),
            )

            # Calcula a paginação
            total = len(sorted_rows)
            total_pages = (total + per_page - 1) // per_page
            start = (page - 1) * per_page
            end = start + per_page
            paginated_rows = sorted_rows[start:end]

            # Salva os dados em arquivo se necessário
            if output:
                try:
                    crawler.write_csv(rows, output)
                    logger.info("dados_salvos", output=output)
                except Exception as e:
                    logger.error("erro_salvar_arquivo", error=str(e), output=output)
                    return PrecatorioResponse(
                        status="error",
                        message=f"Erro ao salvar arquivo: {str(e)}",
                        data=None,
                    ).dict()

            # Log do resultado final
            logger.info(
                "busca_concluida",
                total_registros=total,
                total_paginas=total_pages,
                registros_filtrados=len(filtered_rows),
            )

            # Retorna os dados na resposta
            response_message = f"Encontrados {total} registros"
            if output and output != "precatorios.csv":
                response_message += f", salvos em {output}"
            else:
                pass  # A mensagem já está boa.

            return PrecatorioResponse(
                status="success",
                message=response_message,
                data=None,  # Alterado: Não retorna a lista de precatórios no JSON
            ).dict()

        except Exception as e:
            logger.error("erro_busca", error=str(e), exc_info=True)
            return (
                PrecatorioResponse(
                    status="error",
                    message=f"Erro ao buscar dados: {str(e)}",
                    data=None,
                ).dict(),
                500,
            )


@ns.route("/entities")
class EntidadesResource(Resource):
    @api.doc(params={"output": "Nome do arquivo CSV para salvar as entidades"})
    @api.response(200, "Success", entities_response_model)
    @api.response(429, "Muitas requisições")
    @api.response(500, "Internal Server Error")
    @limiter.limit("1 per second")  # Rate limiting por IP
    @cache.cached(timeout=3600, key_prefix=make_cache_key)  # Cache por 1 hora
    @track_time(entity="all")
    def get(self):
        """Busca a lista de entidades disponíveis com seus respectivos slugs"""
        try:
            output = request.args.get("output", "entidades.csv")

            logger = get_logger(__name__).bind(output=output)
            logger.info("iniciando_busca_entidades")

            # Busca as entidades da API do TJCE
            api_entities = crawler.fetch_entities()

            # Cria o mapeamento bidirecional
            entity_mappings = [
                {"official_name": entity, "slug": get_entity_slug(entity)}
                for entity in api_entities
            ]

            if output:
                # Salva os mapeamentos no arquivo
                import csv

                with open(output, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=["official_name", "slug"])
                    writer.writeheader()
                    writer.writerows(entity_mappings)

            return EntidadeResponse(
                status="success",
                message=f"Encontradas {len(entity_mappings)} entidades"
                + (f", salvas em {output}" if output else ""),
                data=entity_mappings,
            ).dict()

        except Exception as e:
            logger.error("erro_busca_entidades", error=str(e), exc_info=True)
            return (
                EntidadeResponse(
                    status="error",
                    message=str(e),
                    data=None,
                ).dict(),
                500,
            )


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
        app.run(host="0.0.0.0", port=5000)
