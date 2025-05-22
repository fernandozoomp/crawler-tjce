#!/usr/bin/env python3
import csv
import json
from datetime import datetime, timedelta
import os
from typing import Dict, List, Optional, Union, Any, Tuple
import locale
import uuid
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, stop_after_attempt, wait_exponential
import re
from copy import deepcopy
from io import StringIO
from decimal import Decimal, InvalidOperation
import urllib.parse

import requests
from pydantic import ValidationError

from config import (
    config,
    field_config,
    PAYLOAD_STRUCTURE,
)
from logger import get_logger
from models import Precatorio
from metrics import REQUESTS_TOTAL, RECORDS_PROCESSED, track_time
from entity_mapping import get_api_entity_name

logger = get_logger(__name__)

# Tenta configurar o locale
try:
    locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")
    LOCALE_OK = True
except locale.Error:
    logger.warning(
        "locale_error",
        message="Não foi possível configurar o locale pt_BR.UTF-8, usando formatação manual",
    )
    LOCALE_OK = False

# PAGINATION_ORDER_BY_COLUMNS = [ # Comentando a lista original
#     "dfslcp_num_ordem",
#     "dfslcp_dsc_proc_precatorio",
#     "dfslcp_num_ano_orcamento",
#     "dfslcp_dsc_natureza",
#     "dfslcp_dat_cadastro",
#     "dfslcp_dsc_tipo_classificao",
#     "dfslcp_vlr_original",
#     "dfslcp_dsc_sit_precatorio",
#     "dfslcp_dsc_comarca",
#     "ValorAtualFormatado",
# ]

# Ajustado para corresponder ao OrderBy do cURL funcional
PAGINATION_ORDER_BY_COLUMNS = ["dfslcp_num_ordem"]


def format_currency(value: float) -> str:
    """Formata valor monetário manualmente se o locale não estiver disponível."""
    if LOCALE_OK:
        return locale.currency(value, grouping=True, symbol=True)

    # Formatação manual
    value_str = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {value_str}"


class PrecatoriosCrawler:
    def __init__(self):
        self.config_instance = config
        self.field_config_instance = field_config
        self.api_url = self.config_instance.api_url
        self.resource_key = self.config_instance.resource_key
        self.headers = self.config_instance.headers
        self.current_entity_processed_records = 0
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.base_payload = PAYLOAD_STRUCTURE
        self.pagination_order_by_columns = PAGINATION_ORDER_BY_COLUMNS
        self.csv_fields = field_config.csv_fields

    def _decode_utf8(self, value: str) -> str:
        """Decodifica uma string com caracteres especiais em UTF-8."""
        if not isinstance(value, str):
            return str(value)
        try:
            # Decodifica sequências de escape unicode (\u00XX)
            return (
                value.encode("latin-1")
                .decode("unicode-escape")
                .encode("latin-1")
                .decode("utf-8")
            )
        except (UnicodeError, AttributeError):
            return value

    def _format_value(self, value: str, field_type: str) -> str:
        """Formata o valor de acordo com o tipo do campo."""
        value_str = str(value)

        if value is None or (
            isinstance(value_str, str)
            and (not value_str.strip() or value_str.lower() == "none")
        ):
            if field_type == "date":
                return ""  # Pydantic converterá para None em Optional[datetime]
            if field_type in ["int", "float", "Decimal"]:
                return "0"  # Default para números
            return "-"  # Default para outras strings vazias

        try:
            if field_type == "processo":
                if isinstance(value, (int, float)):  # value here is original
                    return f"{value:020.0f}"
                return value_str.strip()

            if field_type in ["int", "float", "Decimal"]:
                temp_value = value_str
                if isinstance(temp_value, str):
                    temp_value = temp_value.replace("R$", "").strip()
                    if "," in temp_value and "." in temp_value:
                        if temp_value.rfind(".") < temp_value.rfind(","):
                            temp_value = temp_value.replace(".", "").replace(",", ".")
                    elif "," in temp_value:
                        temp_value = temp_value.replace(",", ".")

                try:
                    val_float = float(temp_value)
                    if field_type == "int":
                        return str(int(val_float))
                    return str(val_float)
                except (ValueError, TypeError):
                    logger.warning(
                        f"Could not convert numeric value '{value_str}' "
                        f"(processed to '{temp_value}') to {field_type}. Defaulting to '0'."
                    )
                    return "0"

            if field_type == "date":
                if isinstance(value_str, str):
                    # 1. Tentar formato "datetime(YYYY,MM,DD...)"
                    if "datetime" in value_str.lower():
                        match = re.search(
                            r"datetime\\s*\\(([^)]+)\\)", value_str, re.IGNORECASE
                        )
                        if match:
                            try:
                                components_str = match.group(1).split(",")
                                components = [int(c.strip()) for c in components_str]
                                if len(components) >= 3:
                                    # Ajustar mês se parecer 0-indexado (improvável com PowerBI, mas seguro)
                                    if components[1] == 0 and len(components) > 1:
                                        components[1] = 1
                                    return str(datetime(*components))
                                else:
                                    logger.warning(
                                        f"Date string '{value_str}' (datetime format) has insufficient components."
                                    )
                                    return ""
                            except ValueError as e:
                                logger.warning(
                                    f"Error parsing datetime components from '{value_str}': {e}"
                                )
                                return ""

                    # 2. Tentar converter para float e verificar se é timestamp ou data serial Excel
                    try:
                        ts = float(value_str)

                        # 2a. Verificar se é timestamp em milissegundos
                        # (Ex: 13 dígitos para datas recentes, 1715558400000)
                        # Limite superior para datas razoáveis em ms (ex: ano 10000)
                        if (
                            ts > 100000000000 and ts < 300000000000000
                        ):  # Approx 1973 to year 10000 in ms
                            return str(datetime.fromtimestamp(ts / 1000.0))

                        # 2b. Verificar se é timestamp em segundos
                        # (Ex: 10 dígitos para datas recentes, 1715558400)
                        if (
                            ts > 1000000000 and ts < 300000000
                        ):  # Approx 1973 to year 2038-ish in seconds
                            return str(datetime.fromtimestamp(ts))

                        # 2c. Verificar se é data serial do Excel (ex: 30000 a 70000 para datas comuns)
                        # O valor 13717.16 é 1937-07-07. O valor 470 é 1901-04-14.
                        if 1 < ts < 80000:  # Cobre de 1900-01-01 até bem depois de 2100
                            try:
                                delta_days = int(ts)
                                delta_fraction = ts - delta_days
                                dt = (
                                    datetime(1899, 12, 30)
                                    + timedelta(days=delta_days)
                                    + timedelta(seconds=delta_fraction * 86400)
                                )
                                return str(
                                    dt.strftime("%Y-%m-%d %H:%M:%S")
                                    if dt.time() != datetime.min.time()
                                    else dt.strftime("%Y-%m-%d")
                                )
                            except (ValueError, OverflowError) as excel_e:
                                logger.warning(
                                    f"Falha ao converter data serial do Excel '{value_str}': {excel_e}"
                                )
                                return ""  # Fallback se a conversão Excel falhar

                        # Se chegou aqui como float mas não se encaixou nos padrões acima
                        logger.warning(
                            f"Valor numérico '{value_str}' não reconhecido como formato de data válido "
                            f"(timestamp ou serial Excel)."
                        )
                        return ""

                    except ValueError:
                        # Não é float, nem "datetime(...)"
                        # Outras tentativas de parse (ISO, DD/MM/YYYY) podem ser adicionadas aqui se necessário
                        logger.debug(
                            f"Valor '{value_str}' para campo de data não é numérico nem formato 'datetime(...)'."
                        )
                        return ""  # Fallback final para strings não reconhecidas

                elif isinstance(
                    value, datetime
                ):  # Se já for datetime (raro neste ponto do fluxo)
                    return str(value)

                logger.warning(
                    f"Unparseable date value encountered: {value_str} (type: {type(value)}). "
                    f"Returning empty for Pydantic."
                )
                return ""

            return value_str.strip()

        except Exception as e:
            logger.warning(
                "format_error",
                value=value_str,
                original_value_type=str(type(value)),
                field_type=field_type,
                error=str(e),
                exc_info=True,
            )
            if field_type in ["int", "float", "Decimal"]:
                return "0"
            if field_type == "date":
                return ""  # Fallback for errors during date formatting
            return "-"

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    @track_time
    def _fetch_page(
        self,
        entity: str,
        restart_tokens: Optional[List[Any]] = None,
        count: Optional[int] = None,
        year: Optional[int] = None,
    ) -> Dict:
        """Busca uma página de dados da API."""
        current_headers = self.session.headers.copy()
        current_headers.update(
            {"ActivityId": str(uuid.uuid4()), "RequestId": str(uuid.uuid4())}
        )

        payload = self.get_precatorios_payload(
            entity_slug_or_official_name=entity,
            count=(count if count is not None else self.config_instance.batch_size),
            restart_tokens=restart_tokens,
            year=year,
        )

        try:
            # Tentativa de log mais seguro, verificando a existência das chaves
            binding_details = (
                payload.get("Queries", [{}])[0]
                .get("Query", {})
                .get("Commands", [{}])[0]
                .get("SemanticQueryDataStructure", {})
                .get("Binding")
            )
            if binding_details:
                logger.info(
                    "fetch_page_request_binding", binding_details=binding_details
                )
            else:
                logger.warning("Binding details not found in payload for logging.")
        except (
            Exception
        ) as log_e:  # Pega exceção genérica para o log não quebrar a função
            logger.warning(f"Could not log binding details due to an error: {log_e}")

        REQUESTS_TOTAL.labels(entity=entity).inc()
        response = self.session.post(
            self.api_url, json=payload, headers=current_headers, timeout=180
        )  # Timeout aumentado para 180s
        response.raise_for_status()
        return response.json()

    def get_precatorios_payload(
        self,
        entity_slug_or_official_name: str,
        count: Optional[int] = None,
        skip: int = 0,
        year: Optional[int] = None,
        restart_tokens: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Prepara o payload para a requisição de precatórios, incluindo filtros e paginação."""
        payload = deepcopy(self.base_payload)

        try:
            # Ajustado o caminho para SemanticQueryDataShapeCommand
            command_structure = payload["queries"][0]["Query"]["Commands"][0][
                "SemanticQueryDataShapeCommand"
            ]
        except (KeyError, IndexError) as e:
            logger.error(
                f"Error accessing payload command structure: {e}. Payload might be malformed.",
                exc_info=True,
            )
            raise ValueError("Payload command structure is not as expected.") from e

        # PAYLOAD_STRUCTURE deve ter o Select correto. Não vamos reconstruí-lo dinamicamente por enquanto.
        # Se for necessário, a lógica comentada para select_clauses pode ser reativada e ajustada.
        # select_clauses = []
        # for field_key, col_config in self.field_config_instance.field_mapping.items():
        #     api_name = col_config.get("api_name")
        #     if api_name:
        #         select_clauses.append({
        #             "Column": {
        #                 "Expression": {"SourceRef": {"Source": "d"}},
        #                 "Property": api_name,
        #             },
        #             "Name": f"dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.{api_name}"
        #         })
        # command_structure["Select"] = select_clauses

        # Aplica OrderBy para paginação consistente
        command_structure["OrderBy"] = [
            {
                "Direction": 1,
                "Expression": {
                    "Column": {
                        "Property": col_name,
                        "Expression": {"SourceRef": {"Source": "d"}},
                    }
                },
            }
            for col_name in self.pagination_order_by_columns
        ]

        # Configurações de Binding, DataReduction, e Window no local correto
        try:
            binding_for_window = command_structure["Binding"]
            data_reduction = binding_for_window["DataReduction"]
            primary_reduction = data_reduction["Primary"]
            window_to_modify = primary_reduction["Window"]
        except KeyError as e:
            logger.error(
                f"Estrutura de payload esperada não encontrada ao tentar acessar Window: {e}. "
                f"Verifique PAYLOAD_STRUCTURE em config.py. "
                f"Caminho esperado: SemanticQueryDataShapeCommand.Binding.DataReduction.Primary.Window",
                exc_info=True,
            )
            raise ValueError(
                f"Estrutura de payload inválida para Window, chave ausente: {e}"
            )

        effective_count = (
            count
            if count is not None and count > 0
            else self.config_instance.batch_size
        )
        window_to_modify["Count"] = effective_count

        if restart_tokens:
            window_to_modify["RestartTokens"] = restart_tokens
        elif "RestartTokens" in window_to_modify:
            del window_to_modify["RestartTokens"]

        # Filtro de Entidade e Ano
        api_entity_name = get_api_entity_name(entity_slug_or_official_name)
        if not api_entity_name:
            logger.error(
                f"Nome oficial da API não encontrado para: {entity_slug_or_official_name}"
            )
            raise ValueError(
                f"Slug ou nome da entidade inválido: {entity_slug_or_official_name}"
            )

        # Corrigir o acesso à cláusula Where
        query_definition = command_structure[
            "Query"
        ]  # command_structure é SemanticQueryDataShapeCommand

        if "Where" not in query_definition:
            query_definition["Where"] = []

        # Remover filtros de entidade preexistentes para evitar duplicidade ou conflito.
        preserved_filters = []
        entity_column_names = [
            "dfslcp_nom_entidade_devedora",
            "dfslcp_dsc_entidade",
        ]  # Nomes comuns para colunas de entidade
        for item_filter in query_definition["Where"]:
            is_entity_filter = False
            try:
                # Checa se o filtro atual é um filtro de entidade (Comparison)
                if (
                    item_filter.get("Condition", {})
                    .get("Comparison", {})
                    .get("Left", {})
                    .get("Column", {})
                    .get("Property")
                    in entity_column_names
                ):
                    is_entity_filter = True
                # Checa se o filtro atual é um filtro de entidade (In)
                elif (
                    item_filter.get("Condition", {}).get("In", {})
                    and isinstance(item_filter["Condition"]["In"]["Expressions"], list)
                    and len(item_filter["Condition"]["In"]["Expressions"]) > 0
                    and item_filter["Condition"]["In"]["Expressions"][0]
                    .get("Column", {})
                    .get("Property")
                    in entity_column_names
                ):
                    is_entity_filter = True
            except (KeyError, TypeError, AttributeError):
                logger.warning(
                    "Could not reliably determine if a filter is an entity filter due to structure.",
                    filter_item=item_filter,
                )

            if not is_entity_filter:
                preserved_filters.append(item_filter)
            else:
                logger.debug(f"Removing pre-existing entity filter: {item_filter}")

        query_definition["Where"] = preserved_filters
        new_filters = list(
            preserved_filters
        )  # Começa com os filtros não-entidade preservados

        # Adicionar o filtro de entidade correto, usando a estrutura "In" como no cURL
        new_filters.append(
            {
                "Condition": {
                    "In": {
                        "Expressions": [
                            {
                                "Column": {
                                    "Expression": {"SourceRef": {"Source": "d"}},
                                    "Property": "dfslcp_dsc_entidade",  # Propriedade usada no cURL
                                }
                            }
                        ],
                        "Values": [[{"Literal": {"Value": f"'{api_entity_name}'"}}]],
                    }
                }
            }
        )
        logger.debug(
            f"Added new entity filter for '{api_entity_name}' on property 'dfslcp_dsc_entidade' using 'In' structure."
        )

        # Filtros de ano
        # Remove qualquer filtro de ano existente dos new_filters antes de adicionar o novo (se houver)
        filters_without_year = [
            f
            for f in new_filters
            if not (
                f.get("Condition", {})
                .get("Comparison", {})
                .get("Left", {})
                .get("Column", {})
                .get("Property")
                == "dfslcp_num_ano_orcamento"
            )
        ]
        new_filters = filters_without_year

        if year is not None:
            new_filters.append(
                {
                    "Condition": {
                        "Comparison": {
                            "ComparisonKind": 0,  # Equals
                            "Left": {
                                "Column": {
                                    "Expression": {"SourceRef": {"Source": "d"}},
                                    "Property": "dfslcp_num_ano_orcamento",
                                }
                            },
                            "Right": {
                                "Literal": {
                                    "Value": f"{year}L"
                                }  # L para tipo Long/Integer
                            },
                        }
                    }
                }
            )
            logger.debug(f"Added year filter: {year}")
        else:
            logger.debug("No year filter applied as year was not provided.")

        query_definition["Where"] = new_filters
        logger.debug(f"Final filters for Where clause: {query_definition['Where']}")

        logger.debug(
            "Final payload generated",
            entity=api_entity_name,
            count=effective_count,
            year=year,
            has_restart_tokens=bool(restart_tokens),
        )
        return payload

    @track_time
    def fetch_all_precatorios_data(
        self,
        entity_slug_or_official_name: str,
        count_per_page: Optional[int] = None,
        year: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Busca todos os dados de precatórios para uma entidade, paginando automaticamente."""
        api_entity_name = get_api_entity_name(entity_slug_or_official_name)
        if not api_entity_name:
            logger.error(
                "entity_not_found_in_mapping", entity=entity_slug_or_official_name
            )
            return []

        all_normalized_rows: List[Dict[str, Any]] = []
        current_restart_tokens: Optional[List[Any]] = None
        page_num = 0
        processed_records_for_entity = 0
        last_order_number = 0
        batch_size = (
            count_per_page
            if count_per_page is not None
            else self.config_instance.batch_size
        )

        logger.info(
            "starting_full_fetch",
            entity=api_entity_name,
            batch_size=batch_size,
            year_filter=year,
        )

        while True:
            page_num += 1
            logger.info(
                "fetching_page",
                entity=api_entity_name,
                page=page_num,
                current_total_fetched=len(all_normalized_rows),
                has_restart_tokens=bool(current_restart_tokens),
            )
            try:
                page_data_response = self._fetch_page(
                    entity=api_entity_name,
                    restart_tokens=current_restart_tokens,
                    count=batch_size,
                    year=year,
                )
                if (
                    not page_data_response
                    or "results" not in page_data_response
                    or not page_data_response["results"]
                ):
                    logger.warning(
                        "empty_or_invalid_response_from_api",
                        entity=api_entity_name,
                        page=page_num,
                    )
                    break

                # A função normalize_to_rows espera uma lista de respostas de página
                normalized_page_rows, last_order_number_from_page = (
                    self.normalize_to_rows(
                        [page_data_response], starting_order_number=last_order_number
                    )
                )
                last_order_number = last_order_number_from_page

                if not normalized_page_rows:  # Se a normalização não retornar linhas
                    raw_data_present = bool(
                        page_data_response["results"][0]
                        .get("result", {})
                        .get("data", {})
                        .get("dsr", {})
                        .get("DS", [{}])[0]
                        .get("ValueDicts")
                    )
                    if raw_data_present:
                        logger.info(
                            "page_had_raw_data_but_normalized_to_empty",
                            entity=api_entity_name,
                            page=page_num,
                        )
                    else:
                        logger.info(
                            "no_more_records_or_empty_page_after_normalization",
                            entity=api_entity_name,
                            page=page_num,
                        )
                    break  # Interrompe se não houver mais dados normalizados

                all_normalized_rows.extend(normalized_page_rows)
                processed_records_for_entity += len(normalized_page_rows)
                RECORDS_PROCESSED.labels(entity=api_entity_name).inc(
                    len(normalized_page_rows)
                )
                logger.info(
                    "page_processed_and_normalized",
                    entity=api_entity_name,
                    page=page_num,
                    recs_in_page=len(normalized_page_rows),
                    total_recs=processed_records_for_entity,
                )

                try:
                    new_restart_tokens = page_data_response["results"][0]["result"][
                        "data"
                    ]["dsr"]["DS"][0].get("RT")
                    if new_restart_tokens:
                        if new_restart_tokens == current_restart_tokens:
                            logger.warning(
                                "duplicate_restart_tokens_received_stopping",
                                entity=api_entity_name,
                                page=page_num,
                            )
                            break
                        current_restart_tokens = new_restart_tokens
                        logger.debug(
                            "next_restart_tokens_found_for_next_page",
                            entity=api_entity_name,
                            page=page_num,
                        )
                    else:
                        logger.info(
                            "no_restart_tokens_in_response_ends_pagination",
                            entity=api_entity_name,
                            page=page_num,
                        )
                        break
                except (KeyError, IndexError, TypeError) as e:
                    logger.warning(
                        "error_extracting_restart_tokens_from_response",
                        entity=api_entity_name,
                        page=page_num,
                        error=str(e),
                    )
                    break
            except requests.exceptions.RequestException as e:
                logger.error(
                    "fetch_page_request_failed_halting_pagination",
                    entity=api_entity_name,
                    page=page_num,
                    error=str(e),
                )
                break
            except Exception as e:
                logger.error(
                    "unexpected_error_in_pagination_loop_halting",
                    entity=api_entity_name,
                    page=page_num,
                    error=str(e),
                    exc_info=True,
                )
                break

        logger.info(
            "finished_full_precatorios_fetch",
            entity=api_entity_name,
            pages_fetched=page_num,
            total_recs_aggregated=len(all_normalized_rows),
        )
        return all_normalized_rows

    def normalize_to_rows(
        self, resp_json_pages: List[Dict], starting_order_number: int = 0
    ) -> Tuple[List[Dict], int]:
        """Normaliza os dados JSON da API para uma lista de dicionários (linhas).
        Retorna as linhas normalizadas e o último número de ordem usado.
        """
        normalized_rows: List[Dict] = []
        total_raw_records_count = 0
        current_order_in_normalized_list = starting_order_number

        if not resp_json_pages or not isinstance(resp_json_pages, list):
            logger.warning(
                "normalize_to_rows_entrada_invalida", data=str(resp_json_pages)
            )
            return normalized_rows, current_order_in_normalized_list

        for page_index, resp_json in enumerate(resp_json_pages):
            if not resp_json or not isinstance(resp_json, dict):
                logger.warning(
                    "normalize_to_rows_pagina_invalida",
                    page_index=page_index,
                    page_data=str(resp_json),
                )
                continue

            try:
                data = (
                    resp_json.get("results", [{}])[0].get("result", {}).get("data", {})
                )
                if not data:
                    logger.warning("Pág %s: Sem 'data' no resultado.", page_index)
                    continue

                dsr = data.get("dsr")
                if not dsr:
                    logger.warning("Pág %s: Sem 'dsr' nos dados.", page_index)
                    continue

                current_ds_list = dsr.get("DS", [])
                if not current_ds_list:
                    logger.warning(f"Pág {page_index}: 'DS' está vazio ou ausente.")
                    continue

                current_ds = current_ds_list[0]
                value_dicts = current_ds.get("ValueDicts", {})

                ph_list = current_ds.get("PH", [])
                if not ph_list:
                    logger.warning(f"Pág {page_index}: 'PH' está vazio ou ausente.")
                    continue
                ph = ph_list[0]

                data_rows_container = ph.get("DM0")
                if data_rows_container is None:
                    logger.warning(
                        "Pág %s: Sem 'DM0' (container de linhas).", page_index
                    )
                    continue

                if (
                    isinstance(data_rows_container, list)
                    and len(data_rows_container) == 1
                    and not data_rows_container[0]
                ):
                    logger.info(
                        "Pág %s: DM0 vazio, sem dados nesta página.", page_index
                    )
                    continue

                global_descriptor_selects = data.get("descriptor", {}).get("Select", [])
                if not global_descriptor_selects:
                    logger.error(
                        "Pág %s: Sem descritores globais ('Select'). Pulando página.",
                        page_index,
                    )
                    continue

                api_name_to_csv_field_map: Dict[str, Dict[str, str]] = {}
                for csv_fld, attrs in self.field_config_instance.field_mapping.items():
                    api_n = attrs.get("api_name")
                    if api_n:
                        api_name_to_csv_field_map[api_n] = {
                            "csv_field": csv_fld,
                            "type": attrs.get("type", "str"),
                            "default": attrs.get("default"),
                        }

                data_rows = (
                    data_rows_container if isinstance(data_rows_container, list) else []
                )
                if not data_rows:
                    logger.info("Pág %s: Sem linhas de dados em DM0.", page_index)
                    continue

                total_raw_records_count += len(data_rows)
                logger.info(
                    "normalize_to_rows_processando_pagina",
                    page_index=page_index,
                    num_raw_rows=len(data_rows),
                )

                s_schema = None  # Schema da primeira linha da página
                last_processed_pydantic_row: Dict[str, Any] = {}

                for i, raw_row_data_container in enumerate(data_rows):
                    pydantic_input_row: Dict[str, Any] = {}
                    # Inicializa com defaults do field_config para garantir que todos os campos CSV existam
                    for (
                        csv_f_init,
                        csv_attrs_init,
                    ) in self.field_config_instance.field_mapping.items():
                        pydantic_input_row[csv_f_init] = self._format_value(
                            csv_attrs_init.get("default"),
                            csv_attrs_init.get("type", "str"),
                        )

                    current_c_values = raw_row_data_container.get("C", [])

                    if i == 0:  # Linha Base
                        current_s_list_from_row = raw_row_data_container.get("S")
                        if not current_s_list_from_row or not isinstance(
                            current_s_list_from_row, list
                        ):
                            logger.error(
                                f"Pág {page_index}, Linha {i} (base): schema 'S' inválido. Pulando página."
                            )
                            break
                        s_schema = current_s_list_from_row

                        if len(current_c_values) != len(s_schema):
                            logger.error(
                                f"Pág{page_index},L{i}(base): C/S len mismatch. Skip."
                                f" C:{len(current_c_values)}, S:{len(s_schema)}"
                            )
                            last_processed_pydantic_row = {}
                            continue

                        for col_idx, schema_item in enumerate(s_schema):
                            if col_idx >= len(current_c_values):  # Segurança adicional
                                logger.warning(
                                    f"Pág{page_index},L{i},C{col_idx}:C idx OOB.Skip."
                                )
                                continue
                            raw_value_for_field = current_c_values[col_idx]
                            # Obter informações do descritor global e mapeamento CSV
                            # O índice 'col_idx' é o mesmo para s_schema, current_c_values e global_descriptor_selects
                            if col_idx >= len(global_descriptor_selects):
                                logger.warning(
                                    f"Pág{page_index},L{i},C{col_idx}: Idx OOB for global_descriptors. Skip field."
                                )
                                continue

                            api_name_from_descriptor = global_descriptor_selects[
                                col_idx
                            ].get("Name")
                            base_api_name = self._get_base_field_name(
                                api_name_from_descriptor
                            )
                            csv_field_cfg = api_name_to_csv_field_map.get(base_api_name)

                            if not csv_field_cfg:
                                # logger.debug(f"Pág{page_index},L{i},C{col_idx}: API name not mapped. Skip.")
                                continue

                            target_csv_field = csv_field_cfg["csv_field"]
                            target_field_type = csv_field_cfg["type"]
                            dict_name = schema_item.get("DN")
                            val_to_assign = None
                            resolved_value = False

                            if dict_name:
                                try:
                                    actual_idx = int(raw_value_for_field)
                                    vd_list = value_dicts.get(dict_name)
                                    if isinstance(
                                        vd_list, list
                                    ) and 0 <= actual_idx < len(vd_list):
                                        val_to_assign = vd_list[actual_idx]
                                        resolved_value = True
                                    else:
                                        len_val = (
                                            len(vd_list)
                                            if vd_list is not None
                                            else "N/A"
                                        )
                                        logger.warning(
                                            f"Pág{page_index},L{i}C{col_idx}({target_csv_field}):"
                                            f"VD '{dict_name}',C idx'{raw_value_for_field}'OOB(len:{len_val}).Default."
                                        )
                                except (ValueError, TypeError):
                                    logger.warning(
                                        f"Pág{page_index},L{i}C{col_idx}({target_csv_field}):"
                                        f"VD '{dict_name}',C val'{raw_value_for_field}'not int.Default."
                                    )
                            else:  # No DN, valor literal de C
                                val_to_assign = raw_value_for_field
                                resolved_value = True

                            if resolved_value:
                                decoded = (
                                    self._decode_utf8(str(val_to_assign))
                                    if val_to_assign is not None
                                    else None
                                )
                                pydantic_input_row[target_csv_field] = (
                                    self._format_value(decoded, target_field_type)
                                )

                        last_processed_pydantic_row = pydantic_input_row.copy()

                    else:  # Linhas Delta (i > 0)
                        if (
                            not s_schema
                        ):  # s_schema deve ter sido definido pela primeira linha
                            logger.error(
                                f"Pág{page_index},L{i}(delta): Schema 'S' from base row not available. Skip page."
                            )
                            break
                        if not last_processed_pydantic_row:
                            logger.error(
                                f"Pág{page_index},L{i}(delta): Linha base anterior não processada. Pulando."
                            )
                            continue

                        rulifier_r = raw_row_data_container.get("R")
                        if rulifier_r is None:
                            logger.warning(
                                f"Pág {page_index}, Linha {i} (delta): Sem Rulifier 'R'. Herdou tudo da anterior."
                            )
                            pydantic_input_row = last_processed_pydantic_row.copy()
                        else:
                            c_delta_idx = 0
                            current_c_values_delta = raw_row_data_container.get("C", [])

                            # Inicializa pydantic_input_row como uma cópia da linha anterior processada
                            # antes de aplicar as modificações do Rulifier.
                            pydantic_input_row = last_processed_pydantic_row.copy()

                            logger.debug(
                                f"Pág{page_index},L{i} Delta: R={rulifier_r}({bin(rulifier_r)}), "
                                f"C_delta={current_c_values_delta}"
                            )

                            for col_idx, schema_item in enumerate(s_schema):
                                if col_idx >= len(
                                    global_descriptor_selects
                                ):  # Segurança
                                    logger.warning(
                                        f"Pág{page_index},L{i} Delta,C{col_idx}: Idx OOB for global_desc. Skip."
                                    )
                                    continue

                                api_name_from_descriptor = global_descriptor_selects[
                                    col_idx
                                ].get("Name")
                                base_api_name = self._get_base_field_name(
                                    api_name_from_descriptor
                                )
                                csv_field_cfg = api_name_to_csv_field_map.get(
                                    base_api_name
                                )

                                if not csv_field_cfg:
                                    # logger.debug(f"Pág{page_index},L{i} Delta,C{col_idx}: API name not mapped. Skip.")
                                    continue

                                target_csv_field = csv_field_cfg["csv_field"]
                                target_field_type = csv_field_cfg["type"]

                                # Checa o bit correspondente no Rulifier
                                # Bit 0 (Clear) = Novo Valor, Bit 1 (Set) = Herdar
                                if not (
                                    (rulifier_r >> col_idx) & 1
                                ):  # Bit é 0, Novo valor de C_delta
                                    if c_delta_idx >= len(current_c_values_delta):
                                        logger.error(
                                            f"Pág{page_index},L{i}Del({target_csv_field}):R bit0 (Novo),"
                                            f"C_delta OOB(idx{c_delta_idx}). Herdando."
                                        )
                                        pydantic_input_row[target_csv_field] = (
                                            last_processed_pydantic_row.get(
                                                target_csv_field,
                                                self._format_value(
                                                    csv_field_cfg.get("default"),
                                                    target_field_type,
                                                ),
                                            )
                                        )
                                        # Não incrementa c_delta_idx aqui pois não consumiu
                                        continue  # Pula para o próximo col_idx

                                    raw_value_from_c = current_c_values_delta[
                                        c_delta_idx
                                    ]
                                    schema_item = s_schema[col_idx]
                                    target_field_type = csv_field_cfg.get("type", "str")

                                    # Se o raw_value_from_c for uma string, é um valor direto (ou um valor formatado que deve ser tratado como string inicialmente)
                                    if isinstance(raw_value_from_c, str):
                                        processed_value = self._format_value(
                                            raw_value_from_c, target_field_type
                                        )
                                        pydantic_input_row[target_csv_field] = (
                                            processed_value
                                        )
                                        # logger.debug(f"  L{i}Del({target_csv_field}):R bit0(Novo), C_delta[{c_delta_idx}]='{raw_value_from_c}' (STR Direto) -> '{processed_value}'")
                                    elif isinstance(raw_value_from_c, (int, float)):
                                        dict_name = schema_item.get("DN")
                                        # Caso 1: É um índice para um ValueDict
                                        if dict_name:
                                            if (
                                                dict_name in value_dicts
                                                and isinstance(raw_value_from_c, int)
                                                and 0
                                                <= raw_value_from_c
                                                < len(value_dicts[dict_name])
                                            ):
                                                val_from_dict = value_dicts[dict_name][
                                                    raw_value_from_c
                                                ]
                                                processed_value = self._format_value(
                                                    val_from_dict, target_field_type
                                                )
                                                pydantic_input_row[target_csv_field] = (
                                                    processed_value
                                                )
                                                # logger.debug(f"  L{i}Del({target_csv_field}):R bit0(Novo), C_delta[{c_delta_idx}]={raw_value_from_c} (Índice VD '{dict_name}') -> DictVal '{val_from_dict}' -> '{processed_value}'")
                                            else:
                                                # Fallback para herdar se o índice do dicionário for inválido ou VD não encontrado
                                                pydantic_input_row[target_csv_field] = (
                                                    last_processed_pydantic_row.get(
                                                        target_csv_field,
                                                        csv_field_cfg.get("default"),
                                                    )
                                                )
                                                logger.warning(
                                                    f"Pág{page_index},L{i}Del({target_csv_field}):R bit0 (Novo),"
                                                    f"VD'{dict_name}',C_del idx'{raw_value_from_c}'OOB. Herdando."
                                                )
                                        # Caso 2: É um valor numérico direto (ex: ano, ordem, valor original float)
                                        else:
                                            processed_value = self._format_value(
                                                str(raw_value_from_c), target_field_type
                                            )  # _format_value espera string
                                            pydantic_input_row[target_csv_field] = (
                                                processed_value
                                            )
                                            # logger.debug(f"  L{i}Del({target_csv_field}):R bit0(Novo), C_delta[{c_delta_idx}]={raw_value_from_c} (Numérico Direto) -> '{processed_value}'")
                                    else:
                                        # Tipo inesperado em C_delta, herdar como fallback seguro
                                        pydantic_input_row[target_csv_field] = (
                                            last_processed_pydantic_row.get(
                                                target_csv_field,
                                                csv_field_cfg.get("default"),
                                            )
                                        )
                                        logger.error(
                                            f"Pág{page_index},L{i}Del({target_csv_field}):R bit0 (Novo), C_delta[{c_delta_idx}]={raw_value_from_c} (Tipo Inesperado {type(raw_value_from_c)}). Herdando."
                                        )
                                    c_delta_idx += 1

                        last_processed_pydantic_row = pydantic_input_row.copy()

                    # LOGGING ADICIONADO PARA DEBUG DE LINHAS DELTA - Removido, pois agora processamos com Rulifier

                    try:
                        precatorio_obj = Precatorio(**pydantic_input_row)
                        dumped_row = precatorio_obj.dict()

                        current_order_in_normalized_list += 1
                        dumped_row["ordem"] = current_order_in_normalized_list

                        logger.debug(
                            "pydantic_output_post_dump",
                            row_index_in_page=i,
                            page_index=page_index,
                            dumped_data=dumped_row,
                        )
                        normalized_rows.append(dumped_row)
                        self.current_entity_processed_records += 1
                        RECORDS_PROCESSED.labels(
                            entity=(
                                self.current_entity_slug
                                if hasattr(self, "current_entity_slug")
                                else "unknown_entity_norm"
                            )
                        ).inc()
                    except ValidationError as e:
                        logger.error(
                            "erro_validacao_pydantic",
                            row_index_in_page=i,
                            page_index=page_index,
                            pydantic_input=pydantic_input_row,
                            errors=e.errors(),
                        )
                    except Exception as e_gen:
                        logger.error(
                            "erro_desconhecido_durante_validacao_pydantic",
                            row_index_in_page=i,
                            page_index=page_index,
                            exception_type=str(type(e_gen)),
                            error_message=str(e_gen),
                            pydantic_input=pydantic_input_row,
                            exc_info=True,
                        )
            except Exception as e:
                logger.error(
                    "erro_processar_pagina_response",
                    page_index=page_index,
                    error=str(e),
                    exc_info=True,
                )
                continue

        logger.info(
            "normalize_to_rows_finalizado",
            total_raw_records=total_raw_records_count,
            normalized_records=len(normalized_rows),
        )
        return normalized_rows, current_order_in_normalized_list

    def write_csv(self, rows: List[Dict], out_file: str):
        """Escreve os dados em um arquivo CSV."""
        logger.info(
            "write_csv_iniciado",
            num_rows=len(rows) if rows else 0,
            output_file=out_file,
        )

        if not rows:
            logger.warning("nenhum_dado_para_escrever_csv", output_file=out_file)
            try:
                with open(out_file, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(
                        f, fieldnames=self.field_config_instance.csv_fields
                    )
                    writer.writeheader()
                logger.info("csv_vazio_com_cabecalhos_escrito", output_file=out_file)
            except Exception as e:
                logger.error(
                    "erro_escrever_csv_vazio",
                    error=str(e),
                    output_file=out_file,
                    exc_info=True,
                )
            return

        ordered_rows = []
        for i, row_data in enumerate(rows):
            ordered_row = {
                field: row_data.get(field)
                for field in self.field_config_instance.csv_fields
            }

            # Formatar data_cadastro
            data_cadastro_obj = ordered_row.get("data_cadastro")
            if isinstance(data_cadastro_obj, datetime):
                ordered_row["data_cadastro"] = data_cadastro_obj.strftime("%d/%m/%Y")
            elif data_cadastro_obj is None or str(data_cadastro_obj).strip() == "":
                ordered_row["data_cadastro"] = ""  # Ou "-" se preferir
            # Se já for string (ex: de um erro anterior ou já formatado), mantém

            # Formatar valores monetários
            for field_name in ["valor_original", "valor_atual"]:
                valor_obj = ordered_row.get(field_name)
                if isinstance(valor_obj, Decimal):
                    try:
                        ordered_row[field_name] = format_currency(float(valor_obj))
                    except Exception as e_format:
                        logger.warning(
                            f"Erro ao formatar '{field_name}' ('{valor_obj}') como moeda: {e_format}. Usando str."
                        )
                        ordered_row[field_name] = str(valor_obj)  # Fallback para string
                elif valor_obj is None:  # Se for None, formata como R$ 0,00
                    ordered_row[field_name] = format_currency(0.0)
                # Se já for string (ex: já formatado ou placeholder), mantém

            logger.debug(
                "write_csv_constructing_ordered_row",
                row_index_in_list=i,
                constructed_ordered_row=ordered_row,
                original_row_data_from_list=row_data,
            )
            # Log para a primeira linha que será escrita (após ordenação)
            if i == 0:
                logger.info(
                    "write_csv_primeira_linha_ordenada_para_escrita",
                    primeira_linha=ordered_row,
                )
            ordered_rows.append(ordered_row)

        try:
            with open(out_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f, fieldnames=self.field_config_instance.csv_fields
                )
                writer.writeheader()
                # writer.writerows(ordered_rows) # Comentado para loop manual
                for i, single_ordered_row in enumerate(ordered_rows):
                    logger.debug(
                        "write_csv_writing_single_row",
                        row_index=i,
                        row_data_to_write=single_ordered_row,
                        row_data_types={
                            k: str(type(v)) for k, v in single_ordered_row.items()
                        },
                    )
                    writer.writerow(single_ordered_row)
            logger.info(
                f"Dados escritos em {out_file}", num_rows_written=len(ordered_rows)
            )
        except Exception as e:
            logger.error(
                "erro_escrever_csv", error=str(e), output_file=out_file, exc_info=True
            )

    @track_time
    def crawl(self, entity_slug: str, out_file: str):
        """Executa o processo completo de crawling para uma entidade."""
        try:
            logger.info("crawl_start", entity=entity_slug, output_file=out_file)

            # Busca os dados
            # Para o crawl completo, não especificamos 'count', então ele tentará buscar tudo com paginação.
            logger.info("fetching_data", entity=entity_slug)
            raw_data = self.fetch_all_precatorios_data(
                entity_slug
            )  # Não passa count aqui para buscar tudo

            if not raw_data:
                logger.warning("no_raw_data", entity=entity_slug)
                return

            # Normaliza os dados
            logger.info("normalizing_data", entity=entity_slug)
            rows, _ = self.normalize_to_rows(raw_data)

            if not rows:
                logger.warning("no_normalized_data", entity=entity_slug)
                return

            # Escreve o CSV
            logger.info(
                "writing_csv", entity=entity_slug, records=len(rows), file=out_file
            )
            self.write_csv(rows, out_file)

            logger.info(
                "crawl_complete",
                entity=entity_slug,
                records=len(rows),
                file=out_file,
            )

        except Exception as e:
            logger.error(
                "crawl_error",
                error=str(e),
                entity=entity_slug,
                output_file=out_file,
                exc_info=True,
            )
            raise

    def _get_base_field_name(self, api_name_str: str) -> str:
        """Obtém o nome base do campo a partir do nome da API."""
        # Ex: 'Sum(dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_num_ano_orcamento)' -> 'dfslcp_num_ano_orcamento'
        match = re.match(
            r"^[A-Za-z_0-9]+\\(([^)]+)\\)$", api_name_str
        )  # Matches Agg(Content)
        if match:
            content_inside_agg = match.group(1)
            if "." in content_inside_agg:
                return content_inside_agg.split(".")[-1]
            return content_inside_agg

        if "." in api_name_str:
            return api_name_str.split(".")[-1]
        return api_name_str
