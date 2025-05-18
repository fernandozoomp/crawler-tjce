#!/usr/bin/env python3
import csv
import json
from datetime import datetime
import os
from typing import Dict, List, Optional, Union
import locale
import uuid
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, stop_after_attempt, wait_exponential
import re

import requests
from pydantic import ValidationError

from config_new import config, field_config as field_cfg
from config import PAYLOAD_STRUCTURE
from logger import get_logger
from models import Precatorio
from metrics import REQUESTS_TOTAL, RECORDS_PROCESSED, track_time

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
        self.field_config_instance = field_cfg
        self.api_url = self.config_instance.api_url
        self.resource_key = self.config_instance.resource_key
        self.headers = self.config_instance.headers
        self.current_entity_processed_records = 0
        self.session = requests.Session()
        self.session.headers.update(self.headers)

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
        if value is None or value == "":
            return "-"

        try:
            # Processo deve ser sempre string
            if field_type == "processo":
                if isinstance(value, (int, float)):
                    return f"{value:020.0f}"
                return str(value).strip()

            # Campos numéricos
            if field_type in ["int", "float"]:
                # Se for número de processo, não converter
                if isinstance(value, str) and ("-" in value or "." in value):
                    return value

                # Tenta converter para float/int
                if isinstance(value, str):
                    value = value.replace("R$", "").replace(".", "").replace(",", ".")

                try:
                    if field_type == "int":
                        return str(int(float(value)))
                    return str(float(value))
                except (ValueError, TypeError):
                    return "0"

            # Data
            if field_type == "date":
                if isinstance(value, str):
                    if "datetime" in value:
                        # Remove a string 'datetime' e parênteses
                        value = value.replace("datetime", "").strip("()")
                        # Separa os componentes
                        components = [int(x) for x in value.split(",")]
                        return str(datetime(*components))
                    elif value.isdigit() and len(str(value)) > 8:
                        # Timestamp em milissegundos
                        return str(datetime.fromtimestamp(float(value) / 1000))
                return str(value)

            # String
            return str(value).strip()

        except Exception as e:
            logger.warning(
                {
                    "value": value,
                    "field_type": field_type,
                    "error": str(e),
                    "event": "format_error",
                }
            )
            if field_type in ["int", "float"]:
                return "0"
            return "-"

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    @track_time(
        entity=lambda self_or_args, *args, **kwargs: (
            self_or_args.config_instance.default_entity
            if hasattr(self_or_args, "config_instance")
            else config.default_entity
        )
    )
    def _fetch_page(self, entity: str, restart_tokens: Optional[str] = None) -> Dict:
        """Busca uma página de dados da API."""
        current_headers = self.session.headers.copy()
        current_headers.update(
            {"ActivityId": str(uuid.uuid4()), "RequestId": str(uuid.uuid4())}
        )

        payload = PAYLOAD_STRUCTURE.copy()
        if restart_tokens:
            payload["queries"][0]["Query"]["Commands"][0][
                "SemanticQueryDataShapeCommand"
            ]["Binding"]["DataReduction"]["Primary"]["Window"]["RestartTokens"] = [
                restart_tokens
            ]
            payload["queries"][0]["Query"]["Commands"][0][
                "SemanticQueryDataShapeCommand"
            ]["Query"]["OrderBy"][0]["Direction"] = 2
        else:
            payload["queries"][0]["Query"]["Commands"][0][
                "SemanticQueryDataShapeCommand"
            ]["Query"]["OrderBy"][0]["Direction"] = 1

        payload["queries"][0]["Query"]["Commands"][0]["SemanticQueryDataShapeCommand"][
            "Query"
        ]["Where"][0]["Condition"]["In"]["Values"][0][0]["Literal"][
            "Value"
        ] = f"'{entity}'"

        REQUESTS_TOTAL.labels(entity=entity).inc()
        response = self.session.post(self.api_url, json=payload, timeout=30)
        response.raise_for_status()

        return response.json()

    def fetch_data(self, entity: str) -> List[Dict]:
        """Busca dados da API do Power BI para a entidade especificada."""
        page_count = 0
        all_results = []
        restart_tokens = None
        total_records = 0

        while True:
            page_count += 1

            try:
                data = self._fetch_page(entity, restart_tokens)

                if not data.get("results"):
                    logger.warning(
                        "no_results",
                        page=page_count,
                        total_records=total_records,
                    )
                    break

                result_data = data["results"][0]["result"]["data"]
                dsr = result_data.get("dsr", {})

                if not dsr:
                    logger.warning("missing_dsr", page=page_count)
                    break

                ds_list = dsr.get("DS", [{}])
                if not ds_list:
                    logger.warning("missing_ds", page=page_count)
                    break

                ds = ds_list[0]
                ph_list = ds.get("PH", [{}])
                if not ph_list:
                    logger.warning("missing_ph", page=page_count)
                    break

                records = ph_list[0].get("DM0", [])
                records_in_page = len(records)
                total_records += records_in_page

                logger.info(
                    "page_fetched",
                    page=page_count,
                    records_in_page=records_in_page,
                    total_records=total_records,
                )

                all_results.append(data)

                # Verifica se há mais páginas
                if records_in_page < 50:  # Tamanho padrão da página
                    logger.info(
                        "no_more_pages",
                        page=page_count,
                        total_records=total_records,
                    )
                    break

                # Obtém o token para a próxima página
                restart_tokens = (
                    result_data.get("descriptor", {})
                    .get("Select", [{}])[0]
                    .get("Expressions", {})
                    .get("Primary", {})
                    .get("Groupings", [{}])[0]
                    .get("RestartTokens")
                )

                if not restart_tokens:
                    logger.info(
                        "no_restart_tokens",
                        page=page_count,
                        total_records=total_records,
                    )
                    break

            except Exception as e:
                logger.error(
                    "fetch_error",
                    error=str(e),
                    page=page_count,
                    exc_info=True,
                )
                break

        logger.info(
            "fetch_complete",
            total_pages=page_count,
            total_records=total_records,
        )

        return all_results

    def _get_base_field_name(self, api_name_str: str) -> str:
        if not isinstance(api_name_str, str):
            # Attempt to convert to string if it's not, though descriptor names should be strings
            return str(api_name_str)

        # Remove "Sum(...)" or similar aggregations like "Min(...)", "Max(...)", etc.
        # and also handles simple "Table.Column" by extracting "Column"
        # e.g. Sum(dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_num_ano_orcamento) -> dfslcp_num_ano_orcamento
        # e.g. dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_dsc_proc_precatorio -> dfslcp_dsc_proc_precatorio
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

    def normalize_to_rows(self, resp_json_pages: List[Dict]) -> List[Dict]:
        all_rows: List[Dict] = []
        if not resp_json_pages:
            return all_rows

        # --- Pré-processamento para mapear G_identifier para campos CSV (usando a primeira página) ---
        first_page_data = (
            resp_json_pages[0].get("results", [{}])[0].get("result", {}).get("data", {})
        )
        global_descriptor_select_list = first_page_data.get("descriptor", {}).get(
            "Select", []
        )

        g_identifier_to_csv_field_map: Dict[str, Dict[str, Any]] = {}
        for (
            csv_field_name,
            config_details,
        ) in self.field_config_instance.field_mapping.items():
            target_api_name_from_config = config_details.get("api_name")
            if not target_api_name_from_config:
                continue

            found_in_descriptor = False
            for descriptor_entry in global_descriptor_select_list:
                name_in_descriptor = descriptor_entry.get("Name")
                if name_in_descriptor:
                    base_name_in_descriptor = self._get_base_field_name(
                        name_in_descriptor
                    )
                    if base_name_in_descriptor == target_api_name_from_config:
                        g_identifier = descriptor_entry.get("Value")
                        if g_identifier:
                            g_identifier_to_csv_field_map[g_identifier] = {
                                "csv_name": csv_field_name,
                                "type": config_details.get("type"),
                                "default": config_details.get("default"),
                            }
                            found_in_descriptor = True
                            break
            if not found_in_descriptor:
                logger.debug(
                    f"API name '{target_api_name_from_config}' for CSV field '{csv_field_name}' not found in global descriptor."
                )

        logger.debug(f"G-Identifier to CSV Field Map: {g_identifier_to_csv_field_map}")

        row_idx_global = 0
        total_raw_entries = 0

        for page_idx, resp_json_item in enumerate(resp_json_pages):
            try:
                page_data = (
                    resp_json_item.get("results", [{}])[0]
                    .get("result", {})
                    .get("data", {})
                )
                if not page_data:
                    logger.warning(
                        f"Página {page_idx} não contém 'results[0].result.data'. Conteúdo: {resp_json_item}"
                    )
                    continue

                dsr = page_data.get("dsr", {})
                if not dsr:
                    logger.warning(
                        f"Página {page_idx} não contém 'dsr' em data. Conteúdo: {page_data}"
                    )
                    continue

                ds_list = dsr.get("DS")
                if not ds_list or not isinstance(ds_list, list) or not ds_list[0]:
                    logger.warning(
                        f"Página {page_idx} não contém 'DS[0]' ou está vazio em dsr. Conteúdo: {dsr}"
                    )
                    continue

                ds0 = ds_list[0]
                ph_list = ds0.get("PH")
                if not ph_list or not isinstance(ph_list, list) or not ph_list[0]:
                    logger.warning(
                        f"Página {page_idx} não contém 'PH[0]' ou está vazio em DS[0]. Conteúdo: {ds0}"
                    )
                    continue

                ph0 = ph_list[0]
                dm0_data = ph0.get("DM0", [])
                value_dicts = ds0.get("ValueDicts", {})
                page_level_s_descriptors = ph0.get(
                    "S", []
                )  # S descritores no nível da página/PH0

                if (
                    not dm0_data and page_idx == 0 and not resp_json_pages[1:]
                ):  # only warn if it's the only page and it's empty
                    logger.info(
                        f"DM0 está vazio na página {page_idx}. Não há dados para normalizar nesta página."
                    )

                total_raw_entries += len(dm0_data)

                for dm_item_idx, dm_item in enumerate(dm0_data):
                    if not isinstance(dm_item, dict):
                        logger.warning(
                            f"Item {dm_item_idx} em DM0 na página {page_idx} não é um dicionário: {dm_item}"
                        )
                        continue

                    row_s_descriptors = dm_item.get("S", page_level_s_descriptors)
                    c_values = dm_item.get("C")

                    if not isinstance(c_values, list):
                        logger.warning(
                            f"DM item {dm_item_idx} (pág {page_idx}) não tem uma lista 'C' ou 'C' está vazio. Item: {dm_item}"
                        )
                        continue

                    if not row_s_descriptors:
                        logger.warning(
                            f"DM item {dm_item_idx} (pág {page_idx}) não tem descritores 'S' (nem local, nem a nível de página). Item: {dm_item}"
                        )
                        continue

                    row_dict_raw = {}
                    for val_idx, value_from_c in enumerate(c_values):
                        if val_idx >= len(row_s_descriptors):
                            logger.warning(
                                f"Mais valores em C ({len(c_values)}) do que descritores S ({len(row_s_descriptors)}) "
                                f"para o item {dm_item_idx} na página {page_idx}. Ignorando valores C extras."
                            )
                            break

                        s_desc_for_this_value = row_s_descriptors[val_idx]
                        if not isinstance(s_desc_for_this_value, dict):
                            logger.warning(
                                f"Descritor S na posição {val_idx} não é um dicionário: {s_desc_for_this_value}"
                            )
                            continue

                        g_identifier = s_desc_for_this_value.get("N")  # e.g. "G0", "G1"
                        if not g_identifier:
                            logger.debug(
                                f"Descritor S {s_desc_for_this_value} não tem 'N' (G-identifier)."
                            )
                            continue

                        field_processing_info = g_identifier_to_csv_field_map.get(
                            g_identifier
                        )

                        if field_processing_info:
                            csv_field_name = field_processing_info["csv_name"]
                            field_type = field_processing_info["type"]

                            value_to_process = value_from_c
                            dict_name_if_indexed = s_desc_for_this_value.get(
                                "DN"
                            )  # e.g. "D0"

                            if dict_name_if_indexed:
                                if dict_name_if_indexed in value_dicts and isinstance(
                                    value_dicts[dict_name_if_indexed], list
                                ):
                                    try:
                                        actual_idx = int(
                                            value_from_c
                                        )  # value_from_c is the index
                                        if (
                                            0
                                            <= actual_idx
                                            < len(value_dicts[dict_name_if_indexed])
                                        ):
                                            value_to_process = value_dicts[
                                                dict_name_if_indexed
                                            ][actual_idx]
                                        else:
                                            logger.warning(
                                                f"Índice {actual_idx} fora dos limites para ValueDict '{dict_name_if_indexed}' (tamanho {len(value_dicts[dict_name_if_indexed])}). Usando valor original do índice."
                                            )
                                            # value_to_process remains the index itself, might cause issues later
                                    except (ValueError, TypeError):
                                        logger.warning(
                                            f"Não foi possível converter o índice '{value_from_c}' para int para ValueDict '{dict_name_if_indexed}'. Usando valor original."
                                        )
                                        # value_to_process remains the original value_from_c
                                else:
                                    logger.warning(
                                        f"ValueDict '{dict_name_if_indexed}' não encontrado ou não é uma lista em value_dicts."
                                    )
                                    # value_to_process remains the original value_from_c

                            formatted_value = self._format_value(
                                str(value_to_process), field_type
                            )  # Ensure value is string for _format_value
                            row_dict_raw[csv_field_name] = formatted_value
                        else:
                            logger.debug(
                                f"G-identifier '{g_identifier}' (valor: {value_from_c}) dos descritores S não mapeado para nenhum campo CSV."
                            )

                    # Aplicar defaults para campos CSV que não foram preenchidos a partir dos dados C
                    for g_id_key, f_info_val in g_identifier_to_csv_field_map.items():
                        csv_n = f_info_val["csv_name"]
                        if csv_n not in row_dict_raw:
                            default_val = f_info_val.get("default")
                            f_type = f_info_val.get("type")
                            if default_val is not None:
                                row_dict_raw[csv_n] = self._format_value(
                                    str(default_val), f_type
                                )
                            # Se não houver default e o campo for obrigatório, Pydantic irá falhar, o que é esperado.

                    try:
                        # Garantir que todos os campos definidos em csv_fields existam, aplicando default genérico se necessário
                        # Esta etapa é mais para garantir que o dicionário tenha todas as chaves esperadas pelo modelo Precatorio
                        # antes da validação, especialmente se alguns G-identifiers não foram encontrados/mapeados.
                        # Pydantic cuidará da validação de tipo e obrigatoriedade.
                        for final_csv_field in self.field_config_instance.csv_fields:
                            if final_csv_field not in row_dict_raw:
                                # Tenta pegar o default da config para este campo específico se não foi mapeado por G-identifier
                                default_value_final = (
                                    self.field_config_instance.field_mapping.get(
                                        final_csv_field, {}
                                    ).get("default")
                                )
                                field_type_final = (
                                    self.field_config_instance.field_mapping.get(
                                        final_csv_field, {}
                                    ).get("type", "str")
                                )
                                if default_value_final is not None:
                                    row_dict_raw[final_csv_field] = self._format_value(
                                        str(default_value_final), field_type_final
                                    )
                                # else: # Se ainda estiver faltando, Pydantic lidará com isso.
                                #    logger.debug(f"Campo final {final_csv_field} não presente em row_dict_raw e sem default na config, Pydantic validará.")

                        validated_data = Precatorio(**row_dict_raw)
                        all_rows.append(validated_data.model_dump())
                        logger.debug(
                            f"Linha normalizada {row_idx_global}: {validated_data.model_dump_json()}"
                        )
                        row_idx_global += 1
                    except ValidationError as e:
                        logger.error(
                            f"Erro de validação Pydantic para a linha bruta: {row_dict_raw} na página {page_idx}, item {dm_item_idx}. Erros: {e.errors()}",
                            exc_info=False,  # Não precisamos do traceback completo aqui, e.errors() é suficiente
                        )
                        # Opcionalmente, adicione a linha bruta a uma lista de erros ou logue-a em um arquivo separado
                    except Exception as e_inner:
                        logger.error(
                            f"Erro inesperado ao validar/processar linha {row_dict_raw} na página {page_idx}, item {dm_item_idx}: {e_inner}",
                            exc_info=True,
                        )

            except Exception as e_outer:
                logger.error(
                    f"Erro crítico ao normalizar página {page_idx}: {e_outer}",
                    exc_info=True,
                    extra={
                        "problematic_page_data": resp_json_item
                    },  # Logar a página problemática inteira
                )
                continue  # Pula para a próxima página se esta falhar catastroficamente

        if total_raw_entries > 0:
            logger.info(
                f"Normalizadas {len(all_rows)} linhas de {total_raw_entries} entradas totais em {len(resp_json_pages)} páginas."
            )
        elif not resp_json_pages:
            logger.info("Nenhuma página de dados para normalizar.")
        else:
            logger.info("Nenhuma entrada encontrada nas páginas para normalizar.")

        return all_rows

    def write_csv(self, rows: List[Dict], out_file: str):
        """Escreve os dados normalizados em um arquivo CSV."""
        if not rows:
            logger.warning("no_data")
            return

        mode = "w" if not os.path.exists(out_file) else "a"
        write_header = mode == "w"

        try:
            with open(out_file, mode, newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=self.field_config_instance.csv_fields,
                    quoting=csv.QUOTE_MINIMAL,
                    quotechar='"',
                    delimiter=",",
                )
                if write_header:
                    writer.writeheader()
                writer.writerows(rows)
            logger.info("save_complete", records=len(rows), file=out_file)
        except IOError as e:
            logger.error("save_error", error=str(e), exc_info=True)
            raise

    def crawl(self, entity: str, out_file: str):
        """Executa o processo completo de crawling para uma entidade."""
        try:
            logger.info("crawl_start", entity=entity, output_file=out_file)

            # Busca os dados
            logger.info("fetching_data", entity=entity)
            raw_data = self.fetch_data(entity)

            if not raw_data:
                logger.warning("no_raw_data", entity=entity)
                return

            # Normaliza os dados
            logger.info("normalizing_data", entity=entity)
            rows = self.normalize_to_rows(raw_data)

            if not rows:
                logger.warning("no_normalized_data", entity=entity)
                return

            # Escreve o CSV
            logger.info("writing_csv", entity=entity, records=len(rows), file=out_file)
            self.write_csv(rows, out_file)

            logger.info(
                "crawl_complete",
                entity=entity,
                records=len(rows),
                file=out_file,
            )

        except Exception as e:
            logger.error(
                "crawl_error",
                error=str(e),
                entity=entity,
                output_file=out_file,
                exc_info=True,
            )
            raise

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    @track_time(entity="all")
    def fetch_entities(self) -> List[str]:
        """Busca a lista de entidades disponíveis."""
        payload = {
            "version": "1.0.0",
            "queries": [
                {
                    "Query": {
                        "Commands": [
                            {
                                "SemanticQueryDataShapeCommand": {
                                    "Query": {
                                        "Version": 2,
                                        "From": [
                                            {
                                                "Name": "d",
                                                "Entity": "dfslcp_SAPRE_LISTA_CRONO_PRECATORIO",
                                                "Type": 0,
                                            }
                                        ],
                                        "Select": [
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "d"}
                                                    },
                                                    "Property": "dfslcp_dsc_entidade",
                                                },
                                                "Name": "dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_dsc_entidade",
                                            }
                                        ],
                                    },
                                    "Binding": {
                                        "Primary": {
                                            "Groupings": [{"Projections": [0]}]
                                        },
                                        "DataReduction": {
                                            "DataVolume": 3,
                                            "Primary": {"Window": {}},
                                        },
                                        "IncludeEmptyGroups": True,
                                        "Version": 1,
                                    },
                                }
                            }
                        ],
                        "QueryId": "",
                    }
                }
            ],
            "cancelQueries": [],
            "modelId": 4287487,
        }

        try:
            logger.info("fetching_entities")
            response = self.session.post(self.api_url, json=payload)
            response.raise_for_status()
            data = response.json()

            if not data.get("results"):
                return []

            result_data = data["results"][0]["result"]["data"]
            dsr = result_data.get("dsr", {})

            if not dsr:
                return []

            ds_list = dsr.get("DS", [])
            if not ds_list:
                return []

            entities = []
            for ds in ds_list:
                if "PH" not in ds:
                    continue

                for ph in ds["PH"]:
                    if "DM0" not in ph:
                        continue

                    for dm0 in ph["DM0"]:
                        if "G0" in dm0:
                            entity = dm0["G0"].strip()
                            # Ignora o item de seleção e remove aspas
                            if entity != "--- Selecione a Entidade":
                                # Decodifica o nome da entidade antes de adicionar à lista
                                decoded_entity = self._decode_utf8(entity.strip("'"))
                                entities.append(decoded_entity)

            # Remove duplicatas e ordena
            entities = sorted(set(entities))

            logger.info("entities_found", count=len(entities))
            return entities

        except Exception as e:
            logger.error("entities_error", error=str(e), exc_info=True)
            raise

    def save_entities(self, entities: List[str], out_file: str):
        """Salva a lista de entidades em um arquivo CSV."""
        try:
            mode = "w" if not os.path.exists(out_file) else "a"
            write_header = mode == "w"

            with open(out_file, mode, newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(["entidade"])
                for entity in entities:
                    writer.writerow([entity])

            logger.info("entities_saved", file=out_file, count=len(entities))

        except IOError as e:
            logger.error("save_error", error=str(e), exc_info=True)
            raise
