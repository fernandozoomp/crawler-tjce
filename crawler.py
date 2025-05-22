#!/usr/bin/env python3
import csv
import json
from datetime import datetime, timedelta
import os
from typing import Dict, List, Optional, Union, Any
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

from config import config, field_config as field_cfg, PAYLOAD_STRUCTURE
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
    @track_time(
        entity=lambda self_or_args, *args, **kwargs: (
            self_or_args.config_instance.default_entity
            if hasattr(self_or_args, "config_instance")
            else config.default_entity
        )
    )
    def _fetch_page(
        self,
        entity: str,
        restart_tokens: Optional[str] = None,
        count: Optional[int] = None,
    ) -> Dict:
        """Busca uma página de dados da API."""
        current_headers = self.session.headers.copy()
        current_headers.update(
            {"ActivityId": str(uuid.uuid4()), "RequestId": str(uuid.uuid4())}
        )

        payload = deepcopy(PAYLOAD_STRUCTURE)

        # Modificar o payload se 'count' for fornecido
        if count is not None:
            try:
                # Ajusta o Count para o número de registros desejado
                payload["queries"][0]["Query"]["Commands"][0][
                    "SemanticQueryDataShapeCommand"
                ]["Binding"]["DataReduction"]["Primary"]["Window"]["Count"] = count

                # Ajusta o DataVolume. Se count for pequeno, podemos usar count.
                # Para counts maiores, o DataVolume original (ex: 30) ou count pode ser usado.
                # Vamos usar count aqui para simplificar, assumindo que a API lida bem.
                # Se isso causar problemas (ex: menos registros que 'count' retornados),
                # pode ser necessário manter DataVolume maior ou igual a Count.
                # No nosso teste com 1 registro, DataVolume:3 e Count:1 funcionou.
                # A estrutura original do PAYLOAD_STRUCTURE já tem DataVolume, podemos mantê-lo ou ajustá-lo.
                # Por agora, vamos ajustar apenas o Window.Count que é o mais direto para o limite.
                # Se count=1, idealmente DataVolume deve ser um pouco maior como 3, conforme descoberto.
                # Vamos generalizar isso: se count for pequeno, aumentar DataVolume.
                # Para um controle mais fino, essa lógica pode ser expandida.
                # Por enquanto, focaremos no Window.Count.
                # A estrutura do PAYLOAD_STRUCTURE já tem um DataReduction.DataVolume.
                # Vamos também ajustar o DataReduction.DataVolume e o DataReduction.Primary.Count (se existir).

                primary_binding = payload["queries"][0]["Query"]["Commands"][0][
                    "SemanticQueryDataShapeCommand"
                ]["Binding"]

                if "DataReduction" in primary_binding:
                    if "DataVolume" in primary_binding["DataReduction"]:
                        # Se count for 1, mantemos DataVolume 3. Caso contrário, pode ser count ou um valor maior.
                        # A estrutura padrão tem 30. Vamos usar count, mas no mínimo 3.
                        primary_binding["DataReduction"]["DataVolume"] = (
                            max(count, 3) if count > 0 else 30
                        )

                    if (
                        "Primary" in primary_binding["DataReduction"]
                        and "Count" in primary_binding["DataReduction"]["Primary"]
                    ):
                        # Este 'Count' (não o Window.Count) às vezes é usado como um limite geral.
                        primary_binding["DataReduction"]["Primary"]["Count"] = count

                logger.info(f"Payload modificado para buscar {count} registros.")

            except (KeyError, IndexError) as e:
                logger.error(
                    f"Erro ao tentar modificar o payload para o count: {e}. Usando payload padrão."
                )
                # payload permanece como a cópia do PAYLOAD_STRUCTURE original

        # Lógica original para restart_tokens e paginação
        if restart_tokens:
            # Esta seção pode precisar de ajuste se a estrutura de RestartTokens não for sob "Window"
            # no PAYLOAD_STRUCTURE quando buscamos múltiplos registros.
            # Por enquanto, vamos assumir que a estrutura original do PAYLOAD_STRUCTURE em config.py
            # (para múltiplos registros) tenha DataReduction.Primary.Window.
            if (
                "Window"
                in payload["queries"][0]["Query"]["Commands"][0][
                    "SemanticQueryDataShapeCommand"
                ]["Binding"]["DataReduction"]["Primary"]
            ):
                payload["queries"][0]["Query"]["Commands"][0][
                    "SemanticQueryDataShapeCommand"
                ]["Binding"]["DataReduction"]["Primary"]["Window"]["RestartTokens"] = [
                    restart_tokens
                ]
                payload["queries"][0]["Query"]["Commands"][0][
                    "SemanticQueryDataShapeCommand"
                ]["Query"]["OrderBy"][0][
                    "Direction"
                ] = 2  # Ordem descendente para próxima página
            else:
                logger.warning(
                    "Estrutura de paginacao (Window) nao encontrada no PAYLOAD_STRUCTURE "
                    "ao tentar aplicar restart_tokens."
                )
        else:
            # Para a primeira página, garantir a direção correta da ordenação (se houver)
            if payload["queries"][0]["Query"]["Commands"][0][
                "SemanticQueryDataShapeCommand"
            ]["Query"].get("OrderBy"):
                payload["queries"][0]["Query"]["Commands"][0][
                    "SemanticQueryDataShapeCommand"
                ]["Query"]["OrderBy"][0][
                    "Direction"
                ] = 1  # Ordem ascendente para primeira página

        # Atualiza o nome da entidade na cláusula Where
        payload["queries"][0]["Query"]["Commands"][0]["SemanticQueryDataShapeCommand"][
            "Query"
        ]["Where"][0]["Condition"]["In"]["Values"][0][0]["Literal"][
            "Value"
        ] = f"'{entity}'"

        REQUESTS_TOTAL.labels(entity=entity).inc()
        logger.info(
            "Efetuando requisição para API Power BI",
            payload_binding_details=payload["queries"][0]["Query"]["Commands"][0][
                "SemanticQueryDataShapeCommand"
            ]["Binding"],
        )
        response = self.session.post(self.api_url, json=payload, timeout=30)
        response.raise_for_status()

        response_json = response.json()
        return response_json

    def fetch_data(
        self,
        entity_slug_or_official_name: str,
        count: Optional[int] = None,
        page: int = 0,
    ) -> Optional[Dict[str, Any]]:
        """
        Busca os dados brutos da API do TJCE para uma entidade específica.
        Agora aceita tanto o slug quanto o nome oficial, mas converterá para nome oficial.
        """
        official_entity_name = get_api_entity_name(entity_slug_or_official_name)
        logger.info(
            f"Buscando dados para: '{official_entity_name}' "
            f"(slug: '{entity_slug_or_official_name}'), pág: {page}, cont: {count}"
        )

        restart_tokens: Optional[str] = None  # Inicializa restart_tokens

        # Cria uma cópia profunda do payload base para evitar modificações acidentais
        payload = deepcopy(PAYLOAD_STRUCTURE)

        # Modifica o payload para a entidade específica
        # Acessa a lista de Conditions dentro do Where (assumindo que sempre existe)
        # O payload original usa 'MUNICÍPIO DE FORTALEZA'
        # Precisamos substituir isso pela official_entity_name correta
        try:
            payload["queries"][0]["Query"]["Commands"][0][
                "SemanticQueryDataShapeCommand"
            ]["Query"]["Where"][0]["Condition"]["In"]["Values"][0][0]["Literal"][
                "Value"
            ] = f"'{official_entity_name}'"
        except (KeyError, IndexError) as e:
            logger.error(
                f"Erro ao modificar payload para entidade '{official_entity_name}'. "
                f"Estrutura PAYLOAD_STRUCTURE inválida. Detalhes: {e}",
                payload_structure=PAYLOAD_STRUCTURE,
            )
            raise ValueError(
                f"Não foi possível definir a entidade no payload da requisição: {e}"
            )

        # Modifica o payload para incluir o 'count' se fornecido
        if count is not None:
            try:
                # Ajusta o Count para o número de registros desejado
                payload["queries"][0]["Query"]["Commands"][0][
                    "SemanticQueryDataShapeCommand"
                ]["Binding"]["DataReduction"]["Primary"]["Window"]["Count"] = count

                # Ajusta o DataVolume. Se count for pequeno, podemos usar count.
                # Para counts maiores, o DataVolume original (ex: 30) ou count pode ser usado.
                # Vamos usar count aqui para simplificar, assumindo que a API lida bem.
                # Se isso causar problemas (ex: menos registros que 'count' retornados),
                # pode ser necessário manter DataVolume maior ou igual a Count.
                # No nosso teste com 1 registro, DataVolume:3 e Count:1 funcionou.
                # A estrutura original do PAYLOAD_STRUCTURE já tem DataVolume, podemos mantê-lo ou ajustá-lo.
                # Por agora, vamos ajustar apenas o Window.Count que é o mais direto para o limite.
                # Se count=1, idealmente DataVolume deve ser um pouco maior como 3, conforme descoberto.
                # Vamos generalizar isso: se count for pequeno, aumentar DataVolume.
                # Para um controle mais fino, essa lógica pode ser expandida.
                # Por enquanto, focaremos no Window.Count.
                # A estrutura do PAYLOAD_STRUCTURE já tem um DataReduction.DataVolume.
                # Vamos também ajustar o DataReduction.DataVolume e o DataReduction.Primary.Count (se existir).

                primary_binding = payload["queries"][0]["Query"]["Commands"][0][
                    "SemanticQueryDataShapeCommand"
                ]["Binding"]

                if "DataReduction" in primary_binding:
                    if "DataVolume" in primary_binding["DataReduction"]:
                        # Se count for 1, mantemos DataVolume 3. Caso contrário, pode ser count ou um valor maior.
                        # A estrutura padrão tem 30. Vamos usar count, mas no mínimo 3.
                        primary_binding["DataReduction"]["DataVolume"] = (
                            max(count, 3) if count > 0 else 30
                        )

                    if (
                        "Primary" in primary_binding["DataReduction"]
                        and "Count" in primary_binding["DataReduction"]["Primary"]
                    ):
                        # Este 'Count' (não o Window.Count) às vezes é usado como um limite geral.
                        primary_binding["DataReduction"]["Primary"]["Count"] = count

                logger.info(f"Payload modificado para buscar {count} registros.")

            except (KeyError, IndexError) as e:
                logger.error(
                    f"Erro ao tentar modificar o payload para o count: {e}. Usando payload padrão."
                )
                # payload permanece como a cópia do PAYLOAD_STRUCTURE original

        # Lógica original para restart_tokens e paginação
        if restart_tokens:
            # Esta seção pode precisar de ajuste se a estrutura de RestartTokens não for sob "Window"
            # no PAYLOAD_STRUCTURE quando buscamos múltiplos registros.
            # Por enquanto, vamos assumir que a estrutura original do PAYLOAD_STRUCTURE em config.py
            # (para múltiplos registros) tenha DataReduction.Primary.Window.
            if (
                "Window"
                in payload["queries"][0]["Query"]["Commands"][0][
                    "SemanticQueryDataShapeCommand"
                ]["Binding"]["DataReduction"]["Primary"]
            ):
                payload["queries"][0]["Query"]["Commands"][0][
                    "SemanticQueryDataShapeCommand"
                ]["Binding"]["DataReduction"]["Primary"]["Window"]["RestartTokens"] = [
                    restart_tokens
                ]
                payload["queries"][0]["Query"]["Commands"][0][
                    "SemanticQueryDataShapeCommand"
                ]["Query"]["OrderBy"][0][
                    "Direction"
                ] = 2  # Ordem descendente para próxima página
            else:
                logger.warning(
                    "Estrutura de paginacao (Window) nao encontrada no PAYLOAD_STRUCTURE "
                    "ao tentar aplicar restart_tokens."
                )
        else:
            # Para a primeira página, garantir a direção correta da ordenação (se houver)
            if payload["queries"][0]["Query"]["Commands"][0][
                "SemanticQueryDataShapeCommand"
            ]["Query"].get("OrderBy"):
                payload["queries"][0]["Query"]["Commands"][0][
                    "SemanticQueryDataShapeCommand"
                ]["Query"]["OrderBy"][0][
                    "Direction"
                ] = 1  # Ordem ascendente para primeira página

        # Atualiza o nome da entidade na cláusula Where
        payload["queries"][0]["Query"]["Commands"][0]["SemanticQueryDataShapeCommand"][
            "Query"
        ]["Where"][0]["Condition"]["In"]["Values"][0][0]["Literal"][
            "Value"
        ] = f"'{official_entity_name}'"

        REQUESTS_TOTAL.labels(entity=official_entity_name).inc()
        logger.info(
            "Efetuando requisição para API Power BI",
            payload_binding_details=payload["queries"][0]["Query"]["Commands"][0][
                "SemanticQueryDataShapeCommand"
            ]["Binding"],
        )
        response = self.session.post(self.api_url, json=payload, timeout=30)
        response.raise_for_status()

        response_json = response.json()
        return response_json

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
        """Normaliza os dados JSON da API para uma lista de dicionários (linhas)."""
        normalized_rows: List[Dict] = []
        total_raw_records_count = 0
        current_order_in_normalized_list = 0  # Contador para o campo 'ordem'

        if not resp_json_pages or not isinstance(resp_json_pages, list):
            logger.warning(
                "normalize_to_rows_entrada_invalida", data=str(resp_json_pages)
            )
            return normalized_rows

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
                                f"Pág{page_index},L{i}(base):C({len(current_c_values)})&S({len(s_schema)})len mismatch.Skip."
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
                            # O índice 'col_idx' aqui é o mesmo para s_schema, current_c_values e global_descriptor_selects
                            if col_idx >= len(global_descriptor_selects):
                                logger.warning(
                                    f"Pág {page_index}, L{i}, Col {col_idx}: Índice fora dos limites para global_descriptor_selects. Pulando campo."
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
                                # logger.debug(f"Pág {page_index}, Linha {i}, Col {col_idx}: API name '{base_api_name}' não mapeado para CSV.")
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
                                f"Pág {page_index}, Linha {i} (delta): Schema 'S' da primeira linha não disponível. Pulando página."
                            )
                            break
                        if not last_processed_pydantic_row:
                            logger.error(
                                f"Pág {page_index}, Linha {i} (delta): Linha base anterior não processada. Pulando."
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

                            logger.debug(
                                f"Pág {page_index}, L{i} Delta: R={rulifier_r} ({bin(rulifier_r)}), C_delta={current_c_values_delta}"
                            )

                            for col_idx, schema_item in enumerate(s_schema):
                                if col_idx >= len(
                                    global_descriptor_selects
                                ):  # Segurança
                                    logger.warning(
                                        f"Pág {page_index}, L{i} Delta, Col {col_idx}: Índice fora dos limites para global_descriptors. Pulando campo."
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
                                    # logger.debug(f"Pág {page_index}, L{i} Delta, Col {col_idx}: API name '{base_api_name}' não mapeado.")
                                    continue

                                target_csv_field = csv_field_cfg["csv_field"]
                                target_field_type = csv_field_cfg["type"]

                                # Checa o bit correspondente no Rulifier
                                # (1 << col_idx) cria uma máscara para o bit na posição col_idx
                                # Se o bit está SET (1), herda. Se CLEAR (0), usa valor de C.
                                if (rulifier_r >> col_idx) & 1:  # Bit é 1, Herda
                                    pydantic_input_row[target_csv_field] = (
                                        last_processed_pydantic_row.get(
                                            target_csv_field,
                                            self._format_value(
                                                csv_field_cfg.get("default"),
                                                target_field_type,
                                            ),
                                        )
                                    )
                                else:  # Bit é 0, Novo valor de C_delta
                                    if c_delta_idx >= len(current_c_values_delta):
                                        logger.error(
                                            f"Pág{page_index},L{i}Del({target_csv_field}):R bit0,"
                                            f"C_delta({current_c_values_delta})OOB(idx{c_delta_idx}).Herdou."
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
                                        continue

                                    raw_value_for_field = current_c_values_delta[
                                        c_delta_idx
                                    ]
                                    c_delta_idx += 1

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
                                            else:  # Índice inválido para ValueDict
                                                len_val = (
                                                    len(vd_list)
                                                    if vd_list is not None
                                                    else "N/A"
                                                )
                                                logger.warning(
                                                    f"Pág{page_index},L{i}Del({target_csv_field}):"
                                                    f"VD'{dict_name}',C_del idx'{raw_value_for_field}'OOB(len:{len_val}).Herdou."
                                                )
                                                pydantic_input_row[target_csv_field] = (
                                                    last_processed_pydantic_row.get(
                                                        target_csv_field,
                                                        self._format_value(
                                                            csv_field_cfg.get(
                                                                "default"
                                                            ),
                                                            target_field_type,
                                                        ),
                                                    )
                                                )
                                        except (ValueError, TypeError):
                                            logger.warning(
                                                f"Pág{page_index},L{i}Del({target_csv_field}):"
                                                f"VD'{dict_name}',C_del val'{raw_value_for_field}'not int.Herdou."
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
                                    else:  # Sem DN, valor literal de C_delta
                                        val_to_assign = raw_value_for_field
                                        resolved_value = True

                                    if resolved_value:
                                        decoded = (
                                            self._decode_utf8(str(val_to_assign))
                                            if val_to_assign is not None
                                            else None
                                        )
                                        pydantic_input_row[target_csv_field] = (
                                            self._format_value(
                                                decoded, target_field_type
                                            )
                                        )
                                    # Se resolved_value for False (devido a erro de VD), o campo já foi definido para herdar

                        last_processed_pydantic_row = pydantic_input_row.copy()

                    # LOGGING ADICIONADO PARA DEBUG DE LINHAS DELTA - Removido, pois agora processamos com Rulifier

                    try:
                        precatorio_obj = Precatorio(**pydantic_input_row)
                        dumped_row = precatorio_obj.dict()

                        current_order_in_normalized_list += 1
                        dumped_row["ordem"] = (
                            current_order_in_normalized_list  # Atribui ordem corrigida
                        )

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
        return normalized_rows

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

    @track_time(
        entity=lambda self_or_args, *args, **kwargs: (
            args[0] if args else config.default_entity
        )
    )
    def crawl(self, entity_slug: str, out_file: str):
        """Executa o processo completo de crawling para uma entidade."""
        try:
            logger.info("crawl_start", entity=entity_slug, output_file=out_file)

            # Busca os dados
            # Para o crawl completo, não especificamos 'count', então ele tentará buscar tudo com paginação.
            logger.info("fetching_data", entity=entity_slug)
            raw_data = self.fetch_data(
                entity_slug
            )  # Não passa count aqui para buscar tudo

            if not raw_data:
                logger.warning("no_raw_data", entity=entity_slug)
                return

            # Normaliza os dados
            logger.info("normalizing_data", entity=entity_slug)
            rows = self.normalize_to_rows(raw_data)

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
