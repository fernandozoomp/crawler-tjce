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

import requests
from pydantic import ValidationError

from config import config, field_config as field_cfg, PAYLOAD_STRUCTURE
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
        logger.info(
            "Resposta JSON completa da API:",
            content_length=len(response.content),
            json_data=response_json,
        )

        return response_json

    def fetch_data(self, entity: str, count: Optional[int] = None) -> List[Dict]:
        """
        Busca e normaliza os dados de precatórios para uma entidade específica.
        Opcionalmente limita o número de registros com o parâmetro 'count'.
        """
        logger.info(
            f"Iniciando busca de dados para a entidade: {entity}, count: {count}"
        )
        all_pages_data = []
        # Se 'count' for especificado, a paginação com restart_tokens pode precisar de ajustes
        # ou ser desabilitada/simplificada, pois queremos um número fixo de registros.
        # A implementação atual de _fetch_page já recebe 'count'.
        # A lógica de loop com restart_tokens aqui em fetch_data pode precisar ser condicional.

        # Se count for especificado, faremos apenas uma chamada.
        # A paginação real para buscar 'count' registros se eles excederem o limite da página da API
        # não está totalmente implementada aqui. _fetch_page buscará até 'count' na primeira consulta.
        if count is not None:
            page_data = self._fetch_page(
                entity=entity, restart_tokens=None, count=count
            )
            if page_data:
                all_pages_data.append(page_data)
        else:
            # Lógica de paginação original quando 'count' não é especificado
            restart_tokens = None
            max_pages = 100  # Limite de segurança para evitar loops infinitos
            pages_fetched = 0
            while pages_fetched < max_pages:
                page_data = self._fetch_page(
                    entity=entity, restart_tokens=restart_tokens
                )
                if not page_data:
                    logger.info(
                        f"Não há mais dados para buscar para a entidade {entity} ou erro na busca."
                    )
                    break

                all_pages_data.append(page_data)
                pages_fetched += 1

                # Extrair restart_tokens para a próxima página
                # A estrutura exata para encontrar RestartTokens pode variar.
                # Baseado na estrutura anterior do payload:
                try:
                    # Tentativa de encontrar RestartTokens na estrutura comum
                    # Isso precisa ser robusto e verificado contra a resposta real da API
                    data_window = (
                        page_data.get("results", [{}])[0]
                        .get("result", {})
                        .get("data", {})
                        .get("dsr", {})
                        .get("DS", [{}])[0]
                        .get("ValueDicts", {})
                        .get("D0", [{}])[0]
                        .get("RestartTokens")
                    )
                    if data_window:  # Se D0 existir e tiver RestartTokens
                        restart_tokens = (
                            data_window[0]
                            if isinstance(data_window, list) and data_window
                            else None
                        )
                    else:  # Tentar outra estrutura comum se a primeira falhar
                        data_window = (
                            page_data.get("results", [{}])[0]
                            .get("result", {})
                            .get("data", {})
                            .get("dsr", {})
                            .get("DataWindows", [{}])[0]
                            .get("RestartTokens")
                        )
                        if data_window:
                            restart_tokens = (
                                data_window[0]
                                if isinstance(data_window, list) and data_window
                                else None
                            )
                        else:
                            restart_tokens = None

                    if restart_tokens:
                        logger.info(
                            f"Próximos RestartTokens encontrados para {entity}: {restart_tokens}"
                        )
                    else:
                        logger.info(
                            f"Não foram encontrados RestartTokens para {entity}, fim da paginação."
                        )
                        break
                except (IndexError, KeyError, TypeError) as e:
                    logger.warning(
                        f"Não foi possível extrair RestartTokens para {entity}: {e}. Interrompendo paginação."
                    )
                    restart_tokens = None  # Garantir que saia do loop
                    break

                if not restart_tokens:
                    logger.info(
                        f"Fim da paginação para entidade {entity} (sem RestartTokens)."
                    )
                    break

            if pages_fetched >= max_pages:
                logger.warning(
                    f"Atingido o número máximo de páginas ({max_pages}) para {entity}."
                )

        if not all_pages_data or not all_pages_data[0]:
            logger.warning(
                f"Nenhuma página de dados foi baixada para a entidade: {entity}"
            )
            return []

        logger.info(
            "fetch_complete",
            total_pages=len(all_pages_data),
            total_records=sum(
                len(
                    page["results"][0]["result"]["data"]["dsr"]["DS"][0]["PH"][0]["DM0"]
                )
                for page in all_pages_data
            ),
        )

        return all_pages_data

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
                    logger.warning(
                        "normalize_to_rows_sem_dados_em_pagina",
                        page_index=page_index,
                        response_keys=list(resp_json.keys()),
                    )
                    if (
                        "results" in resp_json
                        and resp_json["results"]
                        and "result" in resp_json["results"][0]
                        and "error" in resp_json["results"][0]["result"]
                    ):
                        logger.error(
                            "normalize_to_rows_erro_api_powerbi",
                            page_index=page_index,
                            error_details=resp_json["results"][0]["result"]["error"],
                        )
                    continue

                dsr = data.get("dsr")
                if not dsr:
                    logger.warning(
                        "normalize_to_rows_sem_dsr",
                        page_index=page_index,
                        data_keys=list(data.keys()),
                    )
                    continue

                ph = dsr.get("DS", [{}])[0].get("PH", [{}])[0]
                if not ph:
                    logger.warning(
                        "normalize_to_rows_sem_ph",
                        page_index=page_index,
                        ds_keys=list(
                            dsr.get("DS", [{}])[0].keys() if dsr.get("DS") else []
                        ),
                    )
                    continue

                data_rows_container = ph.get("DM0")
                if (
                    data_rows_container is None
                ):  # DM0 pode ser lista vazia [{}] se não houver dados, mas não None
                    logger.warning(
                        "normalize_to_rows_sem_dm0_container",
                        page_index=page_index,
                        ph_keys=list(ph.keys()),
                    )
                    continue

                if (
                    isinstance(data_rows_container, list)
                    and len(data_rows_container) == 1
                    and not data_rows_container[0]
                ):
                    logger.info(
                        "normalize_to_rows_dm0_vazio",
                        page_index=page_index,
                        message="DM0 é uma lista com um dicionário vazio, indicando nenhum dado nesta página.",
                    )
                    continue

                current_ds = dsr.get("DS", [{}])[0]
                value_dicts = current_ds.get("ValueDicts", {})

                # Mapa global: índice global (0-N) para nome base da API
                # Este mapa é construído a partir dos descritores globais da query.
                global_descriptor_selects = data.get("descriptor", {}).get("Select", [])
                if not global_descriptor_selects:
                    logger.warning(
                        "normalize_to_rows_sem_descritores_globais",
                        page_index=page_index,
                        descriptor=data.get("descriptor", {}),
                    )
                    continue  # Não podemos prosseguir sem os descritores globais

                # Mapa: nome de API base -> campo CSV e tipo
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
                    logger.info(
                        "normalize_to_rows_sem_dados_em_dm0_lista",
                        page_index=page_index,
                        dm0_content=str(data_rows_container)[:200],
                    )
                    continue

                total_raw_records_count += len(data_rows)
                logger.info(
                    "normalize_to_rows_processando_pagina",
                    page_index=page_index,
                    num_raw_rows=len(data_rows),
                )

                last_seen_row_schema = None

                for i, raw_row_data_container in enumerate(data_rows):
                    current_c_values = raw_row_data_container.get("C", [])
                    current_s_list_from_row = raw_row_data_container.get("S")
                    rulifier_r = raw_row_data_container.get("R")  # Obter o Rulifier

                    effective_s_list = None
                    if (
                        current_s_list_from_row
                        and isinstance(current_s_list_from_row, list)
                        and len(current_s_list_from_row) > 0
                    ):
                        last_seen_row_schema = current_s_list_from_row
                        effective_s_list = current_s_list_from_row
                        if i == 0 and page_index == 0:
                            logger.info(
                                "normalize_to_rows_primeira_linha_bruta_da_pagina_com_schema_S",
                                raw_values=current_c_values,
                                row_schema=effective_s_list,
                                rulifier=rulifier_r,
                            )
                    elif last_seen_row_schema:
                        effective_s_list = last_seen_row_schema
                    else:
                        logger.warning(
                            f"Linha {i} (pág {page_index}): Sem schema S e nenhum anterior. Pulando."
                        )
                        continue

                    if (
                        not current_c_values
                        and rulifier_r is not None
                        and rulifier_r != 0
                    ):  # C pode ser vazio se R indicar que nada mudou ou tudo é default.
                        logger.info(
                            f"Linha {i} (pág {page_index}): C está vazio, mas R={rulifier_r}. Assumindo todos defaults."
                        )
                        # pydantic_input_row já está com defaults, então podemos apenas tentar
                        # validar/adicionar se necessário
                        # No entanto, se R != 0, C não deveria estar vazio. Isso pode ser um caso estranho.
                        # Por segurança, vamos pular se C está vazio e R sugere que deveria haver dados.
                        # Se R for 0 (nenhum bit setado), C vazio é esperado.
                        if rulifier_r == 0:
                            # Se R é 0, todos os campos são default/não mudaram
                            # (assumindo defaults preenchidos)
                            # Tentamos criar um Precatorio a partir dos defaults puros.
                            pydantic_input_row_defaults_only: Dict[str, Any] = {}
                            for (
                                csv_f_default,
                                csv_attrs_default,
                            ) in self.field_config_instance.field_mapping.items():
                                pydantic_input_row_defaults_only[csv_f_default] = (
                                    self._format_value(
                                        csv_attrs_default.get("default"),
                                        csv_attrs_default.get("type", "str"),
                                    )
                                )
                            try:
                                precatorio_obj = Precatorio(
                                    **pydantic_input_row_defaults_only
                                )
                                normalized_rows.append(precatorio_obj.model_dump())
                                # Inc RECORDS_PROCESSED etc.
                            except ValidationError as e_def:
                                logger.error(
                                    "erro_validacao_pydantic_defaults_only_com_R_zero",
                                    row_index_in_page=i,
                                    pydantic_input=pydantic_input_row_defaults_only,
                                    errors=e_def.errors(),
                                )
                            continue  # Próxima linha
                        else:
                            logger.warning(
                                f"Linha {i} (pág {page_index}): C vazio, mas R={rulifier_r} != 0. "
                                f"Pulando (inconsistente)."
                            )
                            continue
                    elif not current_c_values:  # C vazio e R é None ou 0
                        logger.info(
                            f"Linha {i} (pág {page_index}): C vazio (R={rulifier_r}). Usando defaults."
                        )
                        pydantic_input_row_defaults_only_no_c: Dict[str, Any] = {}
                        for (
                            csv_f_no_c,
                            csv_attrs_no_c,
                        ) in self.field_config_instance.field_mapping.items():
                            pydantic_input_row_defaults_only_no_c[csv_f_no_c] = (
                                self._format_value(
                                    csv_attrs_no_c.get("default"),
                                    csv_attrs_no_c.get("type", "str"),
                                )
                            )
                        try:
                            precatorio_obj = Precatorio(
                                **pydantic_input_row_defaults_only_no_c
                            )
                            normalized_rows.append(precatorio_obj.model_dump())
                        except ValidationError as e_no_c:
                            logger.error(
                                "erro_validacao_pydantic_defaults_only_sem_C",
                                row_index_in_page=i,
                                pydantic_input=pydantic_input_row_defaults_only_no_c,
                                errors=e_no_c.errors(),
                            )
                        continue

                    pydantic_input_row: Dict[str, Any] = {}
                    for (
                        csv_f,
                        csv_attrs,
                    ) in self.field_config_instance.field_mapping.items():
                        pydantic_input_row[csv_f] = self._format_value(
                            csv_attrs.get("default"), csv_attrs.get("type", "str")
                        )

                    c_iterator = iter(current_c_values)
                    processed_values_from_c = 0

                    # Validar se effective_s_list e global_descriptor_selects têm o mesmo comprimento
                    if len(effective_s_list) != len(global_descriptor_selects):
                        logger.error(
                            f"Linha {i} (pág {page_index}): Comprimento do schema S da linha ({len(effective_s_list)}) "
                            f"não corresponde ao comprimento dos descritores globais ({len(global_descriptor_selects)}). "
                            f"Pulando linha."
                        )
                        continue

                    for col_idx_in_schema, schema_item_from_s in enumerate(
                        effective_s_list
                    ):
                        is_present_in_c = False
                        if rulifier_r is not None:
                            if (rulifier_r >> col_idx_in_schema) & 1:
                                is_present_in_c = True
                        else:
                            if col_idx_in_schema < len(current_c_values):
                                is_present_in_c = True

                        if is_present_in_c:
                            try:
                                raw_value_in_c = next(c_iterator)
                                processed_values_from_c += 1
                            except StopIteration:
                                logger.error(
                                    f"Linha {i} (pág {page_index}): R indicou col {col_idx_in_schema} "
                                    f"em C, mas C ({len(current_c_values)}) esgotou. R={rulifier_r}. "
                                    f"Erro grave na lógica ou dados da API. Pulando linha."
                                )
                                break  # Sai do loop de colunas para esta linha

                            # Obter informações do descritor global correspondente pelo índice
                            global_desc_item_for_this_col = global_descriptor_selects[
                                col_idx_in_schema
                            ]
                            base_api_name = self._get_base_field_name(
                                global_desc_item_for_this_col.get("Name")
                            )

                            csv_field_info = api_name_to_csv_field_map.get(
                                base_api_name
                            )
                            if not csv_field_info:
                                logger.debug(
                                    f"API_NAME '{base_api_name}' (col {col_idx_in_schema} de S/GlobalDesc) "
                                    f"não mapeado para CSV. Pulando valor de C."
                                )
                                continue

                            csv_field = csv_field_info["csv_field"]
                            field_type = csv_field_info["type"]

                            original_value_from_api: Any = None
                            value_resolved_for_formatting = False

                            # Usar DN do schema_item_from_s (da linha atual)
                            dict_name_from_s_row = schema_item_from_s.get("DN")

                            if dict_name_from_s_row:
                                try:
                                    actual_index = int(raw_value_in_c)
                                    value_dict_list = value_dicts.get(
                                        dict_name_from_s_row
                                    )

                                    logger.debug(
                                        "value_dict_lookup",
                                        csv_field=csv_field,
                                        api_name=base_api_name,
                                        dict_name=dict_name_from_s_row,
                                        index_val_from_c=str(raw_value_in_c)[
                                            :50
                                        ],  # Log original de C (curto)
                                        attempted_int_index=actual_index,
                                        is_list=isinstance(value_dict_list, list),
                                        list_len=(
                                            len(value_dict_list)
                                            if isinstance(value_dict_list, list)
                                            else -1
                                        ),
                                    )

                                    if isinstance(
                                        value_dict_list, list
                                    ) and 0 <= actual_index < len(value_dict_list):
                                        original_value_from_api = value_dict_list[
                                            actual_index
                                        ]
                                        value_resolved_for_formatting = True
                                    else:
                                        logger.warning(
                                            f"Índice {actual_index} (de C '{str(raw_value_in_c)[:50]}') para VD '{dict_name_from_s_row}' "
                                            f"(campo {csv_field}, API {base_api_name}) inv_bounds "
                                            f"(len: {len(value_dict_list) if isinstance(value_dict_list, list) else 'N/A'}). "
                                            f"Usando default para '{csv_field}'."
                                        )
                                except (ValueError, TypeError):
                                    logger.error(
                                        f"Valor '{str(raw_value_in_c)[:50]}' de C para campo DN '{csv_field}' (DN: {dict_name_from_s_row}) "
                                        f"não é índice int válido. Usando default para '{csv_field}'."
                                    )
                            else:  # Sem DN no schema S da linha para esta coluna
                                original_value_from_api = raw_value_in_c
                                value_resolved_for_formatting = True

                            if value_resolved_for_formatting:
                                decoded_value = (
                                    self._decode_utf8(str(original_value_from_api))
                                    if original_value_from_api is not None
                                    else None
                                )
                                formatted_for_pydantic = self._format_value(
                                    decoded_value, field_type
                                )
                                pydantic_input_row[csv_field] = formatted_for_pydantic
                            # else: o default pré-preenchido em pydantic_input_row é usado.

                        # Fim do if is_present_in_c:
                    # Fim do loop de colunas

                    if (
                        processed_values_from_c != len(current_c_values)
                        and rulifier_r is not None
                    ):
                        # Este log agora só dispara se R foi fornecido e houve uma contagem diferente.
                        # Se R não for fornecido, processed_values_from_c pode ser < len(current_c_values)
                        # se effective_s_list for mais curto que C, o que é permitido (C pode ter dados extras).
                        # A validação importante é se R indica X itens e consumimos X itens.
                        # A verificação if col_idx_in_schema < len(current_c_values) no modo sem R é um fallback.
                        # No entanto, a contagem de processed_values_from_c se baseia em R.
                        # Se R diz para pegar 5 itens, e C tem 10, processed_values_from_c será 5.
                        # O warning original era: processed_values_from_c != len(current_c_values)
                        # Isso ainda pode ser útil para detectar se C tem mais dados do que R indica.
                        # Mas não é necessariamente um erro se R foi respeitado.
                        # Vamos refinar o log para focar se o número de itens consumidos (baseado em R)
                        # difere do número de bits setados em R.
                        num_bits_set_in_r = (
                            bin(rulifier_r).count("1")
                            if rulifier_r is not None
                            else len(effective_s_list)
                        )
                        if (
                            processed_values_from_c != num_bits_set_in_r
                            and rulifier_r is not None
                        ):
                            logger.warning(
                                f"Linha {i} (pág {page_index}): Consumidos de C ({processed_values_from_c}) "
                                f"!= bits em R ({num_bits_set_in_r}). R={rulifier_r}. "
                                f"Verificar API ou lógica de iteração de C."
                            )
                        elif (
                            rulifier_r is None
                            and processed_values_from_c < len(effective_s_list)
                            and processed_values_from_c < len(current_c_values)
                        ):
                            # Modo sem R, e C tinha mais dados do que S e consumimos menos que C.
                            pass  # Comportamento esperado, C pode ser maior.

                    logger.debug(
                        "pydantic_input_pre_validation",
                        row_index_in_page=i,
                        page_index=page_index,
                        input_data=pydantic_input_row,
                        rulifier_R=rulifier_r,
                        values_C=current_c_values,
                        schema_S_effective=effective_s_list,
                    )

                    try:
                        precatorio_obj = Precatorio(**pydantic_input_row)
                        dumped_row = precatorio_obj.model_dump()
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
