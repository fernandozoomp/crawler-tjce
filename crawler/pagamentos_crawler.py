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
)
from logger import get_logger
from models.models import Pagamento
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


class PagamentosCrawler:
    def __init__(self):
        self.config_instance = config
        self.api_url = self.config_instance.api_url
        self.resource_key = "875de184-379e-4d19-b1e1-b3ae0c80112d"  # Resource key específico para pagamentos
        self.headers = self._get_pagamentos_headers()
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.page_size = getattr(config, "pagamentos_page_size", 500)
        self.max_pages = getattr(config, "max_pagamentos_pages", 100)

    def _get_pagamentos_headers(self) -> Dict[str, str]:
        """Retorna headers específicos para a API de pagamentos."""
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "ActivityId": str(uuid.uuid4()),
            "Connection": "keep-alive",
            "Content-Type": "application/json;charset=UTF-8",
            "Origin": "https://app.powerbi.com",
            "Referer": "https://app.powerbi.com/",
            "RequestId": str(uuid.uuid4()),
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "cross-site",
            "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36",
            "X-PowerBI-ResourceKey": self.resource_key,
            "sec-ch-ua": '"Google Chrome";v="141", "Not_A_Brand";v="8", "Chromium";v="141"',
            "sec-ch-ua-mobile": "?1",
            "sec-ch-ua-platform": '"Android"',
        }

    def _build_pagamentos_payload(
        self, restart_tokens: Optional[List[Any]] = None, count: int = 500
    ) -> Dict[str, Any]:
        """Constrói o payload para a requisição de pagamentos."""
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
                                                "Name": "c",
                                                "Entity": "CONSOLIDADO - EST CE",
                                                "Type": 0,
                                            }
                                        ],
                                        "Select": [
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "QUANT",
                                                },
                                                "Name": "Sum(CONSOLIDADO - EST CE.QUANT)",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "MODALIDADE",
                                                },
                                                "Name": "CONSOLIDADO - EST CE.MODALIDADE",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "NATUREZA",
                                                },
                                                "Name": "CONSOLIDADO - EST CE.NATUREZA",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "EXERCÍCIO",
                                                },
                                                "Name": "CONSOLIDADO - EST CE.EXERCÍCIO",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "DATA DE PROTOCOLO",
                                                },
                                                "Name": "CONSOLIDADO - EST CE.DATA DE PROTOCOLO",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "PRECATÓRIO",
                                                },
                                                "Name": "CONSOLIDADO - EST CE.PRECATÓRIO",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "CREDOR/BENEFICIÁRIO",
                                                },
                                                "Name": "CONSOLIDADO - EST CE.CREDOR/BENEFICIÁRIO",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "TIPO",
                                                },
                                                "Name": "CONSOLIDADO - EST CE.TIPO",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "DATA DE PAG",
                                                },
                                                "Name": "CONSOLIDADO - EST CE.DATA DE PAG",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "CPF/CNPJ",
                                                },
                                                "Name": "CONSOLIDADO - EST CE.CPF/CNPJ",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "V. BRUTO",
                                                },
                                                "Name": "Sum(CONSOLIDADO - EST CE.V. BRUTO)",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "PREVIDÊNCIA",
                                                },
                                                "Name": "CONSOLIDADO - EST CE.PREVIDÊNCIA",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "IRRF",
                                                },
                                                "Name": "CONSOLIDADO - EST CE.IRRF",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "HONORÁRIOS",
                                                },
                                                "Name": "CONSOLIDADO - EST CE.HONORÁRIOS",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "V. BRUTO CONTRATUAL",
                                                },
                                                "Name": "CONSOLIDADO - EST CE.V. BRUTO CONTRATUAL",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "RRA",
                                                },
                                                "Name": "CONSOLIDADO - EST CE.RRA",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "V. LÍQUIDO",
                                                },
                                                "Name": "Sum(CONSOLIDADO - EST CE.V. LÍQUIDO)",
                                            },
                                            {
                                                "Measure": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "_titulo",
                                                },
                                                "Name": "CONSOLIDADO - EST CE._titulo",
                                            },
                                        ],
                                    },
                                    "Binding": {
                                        "Primary": {
                                            "Groupings": [
                                                {
                                                    "Projections": [
                                                        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
                                                        10, 11, 12, 13, 14, 15, 16
                                                    ],
                                                    "Subtotal": 1,
                                                }
                                            ]
                                        },
                                        "Projections": [17],
                                        "DataReduction": {
                                            "DataVolume": 3,
                                            "Primary": {
                                                "Window": {"Count": count}
                                            },
                                        },
                                        "Version": 1,
                                    },
                                    "ExecutionMetricsKind": 1,
                                }
                            }
                        ],
                        "QueryId": "",
                        "ApplicationContext": {
                            "DatasetId": "d8e95606-6cf4-4ee2-9945-6ee3424f7941",
                            "Sources": [
                                {
                                    "ReportId": "15a9b547-caa8-4c5f-9a38-8b8ae46bd3c6",
                                    "VisualId": "0d307992d907b8e07325",
                                }
                            ],
                        },
                    }
                }
            ],
            "cancelQueries": [],
            "modelId": 5283863,
        }

        # Adiciona RestartTokens se fornecidos
        if restart_tokens:
            payload["queries"][0]["Query"]["Commands"][0]["SemanticQueryDataShapeCommand"]["Binding"]["DataReduction"]["Primary"]["Window"]["RestartTokens"] = restart_tokens

        return payload

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    @track_time
    def _fetch_page(
        self,
        restart_tokens: Optional[List[Any]] = None,
        count: int = 500,
    ) -> Dict:
        """Busca uma página de dados da API de pagamentos."""
        current_headers = self.session.headers.copy()
        current_headers.update(
            {"ActivityId": str(uuid.uuid4()), "RequestId": str(uuid.uuid4())}
        )

        payload = self._build_pagamentos_payload(restart_tokens=restart_tokens, count=count)

        REQUESTS_TOTAL.labels(entity="pagamentos").inc()
        response = self.session.post(
            self.api_url, json=payload, headers=current_headers, timeout=180
        )
        response.raise_for_status()
        return response.json()

    def fetch_pagamentos(self) -> List[Dict[str, Any]]:
        """Busca a lista de todos os pagamentos disponíveis, lidando com paginação."""
        all_pagamentos = []
        current_restart_tokens: Optional[List[Any]] = None
        page_num = 0
        global_record_count = 0  # Contagem global de registros

        logger.info("Iniciando busca de pagamentos")

        while True:
            page_num += 1
            logger.info(
                f"Buscando página {page_num} de pagamentos. Tokens: {current_restart_tokens is not None}"
            )

            try:
                page_data_response = self._fetch_page(
                    restart_tokens=current_restart_tokens,
                    count=self.page_size,
                )

                if (
                    not page_data_response
                    or "results" not in page_data_response
                    or not page_data_response["results"]
                ):
                    logger.warning(f"Página {page_num}: Resposta vazia ou inválida")
                    break

                # Normaliza os dados da página com contagem global
                normalized_page_rows = self.normalize_pagamento_data(
                    [page_data_response],
                    starting_order_number=global_record_count + 1
                )

                if not normalized_page_rows:
                    logger.info(f"Página {page_num}: Sem dados normalizados")
                    break

                # Atualiza a contagem global
                global_record_count += len(normalized_page_rows)
                all_pagamentos.extend(normalized_page_rows)

                # Verifica se há mais páginas
                try:
                    new_restart_tokens = page_data_response["results"][0]["result"][
                        "data"
                    ]["dsr"]["DS"][0].get("RT")
                    if new_restart_tokens:
                        if new_restart_tokens == current_restart_tokens:
                            logger.warning(f"Página {page_num}: Tokens duplicados, interrompendo")
                            break
                        current_restart_tokens = new_restart_tokens
                        logger.debug(f"Página {page_num}: Próximos tokens obtidos")
                    else:
                        logger.info(f"Página {page_num}: Fim da paginação")
                        break
                except (KeyError, IndexError, TypeError) as e:
                    logger.warning(f"Página {page_num}: Erro ao extrair tokens: {e}")
                    break

            except requests.exceptions.RequestException as e:
                logger.error(f"Página {page_num}: Erro HTTP: {e}")
                break
            except Exception as e:
                logger.error(f"Página {page_num}: Erro inesperado: {e}")
                break

            if page_num >= self.max_pages:
                logger.warning(f"Limite de páginas ({self.max_pages}) atingido")
                break

        logger.info(f"Busca concluída: {len(all_pagamentos)} pagamentos em {page_num} páginas")
        return all_pagamentos

    def normalize_pagamento_data(self, resp_json_pages: List[Dict], starting_order_number: int = 1) -> List[Dict[str, Any]]:
        """Normaliza os dados JSON da API de pagamentos para uma lista de dicionários."""
        normalized_rows: List[Dict] = []

        if not resp_json_pages or not isinstance(resp_json_pages, list):
            return normalized_rows

        for page_index, resp_json in enumerate(resp_json_pages):
            try:
                data = (
                    resp_json.get("results", [{}])[0].get("result", {}).get("data", {})
                )
                if not data:
                    continue

                dsr = data.get("dsr")
                if not dsr:
                    continue

                current_ds_list = dsr.get("DS", [])
                if not current_ds_list:
                    continue

                current_ds = current_ds_list[0]
                value_dicts = current_ds.get("ValueDicts", {})

                ph_list = current_ds.get("PH", [])
                if not ph_list:
                    continue
                ph = ph_list[0]

                data_rows_container = ph.get("DM0")
                if data_rows_container is None:
                    continue

                if (
                    isinstance(data_rows_container, list)
                    and len(data_rows_container) == 1
                    and not data_rows_container[0]
                ):
                    continue

                data_rows = (
                    data_rows_container if isinstance(data_rows_container, list) else []
                )

                if not data_rows:
                    logger.info(f"Página {page_index}: Sem linhas de dados em DM0.")
                    continue

                # Processar primeira linha para obter schema
                first_row = data_rows[0]
                if not isinstance(first_row, dict) or "S" not in first_row:
                    logger.error(f"Página {page_index}: Primeira linha não tem estrutura esperada (S)")
                    continue

                schema_list = first_row.get("S", [])

                # Verificar se a primeira linha tem schema
                if not schema_list:
                    logger.error(f"Página {page_index}: Schema vazio")
                    continue

                # Mapeamento baseado na estrutura do response_pagamentos.json
                field_mapping = []
                for i, schema_item in enumerate(schema_list):
                    if i == 0:  # Quantidade (índice 0)
                        field_mapping.append({"name": "quantidade", "type": "int", "dict": None, "index": i})
                    elif i == 1:  # Modalidade (índice 1) - D0
                        field_mapping.append({"name": "modalidade", "type": "str", "dict": "D0", "index": i})
                    elif i == 2:  # Natureza (índice 2) - D1
                        field_mapping.append({"name": "natureza", "type": "str", "dict": "D1", "index": i})
                    elif i == 3:  # Exercício (índice 3) - D2
                        field_mapping.append({"name": "exercicio", "type": "int", "dict": "D2", "index": i})
                    elif i == 4:  # Data Protocolo (índice 4) - D3
                        field_mapping.append({"name": "data_protocolo", "type": "str", "dict": "D3", "index": i})
                    elif i == 5:  # Precatório (índice 5) - D4
                        field_mapping.append({"name": "precatorio", "type": "str", "dict": "D4", "index": i})
                    elif i == 6:  # Credor/Beneficiário (índice 6) - D5
                        field_mapping.append({"name": "credor_beneficiario", "type": "str", "dict": "D5", "index": i})
                    elif i == 7:  # Tipo (índice 7) - D6
                        field_mapping.append({"name": "tipo", "type": "str", "dict": "D6", "index": i})
                    elif i == 8:  # Data Pagamento (índice 8) - D7
                        field_mapping.append({"name": "data_pagamento", "type": "str", "dict": "D7", "index": i})
                    elif i == 9:  # CPF/CNPJ (índice 9) - D8
                        field_mapping.append({"name": "cpf_cnpj", "type": "str", "dict": "D8", "index": i})
                    elif i == 10:  # Valor Bruto (índice 10)
                        field_mapping.append({"name": "valor_bruto", "type": "Decimal", "dict": None, "index": i})
                    elif i == 11:  # Previdência (índice 11)
                        field_mapping.append({"name": "previdencia", "type": "Decimal", "dict": None, "index": i})
                    elif i == 12:  # IRRF (índice 12)
                        field_mapping.append({"name": "irrf", "type": "Decimal", "dict": None, "index": i})
                    elif i == 13:  # Honorários (índice 13)
                        field_mapping.append({"name": "honorarios", "type": "Decimal", "dict": None, "index": i})
                    elif i == 14:  # Valor Bruto Contratual (índice 14)
                        field_mapping.append({"name": "valor_bruto_contratual", "type": "Decimal", "dict": None, "index": i})
                    elif i == 15:  # RRA (índice 15)
                        field_mapping.append({"name": "rra", "type": "Decimal", "dict": None, "index": i})
                    elif i == 16:  # Valor Líquido (índice 16)
                        field_mapping.append({"name": "valor_liquido", "type": "Decimal", "dict": None, "index": i})

                # Nota: campo ordem será adicionado separadamente após processamento

                logger.info(f"Página {page_index}: Processando {len(data_rows)} linhas com {len(field_mapping)} campos")

                # Processar apenas as linhas delta (começando da linha 1)
                # A linha 0 é apenas schema e não deve ser processada como dados
                for i, raw_row_data_container in enumerate(data_rows[1:], 1):  # Começar do índice 1
                    row_dict = {}

                    # Inicializa com valores padrão
                    for field_info in field_mapping:
                        if field_info["type"] in ["int", "float", "Decimal"]:
                            row_dict[field_info["name"]] = 0
                        else:
                            row_dict[field_info["name"]] = "-"

                    current_c_values = raw_row_data_container.get("C", [])
                    rulifier_r = raw_row_data_container.get("R", 0)

                    # Cada linha delta herda todos os valores da linha anterior
                    # e só sobrescreve os campos indicados pelo rulifier
                    if normalized_rows:
                        # Se já temos linhas processadas, herda da última
                        previous_row_data = normalized_rows[-1]
                        row_dict = previous_row_data.copy()
                    # Se é a primeira linha delta, usa os valores padrão inicializados acima

                    # Para cada campo, verifica se o bit correspondente está setado no rulifier
                    c_idx = 0  # Índice no array C (só conta campos que têm bits ZERADOS = novo valor)
                    for field_info in field_mapping:
                        col_idx = field_info["index"]
                        field_name = field_info["name"]

                        # Verifica se o bit está ZERADO (0 = novo valor, 1 = herdar da linha anterior)
                        if not (rulifier_r & (1 << col_idx)):
                            # Tem novo valor neste campo - pega do array C
                            if c_idx < len(current_c_values):
                                raw_value = current_c_values[c_idx]
                                dict_name = field_info["dict"]

                                if dict_name and dict_name in value_dicts:
                                    try:
                                        dict_idx = int(raw_value)
                                        if 0 <= dict_idx < len(value_dicts[dict_name]):
                                            dict_value = value_dicts[dict_name][dict_idx]
                                            row_dict[field_name] = self._format_pagamento_value(
                                                dict_value, field_info["type"]
                                            )
                                        else:
                                            row_dict[field_name] = self._format_pagamento_value(
                                                "-", field_info["type"]
                                            )
                                    except (ValueError, TypeError) as e:
                                        row_dict[field_name] = self._format_pagamento_value(
                                            "-", field_info["type"]
                                        )
                                else:
                                    row_dict[field_name] = self._format_pagamento_value(
                                        raw_value, field_info["type"]
                                    )
                            else:
                                logger.warning(f"Linha {i}, Campo {field_name}: Índice C ({c_idx}) fora do limite (len={len(current_c_values)})")
                                row_dict[field_name] = "-"
                            c_idx += 1  # Próximo índice no array C
                        # Se o bit está setado (1), mantém o valor herdado da linha anterior (já está em row_dict)

                    # Valida e adiciona à lista
                    try:
                        # Trata valores especiais
                        if row_dict.get("data_protocolo") == "-":
                            row_dict["data_protocolo"] = ""
                        if row_dict.get("data_pagamento") == "-":
                            row_dict["data_pagamento"] = ""
                        if row_dict.get("cpf_cnpj") == "-":
                            row_dict["cpf_cnpj"] = ""

                        # Converte valores zero para Decimal apropriado
                        for decimal_field in ["valor_bruto", "previdencia", "irrf", "honorarios", "valor_bruto_contratual", "rra", "valor_liquido"]:
                            if row_dict.get(decimal_field) == 0:
                                row_dict[decimal_field] = Decimal("0.0")

                        logger.debug(f"Linha {i} processada: {row_dict}")

                        # Adicionar campo ordem (não vem da API, é calculado)
                        row_dict["ordem"] = starting_order_number + len(normalized_rows)  # Usa índice sequencial global

                        normalized_rows.append(row_dict)

                    except ValidationError as e:
                        logger.error(f"Erro de validação na linha {i}: {e}, dados: {row_dict}")
                    except Exception as e:
                        logger.error(f"Erro inesperado na linha {i}: {e}, dados: {row_dict}")

            except Exception as e:
                logger.error(f"Erro ao processar página {page_index}: {e}", exc_info=True)

        # Ordena as linhas por ordem crescente
        normalized_rows.sort(key=lambda x: x.get("ordem", 0))

        logger.info(f"Dados ordenados por ordem crescente: {len(normalized_rows)} linhas")
        return normalized_rows

    def _format_pagamento_value(self, value: Any, field_type: str) -> Any:
        """Formata valor de acordo com o tipo do campo."""
        if value is None:
            if field_type in ["int", "float", "Decimal"]:
                return 0
            return "-"

        if isinstance(value, str) and not value.strip():
            if field_type in ["int", "float", "Decimal"]:
                return 0
            return "-"

        try:
            if field_type == "int":
                # Trata casos especiais como strings vazias ou valores inválidos
                if isinstance(value, str):
                    value = value.strip()
                    if not value or value == "-":
                        return 0
                return int(float(value))
            elif field_type == "float":
                if isinstance(value, str):
                    value = value.strip()
                    if not value or value == "-":
                        return 0.0
                return float(value)
            elif field_type == "Decimal":
                if isinstance(value, str):
                    value = value.strip()
                    if not value or value == "-":
                        return Decimal("0.0")
                return Decimal(str(value))
            elif field_type == "str":
                # Para campos de texto, apenas retorna a string limpa
                if isinstance(value, str):
                    return value.strip()
                return str(value).strip()
            else:
                # Para outros tipos, converte para string
                return str(value).strip()
        except (ValueError, TypeError, InvalidOperation):
            if field_type in ["int", "float", "Decimal"]:
                return 0
            return "-"

    def save_pagamentos(self, pagamentos: List[Dict[str, Any]], out_file: str) -> None:
        """Salva a lista de pagamentos em um arquivo CSV."""
        try:
            os.makedirs(os.path.dirname(out_file), exist_ok=True)

            with open(out_file, "w", newline="", encoding="utf-8-sig") as f:
                if pagamentos:
                    fieldnames = [
                        "quantidade", "modalidade", "natureza", "exercicio", "data_protocolo",
                        "precatorio", "credor_beneficiario", "tipo", "data_pagamento",
                        "cpf_cnpj", "valor_bruto", "previdencia", "irrf", "honorarios",
                        "valor_bruto_contratual", "rra", "valor_liquido"
                    ]
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()

                    for pagamento in pagamentos:
                        # Formatar dados para CSV
                        csv_row = {}
                        for field in fieldnames:
                            value = pagamento.get(field, "-")
                            if field in ["data_protocolo", "data_pagamento"] and isinstance(value, str):
                                # Já está formatado como dd/mm/yyyy
                                csv_row[field] = value
                            elif field in ["valor_bruto", "previdencia", "irrf", "honorarios", "valor_bruto_contratual", "rra", "valor_liquido"] and isinstance(value, Decimal):
                                csv_row[field] = format_currency(float(value))
                            else:
                                csv_row[field] = str(value) if value != "-" else ""

                        writer.writerow(csv_row)
                else:
                    # Cabeçalhos mesmo se vazio
                    writer = csv.writer(f)
                    writer.writerow([
                        "quantidade", "modalidade", "natureza", "exercicio", "data_protocolo",
                        "precatorio", "credor_beneficiario", "tipo", "data_pagamento",
                        "cpf_cnpj", "valor_bruto", "previdencia", "irrf", "honorarios",
                        "valor_bruto_contratual", "rra", "valor_liquido"
                    ])

            logger.info(f"Pagamentos salvos em {out_file}", count=len(pagamentos))

        except IOError as e:
            logger.error(f"Erro de I/O ao salvar pagamentos: {e}")
        except Exception as e:
            logger.error(f"Erro inesperado ao salvar pagamentos: {e}")

    def get_and_save_pagamentos(self, out_file: str) -> List[Dict[str, Any]]:
        """Orquestra busca e salvamento de pagamentos."""
        logger.info(f"Iniciando coleta de pagamentos para: {out_file}")

        pagamentos = self.fetch_pagamentos()
        if not pagamentos:
            logger.warning("Nenhum pagamento encontrado")
            self.save_pagamentos([], out_file)
            return []

        self.save_pagamentos(pagamentos, out_file)
        logger.info(f"{len(pagamentos)} pagamentos processados")

        return pagamentos
