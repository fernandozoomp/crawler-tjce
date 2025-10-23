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
from models.models import Edital
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


class EditalCrawler:
    def __init__(self):
        self.config_instance = config
        self.api_url = self.config_instance.api_url
        self.resource_key = "eacd5a09-9f5f-4e8a-969c-162c1c10d400"  # Resource key específico para editais
        self.headers = self._get_edital_headers()
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.page_size = getattr(config, "edital_page_size", 500)
        self.max_pages = getattr(config, "max_edital_pages", 100)

    def _get_edital_headers(self) -> Dict[str, str]:
        """Retorna headers específicos para a API de editais."""
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

    def _build_edital_payload(
        self, restart_tokens: Optional[List[Any]] = None, count: int = 500
    ) -> Dict[str, Any]:
        """Constrói o payload para a requisição de editais."""
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
                                                "Name": "e",
                                                "Entity": "Estado_Acordo_Edital_01_2024",
                                                "Type": 0,
                                            }
                                        ],
                                        "Select": [
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "e"}
                                                    },
                                                    "Property": "Ordem",
                                                },
                                                "Name": "Sum(Estado_Acordo_Edital_01_2024.Ordem)",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "e"}
                                                    },
                                                    "Property": "Ano Orçamento",
                                                },
                                                "Name": "Sum(Estado_Acordo_Edital_01_2024.Ano Orçamento)",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "e"}
                                                    },
                                                    "Property": "Natureza do Crédito",
                                                },
                                                "Name": "Estado_Acordo_Edital_01_2024.Natureza do Crédito",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "e"}
                                                    },
                                                    "Property": "Data de Cadastro",
                                                },
                                                "Name": "Estado_Acordo_Edital_01_2024.Data de Cadastro",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "e"}
                                                    },
                                                    "Property": "Precatório",
                                                },
                                                "Name": "Estado_Acordo_Edital_01_2024.Precatório",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "e"}
                                                    },
                                                    "Property": "STATUS",
                                                },
                                                "Name": "Estado_Acordo_Edital_01_2024.STATUS",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "e"}
                                                    },
                                                    "Property": "Valor",
                                                },
                                                "Name": "Sum(Estado_Acordo_Edital_01_2024.Valor)",
                                            },
                                        ],
                                        "Where": [
                                            {
                                                "Condition": {
                                                    "In": {
                                                        "Expressions": [
                                                            {
                                                                "Column": {
                                                                    "Expression": {
                                                                        "SourceRef": {"Source": "e"}
                                                                    },
                                                                    "Property": "Natureza do Crédito",
                                                                }
                                                            }
                                                        ],
                                                        "Values": [
                                                            [{"Literal": {"Value": "'ALIMENTAR'"}}],
                                                            [{"Literal": {"Value": "'COMUM'"}}],
                                                        ],
                                                    }
                                                }
                                            }
                                        ],
                                        "OrderBy": [
                                            {
                                                "Direction": 2,
                                                "Expression": {
                                                    "Column": {
                                                        "Expression": {
                                                            "SourceRef": {"Source": "e"}
                                                        },
                                                        "Property": "Ordem",
                                                    }
                                                },
                                            }
                                        ],
                                    },
                                    "Binding": {
                                        "Primary": {
                                            "Groupings": [
                                                {
                                                    "Projections": [0, 1, 2, 3, 4, 5, 6],
                                                    "Subtotal": 1,
                                                }
                                            ]
                                        },
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
                            "DatasetId": "0d15eee8-5bba-4eac-8c01-1cc0a16a219c",
                            "Sources": [
                                {
                                    "ReportId": "3416d4a7-1933-4330-b39e-593a3cc3ded5",
                                    "VisualId": "e33104f1258f2f721448",
                                }
                            ],
                        },
                    }
                }
            ],
            "cancelQueries": [],
            "modelId": 5903288,
        }

        # Adiciona RestartTokens se fornecidos
        if restart_tokens:
            payload["queries"][0]["Query"]["Commands"][0]["SemanticQueryDataShapeCommand"]["Binding"]["DataReduction"]["Primary"]["Window"]["RestartTokens"] = restart_tokens

        return payload

    def _build_timestamp_payload(self) -> Dict[str, Any]:
        """Constrói o payload para obter a data de atualização mais recente."""
        return {
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
                                                "Name": "e",
                                                "Entity": "Estado_Acordo_Edital_01_2024",
                                                "Type": 0,
                                            }
                                        ],
                                        "Select": [
                                            {
                                                "Aggregation": {
                                                    "Expression": {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {"Source": "e"}
                                                            },
                                                            "Property": "Atualização do Painel",
                                                        }
                                                    },
                                                    "Function": 3,  # Min function
                                                },
                                                "Name": "Min(Estado_Acordo_Edital_01_2024.Atualização do Painel)",
                                            }
                                        ],
                                    },
                                    "Binding": {
                                        "Primary": {"Groupings": [{"Projections": [0]}]},
                                        "DataReduction": {
                                            "DataVolume": 3,
                                            "Primary": {"Top": {}},
                                        },
                                        "Version": 1,
                                    },
                                    "ExecutionMetricsKind": 1,
                                }
                            }
                        ],
                        "QueryId": "",
                        "ApplicationContext": {
                            "DatasetId": "0d15eee8-5bba-4eac-8c01-1cc0a16a219c",
                            "Sources": [
                                {
                                    "ReportId": "3416d4a7-1933-4330-b39e-593a3cc3ded5",
                                    "VisualId": "12b898ee620157903dee",
                                }
                            ],
                        },
                    }
                }
            ],
            "cancelQueries": [],
            "modelId": 5903288,
        }

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    @track_time
    def _fetch_page(
        self,
        restart_tokens: Optional[List[Any]] = None,
        count: int = 500,
    ) -> Dict:
        """Busca uma página de dados da API de editais."""
        current_headers = self.session.headers.copy()
        current_headers.update(
            {"ActivityId": str(uuid.uuid4()), "RequestId": str(uuid.uuid4())}
        )

        payload = self._build_edital_payload(restart_tokens=restart_tokens, count=count)

        REQUESTS_TOTAL.labels(entity="edital").inc()
        response = self.session.post(
            self.api_url, json=payload, headers=current_headers, timeout=180
        )
        response.raise_for_status()
        return response.json()

    def fetch_editais(self) -> List[Dict[str, Any]]:
        """Busca a lista de todos os editais disponíveis, lidando com paginação."""
        all_editais = []
        current_restart_tokens: Optional[List[Any]] = None
        page_num = 0

        logger.info("Iniciando busca de editais")

        while True:
            page_num += 1
            logger.info(
                f"Buscando página {page_num} de editais. Tokens: {current_restart_tokens is not None}"
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

                # Normaliza os dados da página
                normalized_page_rows = self.normalize_edital_data([page_data_response])

                if not normalized_page_rows:
                    logger.info(f"Página {page_num}: Sem dados normalizados")
                    break

                all_editais.extend(normalized_page_rows)

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

        logger.info(f"Busca concluída: {len(all_editais)} editais em {page_num} páginas")
        return all_editais

    def normalize_edital_data(self, resp_json_pages: List[Dict]) -> List[Dict[str, Any]]:
        """Normaliza os dados JSON da API de editais para uma lista de dicionários."""
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
                if not isinstance(first_row, dict) or "S" not in first_row or "C" not in first_row:
                    logger.error(f"Página {page_index}: Primeira linha não tem estrutura esperada (S e C)")
                    continue

                schema_list = first_row.get("S", [])
                first_row_c_values = first_row.get("C", [])

                if not schema_list or not first_row_c_values:
                    logger.error(f"Página {page_index}: Schema ou dados da primeira linha vazios")
                    continue

                # Mapeamento baseado na estrutura real do response_edital.json
                field_mapping = []
                for i, schema_item in enumerate(schema_list):
                    if i == 0:  # Ordem (índice 0)
                        field_mapping.append({"name": "ordem", "type": "int", "dict": None, "index": i})
                    elif i == 1:  # Ano Orçamento (índice 1) - D0
                        field_mapping.append({"name": "ano_orcamento", "type": "int", "dict": "D0", "index": i})
                    elif i == 2:  # Natureza (índice 2) - D1
                        field_mapping.append({"name": "natureza", "type": "str", "dict": "D1", "index": i})
                    elif i == 3:  # Data Cadastro (índice 3) - D2
                        field_mapping.append({"name": "data_cadastro", "type": "str", "dict": "D2", "index": i})
                    elif i == 4:  # Precatório (índice 4) - D3
                        field_mapping.append({"name": "precatorio", "type": "str", "dict": "D3", "index": i})
                    elif i == 5:  # Status (índice 5) - D4
                        field_mapping.append({"name": "status", "type": "str", "dict": "D4", "index": i})
                    elif i == 6:  # Valor (índice 6)
                        field_mapping.append({"name": "valor", "type": "Decimal", "dict": None, "index": i})

                logger.info(f"Página {page_index}: Processando {len(data_rows)} linhas com {len(field_mapping)} campos")

                # Processar todas as linhas
                for i, raw_row_data_container in enumerate(data_rows):
                    row_dict = {}

                    # Inicializa com valores padrão
                    for field_info in field_mapping:
                        if field_info["type"] in ["int", "float", "Decimal"]:
                            row_dict[field_info["name"]] = 0
                        else:
                            row_dict[field_info["name"]] = "-"

                    current_c_values = raw_row_data_container.get("C", [])

                    if i == 0:  # Primeira linha (linha base)
                        if len(current_c_values) != len(field_mapping):
                            logger.warning(f"Linha {i}: Tamanho C ({len(current_c_values)}) != campos ({len(field_mapping)})")
                            continue

                        for field_info in field_mapping:
                            col_idx = field_info["index"]
                            if col_idx >= len(current_c_values):
                                logger.warning(f"Linha {i} (base): Índice {col_idx} fora do limite para C")
                                continue

                            raw_value = current_c_values[col_idx]
                            field_name = field_info["name"]
                            field_type = field_info["type"]
                            dict_name = field_info["dict"]

                            if dict_name and dict_name in value_dicts:
                                # Verifica se o raw_value é um número de precatório direto
                                if self._is_precatorio_number(str(raw_value)):
                                    # Se é um número de precatório, usa diretamente
                                    row_dict[field_name] = self._format_edital_value(
                                        raw_value, field_type
                                    )
                                else:
                                    try:
                                        dict_idx = int(raw_value)
                                        if 0 <= dict_idx < len(value_dicts[dict_name]):
                                            dict_value = value_dicts[dict_name][dict_idx]
                                            row_dict[field_name] = self._format_edital_value(
                                                dict_value, field_type
                                            )
                                        else:
                                            row_dict[field_name] = self._format_edital_value(
                                                "-", field_type
                                            )
                                    except (ValueError, TypeError) as e:
                                        row_dict[field_name] = self._format_edital_value(
                                            "-", field_type
                                        )
                            else:
                                row_dict[field_name] = self._format_edital_value(
                                    raw_value, field_type
                                )

                    else:  # Linhas delta (aplicam Rulifier)
                        rulifier_r = raw_row_data_container.get("R", 0)

                        # Cada linha delta herda todos os valores da linha anterior
                        # e só sobrescreve os campos indicados pelo rulifier
                        previous_row_data = normalized_rows[-1] if normalized_rows else {}

                        # Inicializa com os dados da linha anterior
                        row_dict = previous_row_data.copy()

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
                                        # Verifica se o raw_value é um número de precatório direto
                                        if self._is_precatorio_number(str(raw_value)):
                                            # Se é um número de precatório, usa diretamente
                                            row_dict[field_name] = self._format_edital_value(
                                                raw_value, field_info["type"]
                                            )
                                        else:
                                            try:
                                                dict_idx = int(raw_value)
                                                if 0 <= dict_idx < len(value_dicts[dict_name]):
                                                    dict_value = value_dicts[dict_name][dict_idx]
                                                    row_dict[field_name] = self._format_edital_value(
                                                        dict_value, field_info["type"]
                                                    )
                                                else:
                                                    row_dict[field_name] = self._format_edital_value(
                                                        "-", field_info["type"]
                                                    )
                                            except (ValueError, TypeError) as e:
                                                row_dict[field_name] = self._format_edital_value(
                                                    "-", field_info["type"]
                                                )
                                    else:
                                        row_dict[field_name] = self._format_edital_value(
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
                        if row_dict.get("data_cadastro") == "-":
                            row_dict["data_cadastro"] = ""

                        if row_dict.get("valor") == 0:
                            row_dict["valor"] = Decimal("0.0")

                        # Só define ordem se não veio da API (ordem == 0)
                        if row_dict.get("ordem") == 0:
                            row_dict["ordem"] = len(normalized_rows) + 1  # Usa índice sequencial

                        logger.debug(f"Linha {i} processada: {row_dict}")
                        normalized_rows.append(row_dict)

                    except ValidationError as e:
                        logger.error(f"Erro de validação na linha {i}: {e}, dados: {row_dict}")
                    except Exception as e:
                        logger.error(f"Erro inesperado na linha {i}: {e}, dados: {row_dict}")

            except Exception as e:
                logger.error(f"Erro ao processar página {page_index}: {e}", exc_info=True)

        logger.info(f"Normalização concluída: {len(normalized_rows)} linhas processadas")

        # Ordena as linhas por ordem crescente
        normalized_rows.sort(key=lambda x: x.get("ordem", 0))

        logger.info(f"Dados ordenados por ordem crescente: {len(normalized_rows)} linhas")
        return normalized_rows

    def _is_precatorio_number(self, value: Any) -> bool:
        """Verifica se um valor parece um número de precatório."""
        if not isinstance(value, str):
            return False
        # Regex para formato de precatório: 0000000-00.0000.0.00.0000
        import re
        pattern = r'^\d{7}-\d{2}\.\d{4}\.\d{1}\.\d{2}\.\d{4}$'
        return bool(re.match(pattern, value.strip()))

    def _format_edital_value(self, value: Any, field_type: str) -> Any:
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

    def save_editais(self, editais: List[Dict[str, Any]], out_file: str) -> None:
        """Salva a lista de editais em um arquivo CSV."""
        try:
            os.makedirs(os.path.dirname(out_file), exist_ok=True)

            with open(out_file, "w", newline="", encoding="utf-8-sig") as f:
                if editais:
                    fieldnames = [
                        "ordem", "ano_orcamento", "natureza", "data_cadastro",
                        "precatorio", "status", "valor"
                    ]
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()

                    for edital in editais:
                        # Formatar dados para CSV
                        csv_row = {}
                        for field in fieldnames:
                            value = edital.get(field, "-")
                            if field == "data_cadastro" and isinstance(value, str):
                                # Já está formatado como dd/mm/yyyy
                                csv_row[field] = value
                            elif field == "valor" and isinstance(value, Decimal):
                                csv_row[field] = format_currency(float(value))
                            else:
                                csv_row[field] = str(value) if value != "-" else ""

                        writer.writerow(csv_row)
                else:
                    # Cabeçalhos mesmo se vazio
                    writer = csv.writer(f)
                    writer.writerow([
                        "ordem", "ano_orcamento", "natureza", "data_cadastro",
                        "precatorio", "status", "valor"
                    ])

            logger.info(f"Editais salvos em {out_file}", count=len(editais))

        except IOError as e:
            logger.error(f"Erro de I/O ao salvar editais: {e}")
        except Exception as e:
            logger.error(f"Erro inesperado ao salvar editais: {e}")

    def get_and_save_editais(self, out_file: str) -> List[Dict[str, Any]]:
        """Orquestra busca e salvamento de editais."""
        logger.info(f"Iniciando coleta de editais para: {out_file}")

        editais = self.fetch_editais()
        if not editais:
            logger.warning("Nenhum edital encontrado")
            self.save_editais([], out_file)
            return []

        self.save_editais(editais, out_file)
        logger.info(f"{len(editais)} editais processados")

        return editais
