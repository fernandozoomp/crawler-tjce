#!/usr/bin/env python3
import argparse
import csv
import json
import logging
from datetime import datetime
import os
import io
import uuid

import requests
from flask import Flask, request, Response, jsonify

# ——— PONTOS A CONFIGURAR ——————————————————————————————————————————
API_URL = (
    "https://wabi-brazil-south-b-primary-api.analysis.windows.net/"
    "public/reports/querydata?synchronous=true"
)

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "ActivityId": "ce773b1d-9336-26fd-0afd-28d03f446ded",
    "Connection": "keep-alive",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://app.powerbi.com",
    "Referer": "https://app.powerbi.com/",
    "RequestId": "5c3f04c9-e312-f232-924a-08fbff7695da",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    ),
    "X-PowerBI-ResourceKey": "e8c26605-679c-4da5-80d5-423cc8062db2",
    "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("circulana_fetcher")

# ——— ESTRUTURA DO PAYLOAD ——————————————————————————————————————
_PAYLOAD_STRUCTURE = {
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
                                            "Property": "dfslcp_dsc_proc_precatorio",
                                        },
                                        "Name": "dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_dsc_proc_precatorio",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "d"}
                                            },
                                            "Property": "dfslcp_num_ano_orcamento",
                                        },
                                        "Name": "Sum(dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_num_ano_orcamento)",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "d"}
                                            },
                                            "Property": "dfslcp_dsc_natureza",
                                        },
                                        "Name": "dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_dsc_natureza",
                                    },
                                    {
                                        "HierarchyLevel": {
                                            "Expression": {
                                                "Hierarchy": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "d"}
                                                    },
                                                    "Hierarchy": "Data_Cadastro",
                                                }
                                            },
                                            "Level": "Data Cadastro",
                                        },
                                        "Name": (
                                            "dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_dat_cadastro"
                                            " Hierarquia.dfslcp_dat_cadastro"
                                        ),
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "d"}
                                            },
                                            "Property": "dfslcp_dsc_tipo_classificao",
                                        },
                                        "Name": "dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_dsc_tipo_classificao",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "d"}
                                            },
                                            "Property": "dfslcp_vlr_original",
                                        },
                                        "Name": "Sum(dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_vlr_original)",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "d"}
                                            },
                                            "Property": "dfslcp_num_ordem",
                                        },
                                        "Name": "Sum(dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_num_ordem)",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "d"}
                                            },
                                            "Property": "dfslcp_dsc_sit_precatorio",
                                        },
                                        "Name": "dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_dsc_sit_precatorio",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "d"}
                                            },
                                            "Property": "dfslcp_dsc_comarca",
                                        },
                                        "Name": "dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_dsc_comarca",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "d"}
                                            },
                                            "Property": "ValorAtualFormatado",
                                        },
                                        "Name": "dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.ValorAtualFormatado",
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
                                                                "SourceRef": {
                                                                    "Source": "d"
                                                                }
                                                            },
                                                            "Property": "dfslcp_dsc_entidade",
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": "'MUNICÍPIO DE FORTALEZA'"
                                                            }
                                                        }
                                                    ]
                                                ],
                                            }
                                        }
                                    }
                                ],
                                "OrderBy": [
                                    {
                                        "Direction": 1,
                                        "Expression": {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "d"}
                                                },
                                                "Property": "dfslcp_num_ordem",
                                            }
                                        },
                                    }
                                ],
                            },
                            "Binding": {
                                "Primary": {
                                    "Groupings": [
                                        {
                                            "Projections": [
                                                0,
                                                1,
                                                2,
                                                3,
                                                4,
                                                5,
                                                6,
                                                7,
                                                8,
                                                9,
                                            ],
                                            "Subtotal": 1,
                                        }
                                    ]
                                },
                                "DataReduction": {
                                    "DataVolume": 3,
                                    "Primary": {"Window": {"Count": 500}},
                                },
                                "Version": 1,
                            },
                            "ExecutionMetricsKind": 1,
                        }
                    }
                ]
            },
            "QueryId": "",
            "ApplicationContext": {
                "DatasetId": "4c290280-9235-4dd4-a48e-888f14efb2d8",
                "Sources": [
                    {
                        "ReportId": "e610bea8-b5e1-4bdf-84b5-db63928dfcd9",
                        "VisualId": "99f187e38dbe0509eab4",
                    }
                ],
            },
        }
    ],
    "cancelQueries": [],
    "modelId": 4287487,
}


# ——— FUNÇÃO DE FETCH + INJEÇÃO DE ENTIDADE E PAGINAÇÃO ——————————————————
def fetch_data(entity: str) -> dict:
    val = f"'{entity}'"
    page_count = 0
    all_dm0_items = []
    all_value_dicts = {}
    restart_tokens = None
    first_page_descriptor = None
    last_successful_resp_json_page = None

    while True:
        page_count += 1
        current_headers = HEADERS.copy()
        current_headers["ActivityId"] = str(uuid.uuid4())
        current_headers["RequestId"] = str(uuid.uuid4())

        payload_instance = json.loads(json.dumps(_PAYLOAD_STRUCTURE))

        try:
            payload_query_command = payload_instance["queries"][0]["Query"]["Commands"][
                0
            ]["SemanticQueryDataShapeCommand"]

            # Ajusta o payload para solicitar explicitamente os ValueDicts
            if page_count == 1:
                logger.info(
                    "Estrutura do payload da primeira página: "
                    f"{json.dumps(payload_instance, indent=2)[:1000]}..."
                )

            # Modifica a condição Where para a entidade
            payload_query_command["Query"]["Where"][0]["Condition"]["In"]["Values"][0][
                0
            ]["Literal"]["Value"] = val

            # Ajusta as configurações de DataReduction
            data_reduction_binding = payload_query_command["Binding"]["DataReduction"][
                "Primary"
            ]
            if "Window" not in data_reduction_binding:
                data_reduction_binding["Window"] = {}

            # Configura a janela de paginação
            window_binding = data_reduction_binding["Window"]
            window_binding["Count"] = 500

            # Adiciona RestartTokens se disponível
            if restart_tokens:
                window_binding["RestartTokens"] = restart_tokens
            elif "RestartTokens" in window_binding:
                del window_binding["RestartTokens"]

            logger.info(
                f"Página {page_count}: Enviando payload com Window: {json.dumps(window_binding)}"
            )

        except (KeyError, IndexError) as e:
            logger.error(f"Erro ao tentar modificar o payload para paginação: {e}")
            raise ValueError(
                "Estrutura do payload inesperada ao tentar injetar entidade ou tokens."
            )

        resp = requests.post(
            API_URL, headers=current_headers, json=payload_instance, timeout=60
        )
        resp.raise_for_status()
        resp_json_page = resp.json()
        last_successful_resp_json_page = resp_json_page

        # Log detalhado da primeira resposta
        if page_count == 1:
            logger.info(
                "Estrutura completa da primeira resposta: "
                f"{json.dumps(resp_json_page, indent=2)}"
            )

        # Extrai dados desta página
        try:
            result_data = resp_json_page["results"][0]["result"]["data"]
            current_dsr = result_data.get("dsr")

            if not current_dsr:
                logger.warning(
                    f"Chave DSR não encontrada na resposta da página {page_count}. "
                    f"Resposta: {json.dumps(resp_json_page)}"
                )
                break

            # Captura o descriptor da primeira página
            if page_count == 1:
                first_page_descriptor = result_data.get("descriptor")
                if first_page_descriptor:
                    logger.info(
                        "Descriptor da primeira página: "
                        f"{json.dumps(first_page_descriptor, indent=2)}"
                    )

            # Define current_dm0_list ANTES de usá-lo no log
            ds_list = current_dsr.get("DS", [])
            if ds_list:
                logger.info(f"Página {page_count}: DS tem {len(ds_list)} elementos")
                for ds_idx, ds in enumerate(ds_list):
                    ph_list = ds.get("PH", [])
                    logger.info(f"  DS[{ds_idx}]: PH tem {len(ph_list)} elementos")
                    for ph_idx, ph in enumerate(ph_list):
                        dm0_list = ph.get("DM0", [])
                        logger.info(
                            f"    PH[{ph_idx}]: DM0 tem {len(dm0_list)} elementos"
                        )

            current_dm0_list = (
                current_dsr.get("DS", [{}])[0].get("PH", [{}])[0].get("DM0", [])
            )

            # Log detalhado dos ValueDicts recebidos
            current_value_dicts = current_dsr.get("ValueDicts", {})
            if current_value_dicts:
                logger.info(
                    f"Página {page_count}: ValueDicts recebidos: "
                    f"{list(current_value_dicts.keys())}"
                )
                for key, values in current_value_dicts.items():
                    preview = values[:3] if len(values) > 3 else values
                    logger.info(
                        f"  ValueDict['{key}']: {len(values)} valores. "
                        f"Amostra: {preview}"
                    )
            else:
                logger.warning(f"Página {page_count}: Nenhum ValueDict na resposta!")

            dsr_ic_value = current_dsr.get("IC")  # IsComplete token
            logger.info(
                f"Página {page_count}: Itens DM0: {len(current_dm0_list)}, "
                f"DSR IC: {dsr_ic_value}"
            )

            # Atualiza os dados agregados
            all_dm0_items.extend(current_dm0_list)
            all_value_dicts.update(current_value_dicts)

            # Verifica se há mais páginas
            restart_tokens = current_dsr.get("RT")
            if restart_tokens:
                logger.info(
                    f"Página {page_count}: RestartTokens (RT) para próxima página: "
                    f"{json.dumps(restart_tokens)}"
                )
            else:
                logger.info(
                    f"Fim da paginação na página {page_count}: "
                    "Não há RestartTokens (RT) na resposta."
                )
                break

            # Se não houver mais itens e não houver RT, termina
            if not current_dm0_list and not restart_tokens:
                logger.info(
                    f"Página {page_count}: Sem mais itens e sem RT, "
                    "finalizando paginação."
                )
                break

        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"Erro ao processar a resposta da página {page_count}: {e}")
            logger.debug(f"Resposta JSON da página com erro: {resp_json_page}")
            break

    logger.info(
        f"Total de {len(all_dm0_items)} itens buscados em {page_count} página(s). "
        f"ValueDicts agregados: {list(all_value_dicts.keys())}"
    )

    final_descriptor = first_page_descriptor
    if not final_descriptor and last_successful_resp_json_page:
        try:
            final_descriptor = last_successful_resp_json_page["results"][0]["result"][
                "data"
            ]["descriptor"]
        except (KeyError, IndexError, TypeError):
            logger.warning("Não foi possível extrair o descriptor da última resposta.")
            final_descriptor = {}
    elif not final_descriptor:
        final_descriptor = {}

    final_aggregated_json = {
        "results": [
            {
                "result": {
                    "data": {
                        "descriptor": final_descriptor,
                        "dsr": {
                            "DS": [{"PH": [{"DM0": all_dm0_items}]}],
                            "ValueDicts": all_value_dicts,
                        },
                    }
                }
            }
        ]
    }
    return final_aggregated_json


# ——— NORMALIZAÇÃO PARA CSV —————————————————————————————————————————————
def _get_value_from_dict(value_dicts, dict_key, index, default_if_missing=None):
    """Helper para buscar valor de um ValueDict de forma segura."""
    if index is None:
        return default_if_missing

    # Se o dicionário não existe ou o índice não é inteiro, retorna o valor padrão
    # sem gerar warning (isso é esperado para alguns campos)
    if not value_dicts or dict_key not in value_dicts or not isinstance(index, int):
        return str(index) if default_if_missing is None else default_if_missing

    dictionary = value_dicts[dict_key]
    if 0 <= index < len(dictionary):
        return dictionary[index]

    # Se chegou aqui, temos um índice numérico válido mas fora dos limites
    # Isso sim merece um warning
    logger.warning(
        f"Índice {index} fora dos limites do ValueDict['{dict_key}'] "
        f"(tamanho: {len(dictionary)}). Usando valor padrão."
    )
    return str(index) if default_if_missing is None else default_if_missing


def normalize_to_rows(resp_json_aggregated: dict) -> list[dict]:
    try:
        result_data = resp_json_aggregated["results"][0]["result"]["data"]
        dsr = result_data["dsr"]
    except (KeyError, IndexError, TypeError):
        logger.warning(
            "Estrutura principal (results/result/data/dsr) não encontrada "
            "ou inválida na resposta agregada."
        )
        return []

    value_dicts = dsr.get("ValueDicts", {})
    logger.info("Iniciando normalize_to_rows. Analisando ValueDicts...")
    if value_dicts:
        logger.info(f"ValueDicts disponíveis: {list(value_dicts.keys())}")
        for key, values in value_dicts.items():
            logger.info(f"ValueDict['{key}']: {len(values)} valores")
            if len(values) > 0:
                logger.info(f"  Primeiros valores de '{key}': {values[:5]}")
    else:
        logger.warning("Nenhum ValueDict encontrado na resposta!")

    dm_list = dsr.get("DS", [{}])[0].get("PH", [{}])[0].get("DM0", [])
    logger.info(f"Total de {len(dm_list)} itens DM0 para processar")

    if not dm_list:
        logger.warning("DM0 não encontrado ou vazio na resposta agregada.")
        return []

    # Configuração dos campos com seus valores padrão e validações
    field_configs = {
        "processo": {
            "idx": 0,
            "dict": None,  # Processo vem direto no C
            "default": "",
            "required": True,
            "min_length": 6,
        },
        "ano_orcamento": {"idx": 1, "dict": None, "default": "", "required": True},
        "natureza": {
            "idx": 2,
            "dict": "D1",
            "default": "Não especificada",
            "required": True,
        },
        "data_cadastro": {"idx": 3, "dict": None, "default": "", "required": True},
        "tipo_classificacao": {
            "idx": 4,
            "dict": "D2",
            "default": "Não especificado",
            "required": True,
        },
        "valor_original": {"idx": 5, "dict": None, "default": "", "required": True},
        "ordem": {"idx": 6, "dict": None, "default": "", "required": True},
        "situacao": {
            "idx": 7,
            "dict": "D3",
            "default": "Não especificada",
            "required": True,
        },
        "comarca": {
            "idx": 8,
            "dict": "D4",
            "default": "Não especificada",
            "required": True,
        },
        "valor_atual": {
            "idx": 9,
            "dict": None,  # Valor atual vem formatado direto no C
            "default": "",
            "required": True,
        },
    }

    # Verifica quais campos com ValueDicts estão faltando
    missing_dicts = [
        f"{field}: {config['dict']}"
        for field, config in field_configs.items()
        if config["dict"] and config["dict"] not in value_dicts
    ]
    if missing_dicts:
        logger.warning(
            "Os seguintes campos não têm ValueDicts correspondentes e usarão "
            f"valores padrão: {', '.join(missing_dicts)}"
        )

    num_fields = 10  # G0 a G9
    last_row_raw_data = [None] * num_fields
    output_rows = []
    rows_processed = 0
    rows_valid = 0
    rows_invalid = 0
    invalid_reasons = {}

    for dm_item in dm_list:
        rows_processed += 1
        c_sparse = dm_item.get("C")
        if c_sparse is None:
            logger.warning(f"Item DM0 sem chave 'C': {dm_item}")
            rows_invalid += 1
            invalid_reasons["sem_chave_C"] = invalid_reasons.get("sem_chave_C", 0) + 1
            continue

        r_mask = dm_item.get("R", 0)
        current_row_reconstructed_raw = [None] * num_fields
        sparse_idx = 0

        for i in range(num_fields):
            if (r_mask >> i) & 1:
                current_row_reconstructed_raw[i] = last_row_raw_data[i]
            else:
                if sparse_idx < len(c_sparse):
                    current_row_reconstructed_raw[i] = c_sparse[sparse_idx]
                    sparse_idx += 1
                else:
                    current_row_reconstructed_raw[i] = None

        last_row_raw_data = list(current_row_reconstructed_raw)
        row_dict = {}

        # Mapeamento e transformação de dados usando os valores padrão configurados
        for field, config in field_configs.items():
            raw_value = current_row_reconstructed_raw[config["idx"]]

            if field == "data_cadastro" and raw_value is not None:
                try:
                    # Converte timestamp em milissegundos para data
                    if isinstance(raw_value, (int, float)):
                        row_dict[field] = datetime.utcfromtimestamp(
                            raw_value / 1000
                        ).strftime("%Y-%m-%d")
                    else:
                        row_dict[field] = config["default"]
                except (TypeError, ValueError):
                    logger.debug(
                        f"Valor inválido para timestamp de data_cadastro: {raw_value}"
                    )
                    row_dict[field] = config["default"]
            elif config["dict"] is not None and raw_value is not None:
                # Campos que usam ValueDict
                # Converte o valor para int se for string numérica
                dict_index = (
                    int(raw_value)
                    if isinstance(raw_value, str) and raw_value.isdigit()
                    else raw_value
                )

                # Tenta buscar o valor no ValueDict
                if isinstance(dict_index, int) and 0 <= dict_index < len(
                    value_dicts.get(config["dict"], [])
                ):
                    row_dict[field] = value_dicts[config["dict"]][dict_index]
                else:
                    # Se não encontrar no ValueDict, usa o valor padrão
                    row_dict[field] = config["default"]
                    logger.debug(
                        f"Valor {dict_index} não encontrado no ValueDict['{config['dict']}'], "
                        f"usando valor padrão para {field}"
                    )
            else:
                # Campos que não usam ValueDict
                if raw_value is not None:
                    # Remove aspas e espaços extras
                    value_str = str(raw_value).strip().strip("'\"")
                    # Formata números se necessário
                    if field in ["valor_original", "ordem"]:
                        try:
                            value_str = str(float(value_str))
                        except ValueError:
                            pass
                    row_dict[field] = value_str
                else:
                    row_dict[field] = config["default"]

        # Validação da linha
        is_row_valid = True
        validation_errors = []

        # Validações específicas
        processo_valor = str(row_dict.get("processo", "")).strip()
        if len(processo_valor) < field_configs["processo"]["min_length"]:
            is_row_valid = False
            validation_errors.append(
                f"processo '{processo_valor}' tem menos de "
                f"{field_configs['processo']['min_length']} caracteres"
            )

        # Validação de campos obrigatórios
        for field, config in field_configs.items():
            if config["required"]:
                value = row_dict.get(field)
                if value is None or str(value).strip() == "":
                    is_row_valid = False
                    validation_errors.append(f"campo '{field}' está vazio")

        if is_row_valid:
            output_rows.append(row_dict)
            rows_valid += 1
        else:
            rows_invalid += 1
            error_key = ", ".join(validation_errors)
            invalid_reasons[error_key] = invalid_reasons.get(error_key, 0) + 1

    # Log do resumo do processamento
    logger.info(
        f"Processamento concluído: {rows_processed} linhas processadas, "
        f"{rows_valid} válidas, {rows_invalid} inválidas"
    )
    if invalid_reasons:
        logger.info("Motivos de invalidação:")
        for reason, count in invalid_reasons.items():
            logger.info(f"  - {reason}: {count} ocorrências")

    return output_rows


# ——— CSV WRITER ————————————————————————————————————————————————————
def write_csv(rows: list[dict], out_file: str):
    if not rows:
        logger.info(f"Nenhuma linha para escrever em {out_file} após filtragem.")
        # Cabeçalhos padrão para arquivo CSV, caso todas as linhas sejam filtradas
        fieldnames = [
            "processo",
            "ano_orcamento",
            "natureza",
            "data_cadastro",
            "tipo_classificacao",
            "valor_original",
            "ordem",
            "situacao",
            "comarca",
            "valor_atual",
        ]
        with open(out_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=fieldnames,
                quoting=csv.QUOTE_MINIMAL,
                quotechar='"',
                delimiter=",",
            )
            writer.writeheader()
        logger.info(f"CSV vazio (apenas cabeçalhos) salvo em {out_file}")
        return

    # Ordem fixa dos campos
    fieldnames = [
        "processo",
        "ano_orcamento",
        "natureza",
        "data_cadastro",
        "tipo_classificacao",
        "valor_original",
        "ordem",
        "situacao",
        "comarca",
        "valor_atual",
    ]

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            quoting=csv.QUOTE_MINIMAL,
            quotechar='"',
            delimiter=",",
        )
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"CSV salvo em {out_file}")


# ——— CLI ————————————————————————————————————————————————————————
def cli():
    p = argparse.ArgumentParser(description="Fetcher PowerBI → CSV")
    p.add_argument(
        "-e",
        "--entity",
        required=True,
        help="Nome da ENTIDADE (ex: MUNICÍPIO DE FORTALEZA)",
    )
    p.add_argument("-o", "--output", default="output.csv", help="Arquivo de saída CSV")
    args = p.parse_args()

    logger.info(f"Buscando dados para entidade: {args.entity}")
    try:
        data_aggregated = fetch_data(args.entity)  # Agora retorna dados agregados
        rows = normalize_to_rows(data_aggregated)  # Processa dados agregados
        write_csv(rows, args.output)
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro na requisição HTTP: {e}")
    except ValueError as e:
        logger.error(f"Erro no processamento dos dados (ValueError): {e}")
    except KeyError as e:
        logger.error(
            f"Erro no processamento dos dados (KeyError - estrutura JSON inesperada?): {e}"
        )
    except Exception as e:
        logger.error(f"Um erro inesperado ocorreu: {e}", exc_info=True)


# ——— FLASK API ————————————————————————————————————————————————————
app = Flask(__name__)


@app.route("/fetch", methods=["GET"])
def api_fetch():
    entity = request.args.get("e", "")
    if not entity:
        return jsonify({"error": "Parametro `e` (entity) é obrigatório"}), 400
    try:
        data_aggregated = fetch_data(entity)  # Agora retorna dados agregados
        rows = normalize_to_rows(data_aggregated)  # Processa dados agregados

        string_io_buffer = io.StringIO()

        if rows:
            fieldnames = list(rows[0].keys())
            writer = csv.DictWriter(string_io_buffer, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        else:
            # Cabeçalhos padrão para CSV vazio
            fieldnames_default = [
                "processo",
                "ano_orcamento",
                "natureza",
                "data_cadastro",
                "tipo_classificacao",
                "valor_original",
                "ordem",
                "situacao",
                "comarca",
                "valor_atual",
            ]
            writer = csv.DictWriter(string_io_buffer, fieldnames=fieldnames_default)
            writer.writeheader()
            logger.info("Nenhuma linha para retornar na API após filtragem.")

        csv_data = string_io_buffer.getvalue()
        string_io_buffer.close()

        return Response(csv_data, mimetype="text/csv")
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro na API (RequestException): {e}")
        return jsonify({"error": f"Erro na requisição HTTP: {e}"}), 500
    except ValueError as e:
        logger.error(f"Erro na API (ValueError): {e}")
        return jsonify({"error": f"Erro no processamento dos dados: {e}"}), 500
    except KeyError as e:
        logger.error(f"Erro na API (KeyError): {e}")
        return jsonify({"error": f"Estrutura de dados inesperada: {e}"}), 500
    except Exception as err:
        logger.exception("Erro inesperado na API")
        return jsonify({"error": f"Erro interno no servidor: {str(err)}"}), 500


# ——— PONTO DE ENTRADA ——————————————————————————————————————————————
if __name__ == "__main__":
    import sys

    # io já importado no topo

    # Verifica se o script está sendo executado pelo Flask CLI ou diretamente
    is_flask_run = False
    if "FLASK_APP" in os.environ and os.environ["FLASK_APP"] == __name__ + ".py":
        is_flask_run = True
    elif len(sys.argv) > 1 and sys.argv[0].endswith("flask") and sys.argv[1] == "run":
        is_flask_run = True

    if not is_flask_run:
        if not ("flask" in sys.argv[0].lower() and "run" in sys.argv):
            cli()
