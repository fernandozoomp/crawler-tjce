#!/usr/bin/env python3
import os
import uuid
from dotenv import load_dotenv

# Carrega as variáveis de ambiente
load_dotenv()

API_URL = os.getenv(
    "API_URL",
    "https://wabi-brazil-south-b-primary-api.analysis.windows.net/public/reports/querydata",
)

if not API_URL.endswith("synchronous=true"):
    API_URL = f"{API_URL}{'?' if '?' not in API_URL else '&'}synchronous=true"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "ActivityId": str(uuid.uuid4()),
    "Connection": "keep-alive",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://app.powerbi.com",
    "RequestId": str(uuid.uuid4()),
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    ),
    "X-PowerBI-ResourceKey": os.getenv(
        "POWERBI_RESOURCE_KEY", "e8c26605-679c-4da5-80d5-423cc8062db2"
    ),
    "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

# Campos do CSV na ordem correta
CSV_FIELDS = [
    "ordem",
    "processo",
    "comarca",
    "ano_orcamento",
    "natureza",
    "data_cadastro",
    "tipo_classificacao",
    "valor_original",
    "valor_atual",
    "situacao",
]

# Mapeamento de campos da API para campos do CSV
FIELD_MAPPING = {
    "ordem": {"property": "dfslcp_num_ordem", "type": "int"},
    "processo": {"property": "dfslcp_dsc_proc_precatorio", "type": "str"},
    "comarca": {"property": "dfslcp_dsc_comarca", "type": "str"},
    "ano_orcamento": {"property": "dfslcp_num_ano_orcamento", "type": "int"},
    "natureza": {"property": "dfslcp_dsc_natureza", "type": "str"},
    "data_cadastro": {"property": "dfslcp_dat_cadastro", "type": "date"},
    "tipo_classificacao": {"property": "dfslcp_dsc_tipo_classificao", "type": "str"},
    "valor_original": {"property": "dfslcp_vlr_original", "type": "currency"},
    "valor_atual": {"property": "ValorAtualFormatado", "type": "currency"},
    "situacao": {"property": "dfslcp_dsc_sit_precatorio", "type": "str"},
}

# Estrutura do payload para a API
PAYLOAD_STRUCTURE = {
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
                                        "Name": "dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_num_ano_orcamento",
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
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "d"}
                                            },
                                            "Property": "dfslcp_dat_cadastro",
                                        },
                                        "Name": "dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_dat_cadastro",
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
                                        "Name": "dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_vlr_original",
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "d"}
                                            },
                                            "Property": "dfslcp_num_ordem",
                                        },
                                        "Name": "dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_num_ordem",
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
                                            "Property": "dfslcp_vlr_atual",
                                        },
                                        "Name": "dfslcp_SAPRE_LISTA_CRONO_PRECATORIO.dfslcp_vlr_atual",
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
                                        {"Projections": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]}
                                    ]
                                },
                                "DataReduction": {
                                    "DataVolume": 3,
                                    "Primary": {
                                        "Window": {
                                            "Count": int(os.getenv("BATCH_SIZE", "500"))
                                        }
                                    },
                                },
                                "Version": 1,
                            },
                            "ExecutionMetricsKind": 1,
                        }
                    }
                ]
            },
            "CacheKey": str(uuid.uuid4()),
        }
    ],
    "cancelQueries": [],
    "modelId": 4287487,
}
