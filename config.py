#!/usr/bin/env python3
import os
import uuid
from dotenv import load_dotenv
from dataclasses import dataclass, field
from typing import (
    Dict,
    List,
    Optional
)  # Adicionado Any para compatibilidade com PAYLOAD_STRUCTURE se necessário, mas Dict e List são os principais.

# Carrega as variáveis de ambiente
load_dotenv()


@dataclass
class CrawlerConfig:
    api_url: str = field(
        default_factory=lambda: os.getenv(
            "API_URL",
            "https://wabi-brazil-south-b-primary-api.analysis.windows.net/public/reports/querydata",
        )
    )
    batch_size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "500")))
    resource_key: str = field(
        default_factory=lambda: os.getenv(
            "POWERBI_RESOURCE_KEY", "e8c26605-679c-4da5-80d5-423cc8062db2"
        )
    )
    default_entity: str = "MUNICIPIO DE FORTALEZA"
    model_id: int = field(default_factory=lambda: int(os.getenv("MODEL_ID", "4287487")))
    entities_output_filename: str = field(default_factory=lambda: os.getenv("ENTITIES_OUTPUT_FILENAME", "entidades_tjce.csv"))

    # Configurações de Cache
    cache_default_timeout: int = field(default_factory=lambda: int(os.getenv("CACHE_DEFAULT_TIMEOUT", "300")))
    cache_timeout_entities: int = field(default_factory=lambda: int(os.getenv("CACHE_TIMEOUT_ENTITIES", "3600")))

    # Configurações de Rate Limit
    rate_limit_default: str = field(default_factory=lambda: os.getenv("RATE_LIMIT_DEFAULT", "200 per day,50 per hour"))
    rate_limit_entities: str = field(default_factory=lambda: os.getenv("RATE_LIMIT_ENTITIES", "60 per hour"))
    rate_limit_fetch: str = field(default_factory=lambda: os.getenv("RATE_LIMIT_FETCH", "50 per hour,10 per minute"))

    # Configurações do Pinata
    pinata_api_jwt: Optional[str] = field(default_factory=lambda: os.getenv("PINATA_API_JWT"))
    pinata_gateway_url: Optional[str] = field(default_factory=lambda: os.getenv("PINATA_GATEWAY_URL", "https://gateway.pinata.cloud/ipfs/"))
    pinata_api_upload_url: str = "https://api.pinata.cloud/pinning/pinFileToIPFS"

    # Configurações do Flask
    flask_debug_mode: bool = field(default_factory=lambda: os.getenv("FLASK_DEBUG_MODE", "False").lower() == "true")
    flask_port: int = field(default_factory=lambda: int(os.getenv("FLASK_PORT", "5000")))

    def __post_init__(self):
        if not self.api_url.endswith("synchronous=true"):
            self.api_url = f"{self.api_url}{'?' if '?' not in self.api_url else '&'}synchronous=true"

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "ActivityId": str(
                uuid.uuid4()
            ),  # Gerar novo ActivityId a cada chamada de headers
            "Connection": "keep-alive",
            "Content-Type": "application/json;charset=UTF-8",
            "Origin": "https://app.powerbi.com",
            "RequestId": str(
                uuid.uuid4()
            ),  # Gerar novo RequestId a cada chamada de headers
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "cross-site",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
            ),
            "X-PowerBI-ResourceKey": self.resource_key,
            "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }


@dataclass
class FieldConfig:
    csv_fields: List[str] = field(
        default_factory=lambda: [
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
    )

    field_mapping: Dict[str, Dict[str, str]] = field(
        default_factory=lambda: {
            "ordem": {"type": "int", "default": "0", "api_name": "dfslcp_num_ordem"},
            "processo": {"type": "processo", "api_name": "dfslcp_dsc_proc_precatorio"},
            "comarca": {
                "type": "str",
                "default": "-",
                "api_name": "dfslcp_dsc_comarca",
            },
            "ano_orcamento": {
                "type": "int",
                "default": "2024",
                "api_name": "dfslcp_num_ano_orcamento",
            },
            "natureza": {
                "type": "str",
                "default": "-",
                "api_name": "dfslcp_dsc_natureza",
            },
            "data_cadastro": {"type": "date", "api_name": "dfslcp_dat_cadastro"},
            "tipo_classificacao": {
                "type": "str",
                "default": "-",
                "api_name": "dfslcp_dsc_tipo_classificao",
            },
            "valor_original": {
                "type": "float",  # Mantido como float conforme config_new, mas Pydantic o tratará como Decimal
                "default": "0.0",
                "api_name": "dfslcp_vlr_original",
            },
            "valor_atual": {
                "type": "Decimal",  # Corrigido para Decimal como estava em config_new FieldConfig
                "default": "0.0",
                "api_name": "ValorAtualFormatado",
            },
            "situacao": {
                "type": "str",
                "default": "-",
                "api_name": "dfslcp_dsc_sit_precatorio",
            },
        }
    )


config = CrawlerConfig()
field_config = FieldConfig()

# Estrutura do payload para a API (mantida do config.py original)
# É importante que os "Property" aqui correspondam aos "api_name" em FieldConfig.
# Especialmente, "dfslcp_vlr_atual" foi confirmado.
PAYLOAD_STRUCTURE = {
    "version": 1,
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
                                                                "Value": "'MUNICÍPIO DE FORTALEZA'"  # Mantém o default para o payload base
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
                                        "Direction": 1,  # Default para primeira busca
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
                                    "DataVolume": 3,  # Mantido conforme última configuração funcional
                                    "Primary": {
                                        "Window": {"Count": 1}
                                    },  # Mantido para teste de registro único
                                },
                                "Version": 1,
                            },
                            "ExecutionMetricsKind": 1,
                        }
                    }
                ]
            },
            "CacheKey": str(uuid.uuid4()),  # Gerar novo CacheKey
            "ApplicationContext": {
                "DatasetId": "a5921770-b898-442d-9693-d0393d3e7996",
                "Sources": [
                    {
                        "ReportId": "69f1b060-9e50-402e-99c9-5592f8b001c8",
                        "VisualId": "f6d03712b8e8502833a0",
                    }
                ],
            },
        }
    ],
    "cancelQueries": [],
    "modelId": 4287487,
}
