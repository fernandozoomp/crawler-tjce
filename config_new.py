#!/usr/bin/env python3
from dataclasses import dataclass, field
from typing import Dict, Any, List
import os
import uuid
from dotenv import load_dotenv

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

    def __post_init__(self):
        if not self.api_url.endswith("synchronous=true"):
            self.api_url = f"{self.api_url}{'?' if '?' not in self.api_url else '&'}synchronous=true"

    @property
    def headers(self) -> Dict[str, str]:
        return {
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
                "type": "float",
                "default": "0.0",
                "api_name": "dfslcp_vlr_original",
            },
            "valor_atual": {
                "type": "float",
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
