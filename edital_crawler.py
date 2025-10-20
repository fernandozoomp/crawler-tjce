#!/usr/bin/env python3
import requests
import os
import csv
import json
from typing import List, Optional, Dict, Any
from logger import get_logger
from config import config
from datetime import datetime

logger = get_logger(__name__)


class EditalCrawler:
    def __init__(self):
        self.api_url = config.api_url
        self.headers = config.headers
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.page_size = getattr(config, "edital_page_size", 500)
        self.max_pages = getattr(config, "max_edital_pages", 100)

    def _build_edital_payload(
        self, last_processo: Optional[str] = None
    ) -> Dict[str, Any]:
        """Constrói o payload para a requisição de editais, com suporte à paginação."""
        query_binding: Dict[str, Any] = {
            "Primary": {"Groupings": [{"Projections": [0]}]},
            "DataReduction": {
                "DataVolume": 3,
                "Primary": {
                    "Window": {}
                },
            },
            "IncludeEmptyGroups": True,
            "Version": 1,
        }

        # Adiciona RestartTokens se last_processo for fornecido
        if last_processo:
            formatted_token = f"'{last_processo}'"
            query_binding["DataReduction"]["Primary"]["Window"]["RestartTokens"] = [
                [formatted_token]
            ]
            logger.debug(f"Construindo payload com RestartToken: {formatted_token}")

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
                                                "Entity": "EDITAIS_PUBLICADOS",
                                                "Type": 0,
                                            }
                                        ],
                                        "Select": [
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "e"}
                                                    },
                                                    "Property": "NUMERO_PROCESSO",
                                                },
                                                "Name": "EDITAIS_PUBLICADOS.NUMERO_PROCESSO",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "e"}
                                                    },
                                                    "Property": "TIPO_EDITAL",
                                                },
                                                "Name": "EDITAIS_PUBLICADOS.TIPO_EDITAL",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "e"}
                                                    },
                                                    "Property": "DATA_PUBLICACAO",
                                                },
                                                "Name": "EDITAIS_PUBLICADOS.DATA_PUBLICACAO",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "e"}
                                                    },
                                                    "Property": "OBJETO",
                                                },
                                                "Name": "EDITAIS_PUBLICADOS.OBJETO",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "e"}
                                                    },
                                                    "Property": "ENTIDADE",
                                                },
                                                "Name": "EDITAIS_PUBLICADOS.ENTIDADE",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "e"}
                                                    },
                                                    "Property": "VALOR_ESTIMADO",
                                                },
                                                "Name": "EDITAIS_PUBLICADOS.VALOR_ESTIMADO",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "e"}
                                                    },
                                                    "Property": "SITUACAO",
                                                },
                                                "Name": "EDITAIS_PUBLICADOS.SITUACAO",
                                            },
                                        ],
                                        "OrderBy": [
                                            {
                                                "Direction": 1,
                                                "Expression": {
                                                    "Column": {
                                                        "Expression": {
                                                            "SourceRef": {"Source": "e"}
                                                        },
                                                        "Property": "DATA_PUBLICACAO",
                                                    }
                                                },
                                            }
                                        ],
                                    },
                                    "Binding": query_binding,
                                }
                            }
                        ],
                        "QueryId": "",
                    }
                }
            ],
            "cancelQueries": [],
            "modelId": getattr(config, "edital_model_id", 4287487),
        }
        return payload

    def _parse_editais_from_data(self, data: Dict[str, Any]) -> List[str]:
        """Extrai dados de editais da resposta JSON da API."""
        editais_in_page: List[Dict[str, Any]] = []
        try:
            result_data = data["results"][0]["result"]["data"]
            dsr = result_data.get("dsr", {})
            ds_list = dsr.get("DS", [])

            for ds_item in ds_list:
                if "PH" not in ds_item:
                    continue
                for ph_item in ds_item["PH"]:
                    if "DM0" not in ph_item:
                        continue
                    for dm0_item in ph_item["DM0"]:
                        # Cada item DM0 representa um edital
                        processo = dm0_item.get("C", [""])[0] if "C" in dm0_item and dm0_item["C"] else ""

                        if processo and processo.strip():
                            edital_data = {
                                "numero_processo": processo.strip(),
                                "tipo_edital": "",
                                "data_publicacao": "",
                                "objeto": "",
                                "entidade": "",
                                "valor_estimado": "",
                                "situacao": ""
                            }
                            editais_in_page.append(edital_data)

        except (IndexError, KeyError, TypeError) as e:
            logger.warning(
                f"Estrutura inesperada nos dados da API ao parsear editais: {e}",
                exc_info=True,
                response_data=data,
            )
            return []

        return editais_in_page

    def fetch_editais(self) -> List[Dict[str, Any]]:
        """Busca a lista de todos os editais disponíveis, lidando com paginação."""
        all_editais = []
        last_processo: Optional[str] = None
        page_count = 0

        while True:
            page_count += 1
            logger.info(
                f"Buscando página {page_count} de editais. Último processo: {last_processo or 'Nenhum'}"
            )

            payload = self._build_edital_payload(last_processo)

            try:
                response = self.session.post(self.api_url, json=payload, timeout=60)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.Timeout:
                logger.error(
                    f"Timeout ao buscar página {page_count} de editais.",
                    exc_info=True,
                )
                break
            except requests.exceptions.RequestException as e:
                logger.error(
                    f"Erro HTTP ({e.response.status_code if e.response else 'N/A'}) na pág. {page_count}: {e}"
                )
                if e.response is not None:
                    logger.debug(
                        f"Detalhes do erro da API: {e.response.text}",
                        response_content=e.response.text,
                    )
                break
            except Exception as e:
                logger.error(
                    f"Erro inesperado ao buscar página {page_count} de editais: {e}",
                    exc_info=True,
                )
                break

            editais_in_page = self._parse_editais_from_data(data)

            if not editais_in_page:
                logger.info(
                    f"Nenhum edital retornado na página {page_count}. Fim da paginação."
                )
                break

            all_editais.extend(editais_in_page)

            # Verifica se há mais páginas
            try:
                dsr = data["results"][0]["result"]["data"]["dsr"]
                restart_tokens = dsr.get("RT")
                if restart_tokens:
                    # O último processo será usado como token para a próxima página
                    last_processo = editais_in_page[-1]["numero_processo"] if editais_in_page else None
                    logger.info(
                        f"Página {page_count}: {len(editais_in_page)} editais recebidos. Próxima página com token: {last_processo}"
                    )
                else:
                    logger.info(
                        f"Fim da paginação na página {page_count}: Não há RestartTokens."
                    )
                    break
            except (KeyError, IndexError):
                logger.warning(f"Não foi possível determinar se há mais páginas na página {page_count}")
                break

            if page_count >= self.max_pages:
                logger.warning(
                    f"Atingido o limite de {self.max_pages} páginas. Interrompendo."
                )
                break

        logger.info(
            f"Busca de editais concluída. Total de {len(all_editais)} editais encontrados em {page_count} página(s)."
        )
        return all_editais

    def save_editais(self, editais: List[Dict[str, Any]], out_file: str) -> None:
        """Salva a lista de editais em um arquivo CSV."""
        try:
            os.makedirs(os.path.dirname(out_file), exist_ok=True)

            with open(out_file, "w", newline="", encoding="utf-8-sig") as f:
                if editais:
                    fieldnames = editais[0].keys()
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(editais)
                else:
                    # Escreve cabeçalhos mesmo se não houver dados
                    writer = csv.writer(f)
                    writer.writerow([
                        "numero_processo", "tipo_edital", "data_publicacao",
                        "objeto", "entidade", "valor_estimado", "situacao"
                    ])

            logger.info(f"Lista de editais salva em {out_file}", count=len(editais))

        except IOError as e:
            logger.error(
                f"Erro de I/O ao salvar editais em {out_file}: {e}", exc_info=True
            )
        except Exception as e:
            logger.error(f"Erro inesperado ao salvar editais: {e}", exc_info=True)

    def get_and_save_editais(self, out_file: str) -> List[Dict[str, Any]]:
        """Orquestra busca e salvamento de editais."""
        logger.info(f"Iniciando get_and_save_editais, output para: {out_file}")

        editais = self.fetch_editais()
        if not editais:
            logger.warning(
                "Nenhum edital foi buscado. O arquivo de saída pode ficar vazio."
            )
            self.save_editais([], out_file)
            return []

        self.save_editais(editais, out_file)

        logger.info(
            f"{len(editais)} editais processados e salvos."
        )
        return editais
