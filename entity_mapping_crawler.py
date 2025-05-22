import requests
import os
import csv
from typing import List, Optional, Dict, Any
from logger import get_logger
from config import config  # Para API_URL, HEADERS

logger = get_logger(__name__)


class EntityMappingCrawler:
    def __init__(self):
        self.api_url = config.api_url
        self.headers = config.headers  # Assumindo que config tem os headers necessários
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        # Definindo um tamanho de página padrão. Pode ser ajustado ou movido para config.
        self.page_size = getattr(config, "entity_page_size", 500)
        self.max_pages = getattr(config, "max_entity_pages", 100)

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

    def _build_entity_payload(
        self, last_entity_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Constrói o payload para a requisição de entidades, com suporte à paginação."""
        query_binding: Dict[str, Any] = {
            "Primary": {"Groupings": [{"Projections": [0]}]},
            "DataReduction": {
                "DataVolume": 3,  # Este DataVolume não parece controlar a contagem de itens diretamente para entidades.
                # A paginação é feita via RestartTokens.
                "Primary": {
                    "Window": {}  # Inicialmente vazio, será preenchido com RestartTokens ou Count
                },
            },
            "IncludeEmptyGroups": True,
            "Version": 1,
        }

        # Adiciona RestartTokens se last_entity_name for fornecido
        if last_entity_name:
            # O token precisa estar entre aspas simples e duplas, como no exemplo: [["'NOME DA ENTIDADE'"]]
            formatted_token = f"'{last_entity_name}'"
            query_binding["DataReduction"]["Primary"]["Window"]["RestartTokens"] = [
                [formatted_token]
            ]
            logger.debug(f"Construindo payload com RestartToken: {formatted_token}")
        # else:
        # Para a primeira página ou se não houver RestartToken, podemos tentar controlar com Count,
        # mas a API pode ignorar isso para a listagem de entidades, priorizando o RestartTokens.
        # query_binding["DataReduction"]["Primary"]["Window"]["Count"] = self.page_size
        # logger.debug(f"Construindo payload com Count: {self.page_size} (primeira página ou sem RestartToken)")

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
                                        # Adicionar Ordenação é crucial para paginação estável
                                        "OrderBy": [
                                            {
                                                "Direction": 1,  # 1 para Ascendente, 2 para Descendente
                                                "Expression": {
                                                    "Column": {
                                                        "Expression": {
                                                            "SourceRef": {"Source": "d"}
                                                        },
                                                        "Property": "dfslcp_dsc_entidade",
                                                    }
                                                },
                                            }
                                        ],
                                    },
                                    "Binding": query_binding,
                                }
                            }
                        ],
                        "QueryId": "",  # Pode ser deixado em branco ou um UUID
                    }
                }
            ],
            "cancelQueries": [],
            "modelId": getattr(config, "model_id", 4287487),
        }
        return payload

    def _parse_entities_from_data(self, data: Dict[str, Any]) -> List[str]:
        """Extrai nomes de entidades da resposta JSON da API."""
        entities_in_page: List[str] = []
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
                        entity_value = dm0_item.get("G0")
                        if entity_value is None and "C" in dm0_item and dm0_item["C"]:
                            entity_value = dm0_item["C"][0]

                        if entity_value:
                            entity_str = str(entity_value).strip()
                            if (
                                entity_str
                                and entity_str.lower() != "--- selecione a entidade"
                            ):  # Ignora o placeholder
                                decoded_entity = self._decode_utf8(
                                    entity_str.strip("'")
                                )
                                entities_in_page.append(decoded_entity)
        except (IndexError, KeyError, TypeError) as e:
            logger.warning(
                f"Estrutura inesperada nos dados da API ao parsear entidades: {e}",
                exc_info=True,
                response_data=data,
            )
            return []  # Retorna lista vazia se a estrutura não for a esperada

        return entities_in_page

    def fetch_entities(self) -> List[str]:
        """Busca a lista de todas as entidades disponíveis, lidando com paginação."""
        all_entities_set = set()
        last_entity_for_token: Optional[str] = None
        page_count = 0

        while True:
            page_count += 1
            token_info = (
                f"'{last_entity_for_token}'"
                if last_entity_for_token
                else "Nenhum (primeira página)"
            )
            logger.info(
                f"Buscando página {page_count} de entidades. Token: {token_info}."
            )

            payload = self._build_entity_payload(last_entity_for_token)

            try:
                response = self.session.post(self.api_url, json=payload, timeout=60)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.Timeout:
                logger.error(
                    f"Timeout ao buscar página {page_count} de entidades.",
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
                    f"Erro inesperado ao buscar página {page_count} de entidades: {e}",
                    exc_info=True,
                )
                break

            entities_in_page = self._parse_entities_from_data(data)

            if not entities_in_page:
                logger.info(
                    f"Nenhuma entidade retornada na página {page_count}. Fim da paginação ou erro de parsing."
                )
                break

            new_entities_this_page = 0
            current_last_entity_in_page = None

            for entity in entities_in_page:
                if entity not in all_entities_set:
                    all_entities_set.add(entity)
                    new_entities_this_page += 1
                current_last_entity_in_page = entity

            logger.info(
                f"Página {page_count}: {len(entities_in_page)} recebidas, "
                f"{new_entities_this_page} novas. Total único: {len(all_entities_set)}."
            )

            # Condições de parada
            # 1. A API retorna apenas a entidade usada como token (indicando que é o último item)
            if (
                len(entities_in_page) == 1
                and last_entity_for_token == entities_in_page[0]
                and page_count > 1
            ):
                logger.info(
                    f"Página {page_count} retornou apenas o token '{last_entity_for_token}'. Fim."
                )
                break

            # 2. Nenhuma nova entidade adicionada nesta página E a página não parece cheia (após a primeira)
            #    Isso sugere que não há mais dados.
            if (
                new_entities_this_page == 0
                and len(entities_in_page) < self.page_size
                and page_count > 1
            ):
                logger.info(
                    f"Nenhuma nova entidade na pág. {page_count} e página não cheia. Provável fim."
                )
                break

            # 3. O último item processado é o mesmo que o token usado, indicando que não há mais itens.
            #    (Considerar se `new_entities_this_page == 0` não cobre isso melhor)
            if current_last_entity_in_page == last_entity_for_token and page_count > 1:
                logger.info(
                    f"Último item da pág. {page_count} ('{current_last_entity_in_page}') é o mesmo "
                    f"usado para o token. Fim."
                )
                break

            last_entity_for_token = current_last_entity_in_page

            if not last_entity_for_token:
                logger.warning(
                    f"Não foi possível determinar o token para a próxima página ({page_count+1}). Interrompendo."
                )
                break

            if page_count >= self.max_pages:
                logger.warning(
                    f"Atingido o limite de {self.max_pages} páginas. Interrompendo."
                )
                break

        final_entities_list = sorted(list(all_entities_set))
        logger.info(
            f"Busca de entidades concluída. Total de {len(final_entities_list)} entidades únicas "
            f"encontradas após {page_count} página(s)."
        )
        return final_entities_list

    def save_entities(self, entities: List[str], out_file: str) -> None:
        """Salva a lista de entidades em um arquivo CSV."""
        try:
            # Garante que o diretório de saída exista
            os.makedirs(os.path.dirname(out_file), exist_ok=True)

            mode = "w"  # Sempre sobrescreve o arquivo de entidades com a lista mais recente
            write_header = True

            with open(out_file, mode, newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(
                        ["entidade"]
                    )  # Nome da coluna para o CSV de entidades
                for entity in entities:
                    writer.writerow([entity])
            logger.info(f"Lista de entidades salva em {out_file}", count=len(entities))

        except IOError as e:
            logger.error(
                f"Erro de I/O ao salvar entidades em {out_file}: {e}", exc_info=True
            )
            # Não relança o erro para não quebrar o fluxo principal se o salvamento falhar,
            # mas o log registrará o problema.
        except Exception as e:
            logger.error(f"Erro inesperado ao salvar entidades: {e}", exc_info=True)

    def get_and_save_entities(self, out_file: str) -> List[Dict[str, str]]:
        """Orquestra busca e salvamento de entidades, e retorna o mapeamento com slugs."""
        logger.info(f"Iniciando get_and_save_entities, output para: {out_file}")
        # Esta importação é local para evitar dependência circular se entity_mapping.py importar algo deste módulo
        from entity_mapping import get_entity_slug

        official_entities = self.fetch_entities()
        if not official_entities:
            logger.warning(
                "Nenhuma entidade oficial foi buscada. O arquivo de saída pode ficar vazio ou não ser criado."
            )
            # Tenta salvar um arquivo vazio com cabeçalho mesmo assim
            self.save_entities([], out_file)
            return []

        self.save_entities(official_entities, out_file)

        # Gera os slugs e retorna a lista de mapeamentos para a API
        entity_mappings_for_api = []
        for official_name in official_entities:
            slug = get_entity_slug(official_name)  # Função de entity_mapping.py
            entity_mappings_for_api.append(
                {"official_name": official_name, "slug": slug}
            )

        logger.info(
            f"{len(entity_mappings_for_api)} mapeamentos de entidade gerados para resposta da API."
        )
        return entity_mappings_for_api
