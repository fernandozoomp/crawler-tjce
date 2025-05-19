import requests
import os
import csv
from typing import List, Optional, Dict
from logger import get_logger
from config import config # Para API_URL, HEADERS

logger = get_logger(__name__)

class EntityMappingCrawler:
    def __init__(self):
        self.api_url = config.api_url 
        self.headers = config.headers # Assumindo que config tem os headers necessários
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

    def fetch_entities(self) -> List[str]:
        """Busca a lista de entidades disponíveis."""
        # Este payload é específico para buscar entidades, diferente do payload de precatórios
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
                                            "DataVolume": 3, # Um valor pequeno é suficiente para a lista de entidades
                                            "Primary": {"Window": {}}, # Sem contagem específica, busca todos os grupos
                                        },
                                        "IncludeEmptyGroups": True,
                                        "Version": 1,
                                    },
                                }
                            }
                        ],
                        "QueryId": "", # Pode ser deixado em branco ou um UUID
                    }
                }
            ],
            "cancelQueries": [],
            "modelId": config.model_id if hasattr(config, 'model_id') else 4287487, # Usar model_id da config se disponível
        }

        try:
            logger.info("Buscando lista de entidades da API Power BI.")
            response = self.session.post(self.api_url, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not data.get("results") or not data["results"][0].get("result", {}).get("data"):
                logger.warning("Resposta da API para entidades não contém a estrutura esperada.", response_data=data)
                return []

            result_data = data["results"][0]["result"]["data"]
            dsr = result_data.get("dsr", {})
            if not dsr:
                logger.warning("DS_R não encontrado na resposta de entidades.", dsr_data=dsr)
                return []

            ds_list = dsr.get("DS", [])
            if not ds_list:
                logger.warning("DS não encontrado na resposta de entidades.", ds_list_data=ds_list)
                return []

            entities = []
            # A estrutura para extrair entidades pode ser complexa e variar.
            # O código original em PrecatoriosCrawler tinha uma lógica específica.
            # Adaptando essa lógica:
            for ds_item in ds_list: # Iterar sobre os itens em DS (geralmente um)
                if "PH" not in ds_item: continue
                for ph_item in ds_item["PH"]: # Iterar sobre Page Headers
                    if "DM0" not in ph_item: continue
                    for dm0_item in ph_item["DM0"]: # Iterar sobre Data Segments
                        # Em 'DM0', os valores podem estar diretamente sob chaves como 'G0', 'C0', etc.
                        # ou dentro de um array 'C'. O payload acima projeta um único campo.
                        # O 'Select' resultará em um valor, provavelmente em C[0] ou G0 dependendo do binding.
                        # Se o binding tem Groupings com Projections: [0], o valor estará em G0 (ou o que for mapeado para projeção 0)
                        entity_value = dm0_item.get("G0") # Assumindo que a projeção [0] mapeia para G0
                        if entity_value is None and "C" in dm0_item and dm0_item["C"]:
                             entity_value = dm0_item["C"][0] # Fallback para C[0] se G0 não estiver presente

                        if entity_value:
                            entity_str = str(entity_value).strip()
                            if entity_str and entity_str != "--- Selecione a Entidade":
                                decoded_entity = self._decode_utf8(entity_str.strip("'"))
                                if decoded_entity not in entities:
                                    entities.append(decoded_entity)
            
            entities = sorted(list(set(entities))) # Garantir unicidade e ordenação
            logger.info("Entidades encontradas e decodificadas", count=len(entities), entities_sample=entities[:5])
            return entities

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na requisição HTTP ao buscar entidades: {e}", exc_info=True)
            if e.response is not None:
                logger.error(f"Detalhes do erro do Pinata: {e.response.status_code} - {e.response.text}")
            return []
        except Exception as e:
            logger.error(f"Erro inesperado ao buscar entidades: {e}", exc_info=True)
            return []

    def save_entities(self, entities: List[str], out_file: str) -> None:
        """Salva a lista de entidades em um arquivo CSV."""
        try:
            # Garante que o diretório de saída exista
            os.makedirs(os.path.dirname(out_file), exist_ok=True)
            
            mode = "w" # Sempre sobrescreve o arquivo de entidades com a lista mais recente
            write_header = True

            with open(out_file, mode, newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(["entidade"]) # Nome da coluna para o CSV de entidades
                for entity in entities:
                    writer.writerow([entity])
            logger.info(f"Lista de entidades salva em {out_file}", count=len(entities))

        except IOError as e:
            logger.error(f"Erro de I/O ao salvar entidades em {out_file}: {e}", exc_info=True)
            # Não relança o erro para não quebrar o fluxo principal se o salvamento falhar,
            # mas o log registrará o problema.
        except Exception as e:
            logger.error(f"Erro inesperado ao salvar entidades: {e}", exc_info=True)

    def get_and_save_entities(self, out_file: str) -> List[Dict[str,str]]:
        """Orquestra busca e salvamento de entidades, e retorna o mapeamento com slugs."""
        logger.info(f"Iniciando get_and_save_entities, output para: {out_file}")
        # Esta importação é local para evitar dependência circular se entity_mapping.py importar algo deste módulo
        from entity_mapping import get_entity_slug 

        official_entities = self.fetch_entities()
        if not official_entities:
            logger.warning("Nenhuma entidade oficial foi buscada. O arquivo de saída pode ficar vazio ou não ser criado.")
            # Tenta salvar um arquivo vazio com cabeçalho mesmo assim
            self.save_entities([], out_file)
            return []
        
        self.save_entities(official_entities, out_file)
        
        # Gera os slugs e retorna a lista de mapeamentos para a API
        entity_mappings_for_api = []
        for official_name in official_entities:
            slug = get_entity_slug(official_name) # Função de entity_mapping.py
            entity_mappings_for_api.append({"official_name": official_name, "slug": slug})
            
        logger.info(f"{len(entity_mappings_for_api)} mapeamentos de entidade gerados para resposta da API.")
        return entity_mappings_for_api 