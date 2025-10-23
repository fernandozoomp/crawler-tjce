import requests
import os
import json
from typing import Optional, Dict
from logger import get_logger
from config import config # Agora config.pinata_api_upload_url estará disponível

logger = get_logger(__name__)

def _direct_upload_to_pinata(
    local_file_path: str,
    file_name_for_pinata: str,
    pinata_jwt: str,
    pinata_api_url: str,
    pinata_metadata: Optional[Dict] = None
) -> Optional[str]:
    """
    Faz upload direto de um arquivo para o Pinata usando o endpoint /pinning/pinFileToIPFS.
    Retorna o CID (IpfsHash) do arquivo em caso de sucesso.
    """
    if not pinata_jwt:
        logger.error("PINATA_API_JWT não configurado para upload direto.")
        return None

    headers = {
        "Authorization": f"Bearer {pinata_jwt}"
    }

    form_data = {}
    if pinata_metadata:
        form_data['pinataMetadata'] = (None, json.dumps(pinata_metadata))
    
    # Adiciona opções de pin, como cidVersion (opcional, mas bom ter)
    pinata_options = {"cidVersion": 1}
    form_data['pinataOptions'] = (None, json.dumps(pinata_options))

    logger.info(
        "Tentando upload direto para Pinata",
        url=pinata_api_url,
        file_name=file_name_for_pinata,
        local_path=local_file_path,
        jwt_present=bool(pinata_jwt)
    )

    try:
        with open(local_file_path, 'rb') as f:
            files_for_upload = {'file': (file_name_for_pinata, f)}
            # Adiciona outros campos do form_data ao files_for_upload para que sejam enviados como multipart
            for key, value_tuple in form_data.items():
                files_for_upload[key] = value_tuple

            response = requests.post(
                pinata_api_url,
                files=files_for_upload,
                headers=headers,
                timeout=120 # Aumentar timeout para uploads maiores
            )
        
        logger.info(
            "Resposta do upload direto do Pinata",
            status_code=response.status_code,
            response_text_preview=response.text[:500]
        )
        response.raise_for_status() # Lança HTTPError para respostas 4xx/5xx
        
        response_data = response.json()
        ipfs_hash = response_data.get("IpfsHash")
        if ipfs_hash:
            logger.info(f"Upload direto para Pinata bem-sucedido. CID: {ipfs_hash}")
            return ipfs_hash
        else:
            logger.error("Resposta do Pinata (upload direto) não continha IpfsHash.", data=response_data)
            return None

    except requests.exceptions.Timeout:
        logger.error(
            "Timeout durante upload direto para Pinata",
            file_name=file_name_for_pinata,
            url=pinata_api_url
        )
        return None
    except requests.exceptions.RequestException as e:
        logger.error(
            "Erro na requisição de upload direto para Pinata",
            error=str(e),
            file_name=file_name_for_pinata
        )
        if e.response is not None:
            logger.error(
                "Detalhes do erro do Pinata (upload direto)", 
                status_code=e.response.status_code, 
                response_text=e.response.text[:500]
            )
        return None
    except Exception as e:
        logger.error(f"Erro inesperado durante upload direto para Pinata: {e}", exc_info=True)
        return None

def construct_pinata_public_url(cid: str, pinata_gateway_url: Optional[str]) -> Optional[str]:
    """Constrói a URL pública do Pinata a partir do CID e da URL do gateway."""
    if not cid:
        return None
    
    # A config.pinata_gateway_url já vem com /ipfs/ no final geralmente
    # mas vamos garantir que a construção seja robusta.
    gateway_base = pinata_gateway_url or "https://gateway.pinata.cloud"
    
    # Remove /ipfs/ ou /ipfs do final se já estiver presente para evitar duplicação
    if gateway_base.endswith("/ipfs/"):
        gateway_base = gateway_base[:-len("/ipfs/")]
    elif gateway_base.endswith("/ipfs"):
        gateway_base = gateway_base[:-len("/ipfs")]
        
    return f"{gateway_base}/ipfs/{cid}"


def upload_and_get_pinata_url(
        local_file_path: str, 
        file_name_for_pinata: str,
        pinata_jwt: str,
        # pinata_gateway_url é pego do config agora
        # pinata_api_upload_url é pego do config agora
        pinata_metadata: Optional[Dict] = None,
        # pinata_group_id não é diretamente usado pelo /pinning/pinFileToIPFS da mesma forma,
        # pode ser parte do metadata se necessário.
    ) -> Optional[str]:
    """
    Orquestra o processo de upload direto para o Pinata e constrói a URL pública.
    """
    logger.info(f"Iniciando processo de upload direto para Pinata: {local_file_path} como {file_name_for_pinata}")

    if not config.pinata_api_upload_url:
        logger.error("URL da API de upload do Pinata não configurada.")
        return None

    # Prepara metadados básicos se não fornecidos
    if pinata_metadata is None:
        pinata_metadata = {"name": file_name_for_pinata}
    elif "name" not in pinata_metadata:
        pinata_metadata["name"] = file_name_for_pinata

    cid = _direct_upload_to_pinata(
        local_file_path=local_file_path,
        file_name_for_pinata=file_name_for_pinata,
        pinata_jwt=pinata_jwt,
        pinata_api_url=config.pinata_api_upload_url,
        pinata_metadata=pinata_metadata
    )

    if not cid:
        logger.error("Falha ao fazer upload direto do arquivo para o Pinata ou obter CID.")
        return None

    public_url = construct_pinata_public_url(cid, config.pinata_gateway_url)
    if public_url:
        logger.info(f"Arquivo enviado com sucesso (upload direto). URL do Pinata: {public_url}")
    else:
        logger.error("Falha ao construir URL pública do Pinata após upload direto.")
    
    return public_url 