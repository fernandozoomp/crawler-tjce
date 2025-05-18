#!/usr/bin/env python3
from typing import Dict
import unicodedata
import re


def slugify(text: str) -> str:
    """
    Converte um texto para slug (ex: 'MUNICÍPIO DE FORTALEZA' -> 'municipio-de-fortaleza')
    """
    # Converte para minúsculas e remove acentos
    text = text.lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join([c for c in text if not unicodedata.combining(c)])

    # Remove caracteres especiais e substitui espaços por hífen
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text).strip("-")

    return text


def unslugify(slug: str) -> str:
    """
    Converte um slug para o formato de exibição (ex: 'municipio-de-fortaleza' -> 'MUNICIPIO DE FORTALEZA')
    """
    # Substitui hífens por espaços e converte para maiúsculas
    return slug.replace("-", " ").upper()


# Mapeamento de slugs para nomes da API do TJCE
ENTITY_MAPPING: Dict[str, str] = {
    "municipio-de-fortaleza": "MUNICÍPIO DE FORTALEZA",
    "municipio-de-caucaia": "MUNICÍPIO DE CAUCAIA",
    "municipio-de-maracanau": "MUNICÍPIO DE MARACANAÚ",
    "municipio-de-juazeiro-do-norte": "MUNICÍPIO DE JUAZEIRO DO NORTE",
    "municipio-de-sobral": "MUNICÍPIO DE SOBRAL",
    # Adicione mais mapeamentos conforme necessário
}

# Mapeamento reverso: da API do TJCE para slug
REVERSE_ENTITY_MAPPING = {v: slugify(v) for v in ENTITY_MAPPING.values()}


def validate_entity_slug(slug: str) -> bool:
    """
    Valida se um slug de entidade é válido

    Args:
        slug: O slug da entidade a ser validado

    Returns:
        bool: True se o slug é válido, False caso contrário
    """
    return slugify(slug) in ENTITY_MAPPING


def get_api_entity_name(slug: str) -> str:
    """
    Converte um slug para o nome esperado pela API do TJCE.
    Ex: 'municipio-de-fortaleza' -> 'MUNICÍPIO DE FORTALEZA'
    """
    # Normaliza o slug recebido
    normalized_slug = slugify(slug)

    # Procura no mapeamento
    if normalized_slug in ENTITY_MAPPING:
        return ENTITY_MAPPING[normalized_slug]

    # Se não encontrar, converte o slug para o formato de exibição
    return unslugify(normalized_slug)


def get_entity_slug(api_name: str) -> str:
    """
    Converte um nome da API do TJCE para slug.
    Ex: 'MUNICÍPIO DE FORTALEZA' -> 'municipio-de-fortaleza'
    """
    # Procura no mapeamento reverso
    if api_name in REVERSE_ENTITY_MAPPING:
        return REVERSE_ENTITY_MAPPING[api_name]

    # Se não encontrar, gera o slug
    return slugify(api_name)


def add_entity_mapping(slug: str, api_name: str) -> None:
    """Adiciona um novo mapeamento de entidade."""
    normalized_slug = slugify(slug)
    ENTITY_MAPPING[normalized_slug] = api_name
    REVERSE_ENTITY_MAPPING[api_name] = normalized_slug
