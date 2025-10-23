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
    "autarquia-de-transito-e-transporte-rodoviario-e-urbano-do-municipio-de-quixeramobim": "AUTARQUIA DE TRANSITO E TRANSPORTE RODOVIARIO E URBANO DO MUNICIPIO DE QUIXERAMOBIM",
    "casa-de-saude-adilia-maria": "CASA DE SAUDE ADILIA MARIA",
    "companhia-brasileira-de-trens-urbanos-cbtu": "COMPANHIA BRASILEIRA DE TRENS URBANOS - CBTU",
    "companhia-docas-do-ceara": "COMPANHIA DOCAS DO CEARA",
    "consorcio-de-desenvolvimento-da-regiao-do-sertao-central-sul-codessul": "CONSORCIO DE DESENVOLVIMENTO DA REGIAO DO SERTAO CENTRAL SUL - CODESSUL",
    "dmutran-departamento-municipal-de-transito-e-rodoviario-de-morada-nova": "DMUTRAN - DEPARTAMENTO MUNICIPAL DE TRANSITO E RODOVIARIO DE MORADA NOVA",
    "estado-de-sao-paulo": "ESTADO DE SÃO PAULO",
    "estado-do-ceara": "ESTADO DO CEARÁ",
    "estado-do-pernambuco": "ESTADO DO PERNAMBUCO",
    "fundacao-de-saude-publica-do-municipio-de-iguatu": "FUNDACAO DE SAUDE PUBLICA DO MUNICIPIO DE IGUATU",
    "fundacao-universidade-do-amazonas": "FUNDAÇÃO UNIVERSIDADE DO AMAZONAS",
    "fundo-municipal-de-previdencia-social-de-palmacia": "FUNDO MUNICIPAL DE PREVIDENCIA SOCIAL DE PALMACIA",
    "fundo-municipal-de-previdencia-social-dos-servidores-de-juazeiro-nortece-previjuno": "FUNDO MUNICIPAL DE PREVIDENCIA SOCIAL DOS SERVIDORES DE JUAZEIRO NORTE/CE-PREVIJUNO",
    "instituto-de-previdencia-do-municipio-de-acopiara": "INSTITUTO DE PREVIDENCIA DO MUNICIPIO DE ACOPIARA",
    "instituto-de-previdencia-do-municipio-de-boa-viagem-ipmbv": "INSTITUTO DE PREVIDENCIA DO MUNICIPIO DE BOA VIAGEM - IPMBV",
    "instituto-de-previdencia-do-municipio-de-caucaia-ipmc": "INSTITUTO DE PREVIDENCIA DO MUNICIPIO DE CAUCAIA - IPMC",
    "instituto-de-previdencia-do-municipio-de-maracanau-ipm-maracanau": "INSTITUTO DE PREVIDENCIA DO MUNICIPIO DE MARACANAU - IPM MARACANAU",
    "instituto-de-previdencia-do-municipio-de-taua-ipmt": "INSTITUTO DE PREVIDENCIA DO MUNICIPIO DE TAUA - IPMT",
    "instituto-de-previdencia-dos-servidores-municipais-de-itapipoca-itaprev": "INSTITUTO DE PREVIDENCIA DOS SERVIDORES MUNICIPAIS DE ITAPIPOCA - ITAPREV",
    "instituto-de-previdencia-dos-servidores-municipais-de-morada-nova-ipremn": "INSTITUTO DE PREVIDENCIA DOS SERVIDORES MUNICIPAIS DE MORADA NOVA - IPREMN",
    "instituto-de-previdencia-dos-servidores-publicos-municipais-de-ibicuitinga": "INSTITUTO DE PREVIDENCIA DOS SERVIDORES PUBLICOS MUNICIPAIS DE IBICUITINGA",
    "instituto-nacional-do-seguro-social-inss": "INSTITUTO NACIONAL DO SEGURO SOCIAL - INSS",
    "municipio-de-boquira-ba": "MUNICIPIO DE BOQUIRA - BA",
    "municipio-de-abaiara": "MUNICÍPIO DE ABAIARA",
    "municipio-de-acarape": "MUNICÍPIO DE ACARAPE",
    "municipio-de-acarau": "MUNICÍPIO DE ACARAU",
    "municipio-de-acopiara": "MUNICÍPIO DE ACOPIARA",
    "municipio-de-aiuaba": "MUNICÍPIO DE AIUABA",
    "municipio-de-alcantaras": "MUNICÍPIO DE ALCANTARAS",
    "municipio-de-altaneira": "MUNICÍPIO DE ALTANEIRA",
    "municipio-de-alto-santo": "MUNICÍPIO DE ALTO SANTO",
    "municipio-de-amontada": "MUNICÍPIO DE AMONTADA",
    "municipio-de-antonina-do-norte": "MUNICÍPIO DE ANTONINA DO NORTE",
    "municipio-de-apuiares": "MUNICÍPIO DE APUIARES",
    "municipio-de-aquiraz": "MUNICÍPIO DE AQUIRAZ",
    "municipio-de-aracati": "MUNICÍPIO DE ARACATI",
    "municipio-de-aracoiaba": "MUNICÍPIO DE ARACOIABA",
    "municipio-de-ararenda": "MUNICÍPIO DE ARARENDA",
    "municipio-de-araripe": "MUNICÍPIO DE ARARIPE",
    "municipio-de-aratuba": "MUNICÍPIO DE ARATUBA",
    "municipio-de-arneiroz": "MUNICÍPIO DE ARNEIROZ",
    "municipio-de-assare": "MUNICÍPIO DE ASSARE",
    "municipio-de-aurora": "MUNICÍPIO DE AURORA",
    "municipio-de-baixio": "MUNICÍPIO DE BAIXIO",
    "municipio-de-banabuiu": "MUNICÍPIO DE BANABUIU",
    "municipio-de-barbalha": "MUNICÍPIO DE BARBALHA",
    "municipio-de-barreira": "MUNICÍPIO DE BARREIRA",
    "municipio-de-barro": "MUNICÍPIO DE BARRO",
    "municipio-de-barroquinha": "MUNICÍPIO DE BARROQUINHA",
    "municipio-de-baturite": "MUNICÍPIO DE BATURITE",
    "municipio-de-beberibe": "MUNICÍPIO DE BEBERIBE",
    "municipio-de-bela-cruz": "MUNICÍPIO DE BELA CRUZ",
    "municipio-de-boa-viagem": "MUNICÍPIO DE BOA VIAGEM",
    "municipio-de-brejo-santo": "MUNICÍPIO DE BREJO SANTO",
    "municipio-de-camocim": "MUNICÍPIO DE CAMOCIM",
    "municipio-de-campos-sales": "MUNICÍPIO DE CAMPOS SALES",
    "municipio-de-caninde": "MUNICÍPIO DE CANINDE",
    "municipio-de-capistrano": "MUNICÍPIO DE CAPISTRANO",
    "municipio-de-caridade": "MUNICÍPIO DE CARIDADE",
    "municipio-de-carire": "MUNICÍPIO DE CARIRE",
    "municipio-de-caririacu": "MUNICÍPIO DE CARIRIAÇU",
    "municipio-de-carius": "MUNICÍPIO DE CARIUS",
    "municipio-de-carnaubal": "MUNICÍPIO DE CARNAUBAL",
    "municipio-de-cascavel": "MUNICÍPIO DE CASCAVEL",
    "municipio-de-catarina": "MUNICÍPIO DE CATARINA",
    "municipio-de-catunda": "MUNICÍPIO DE CATUNDA",
    "municipio-de-caucaia": "MUNICÍPIO DE CAUCAIA",
    "municipio-de-cedro": "MUNICÍPIO DE CEDRO",
    "municipio-de-chaval": "MUNICÍPIO DE CHAVAL",
    "municipio-de-choro": "MUNICÍPIO DE CHORO",
    "municipio-de-chorozinho": "MUNICÍPIO DE CHOROZINHO",
    "municipio-de-coreau": "MUNICÍPIO DE COREAU",
    "municipio-de-crateus": "MUNICÍPIO DE CRATEUS",
    "municipio-de-crato": "MUNICÍPIO DE CRATO",
    "municipio-de-croata": "MUNICÍPIO DE CROATA",
    "municipio-de-cruz": "MUNICÍPIO DE CRUZ",
    "municipio-de-deputado-irapuan-pinheiro": "MUNICÍPIO DE DEPUTADO IRAPUAN PINHEIRO",
    "municipio-de-erere": "MUNICÍPIO DE ERERE",
    "municipio-de-eusebio": "MUNICÍPIO DE EUSEBIO",
    "municipio-de-farias-brito": "MUNICÍPIO DE FARIAS BRITO",
    "municipio-de-forquilha": "MUNICÍPIO DE FORQUILHA",
    "municipio-de-fortaleza": "MUNICÍPIO DE FORTALEZA",
    "municipio-de-fortim": "MUNICÍPIO DE FORTIM",
    "municipio-de-frecheirinha": "MUNICÍPIO DE FRECHEIRINHA",
    "municipio-de-general-sampaio": "MUNICÍPIO DE GENERAL SAMPAIO",
    "municipio-de-granja": "MUNICÍPIO DE GRANJA",
    "municipio-de-granjeiro": "MUNICÍPIO DE GRANJEIRO",
    "municipio-de-graca": "MUNICÍPIO DE GRAÇA",
    "municipio-de-groairas": "MUNICÍPIO DE GROAIRAS",
    "municipio-de-guaiuba": "MUNICÍPIO DE GUAIUBA",
    "municipio-de-guaraciaba-do-norte": "MUNICÍPIO DE GUARACIABA DO NORTE",
    "municipio-de-guaramiranga": "MUNICÍPIO DE GUARAMIRANGA",
    "municipio-de-hidrolandia": "MUNICÍPIO DE HIDROLANDIA",
    "municipio-de-horizonte": "MUNICÍPIO DE HORIZONTE",
    "municipio-de-ibaretama": "MUNICÍPIO DE IBARETAMA",
    "municipio-de-ibiapina": "MUNICÍPIO DE IBIAPINA",
    "municipio-de-ibicuitinga": "MUNICÍPIO DE IBICUITINGA",
    "municipio-de-icapui": "MUNICÍPIO DE ICAPUI",
    "municipio-de-ico": "MUNICÍPIO DE ICO",
    "municipio-de-iguatu": "MUNICÍPIO DE IGUATU",
    "municipio-de-independencia": "MUNICÍPIO DE INDEPENDENCIA",
    "municipio-de-ipaporanga": "MUNICÍPIO DE IPAPORANGA",
    "municipio-de-ipaumirim": "MUNICÍPIO DE IPAUMIRIM",
    "municipio-de-ipu": "MUNICÍPIO DE IPU",
    "municipio-de-ipueiras": "MUNICÍPIO DE IPUEIRAS",
    "municipio-de-iracema": "MUNICÍPIO DE IRACEMA",
    "municipio-de-iraucuba": "MUNICÍPIO DE IRAUÇUBA",
    "municipio-de-itaicaba": "MUNICÍPIO DE ITAIÇABA",
    "municipio-de-itaitinga": "MUNICÍPIO DE ITAITINGA",
    "municipio-de-itapaje": "MUNICÍPIO DE ITAPAJÉ",
    "municipio-de-itapipoca": "MUNICÍPIO DE ITAPIPOCA",
    "municipio-de-itapiuna": "MUNICÍPIO DE ITAPIÚNA",
    "municipio-de-itarema": "MUNICÍPIO DE ITAREMA",
    "municipio-de-itatira": "MUNICÍPIO DE ITATIRA",
    "municipio-de-jaguaretama": "MUNICÍPIO DE JAGUARETAMA",
    "municipio-de-jaguaribara": "MUNICÍPIO DE JAGUARIBARA",
    "municipio-de-jaguaribe": "MUNICÍPIO DE JAGUARIBE",
    "municipio-de-jaguaruana": "MUNICÍPIO DE JAGUARUANA",
    "municipio-de-jardim": "MUNICÍPIO DE JARDIM",
    "municipio-de-jati": "MUNICÍPIO DE JATI",
    "municipio-de-jijoca-de-jericoacoara": "MUNICÍPIO DE JIJOCA DE JERICOACOARA",
    "municipio-de-juazeiro-do-norte": "MUNICÍPIO DE JUAZEIRO DO NORTE",
    "municipio-de-jucas": "MUNICÍPIO DE JUCÁS",
    "municipio-de-lavras-da-mangabeira": "MUNICÍPIO DE LAVRAS DA MANGABEIRA",
    "municipio-de-limoeiro-do-norte": "MUNICÍPIO DE LIMOEIRO DO NORTE",
    "municipio-de-madalena": "MUNICÍPIO DE MADALENA",
    "municipio-de-maracanau": "MUNICÍPIO DE MARACANAÚ",
    "municipio-de-marco": "MUNICÍPIO DE MARCO",
    "municipio-de-martinopole": "MUNICÍPIO DE MARTINÓPOLE",
    "municipio-de-massape": "MUNICÍPIO DE MASSAPÊ",
    "municipio-de-mauriti": "MUNICÍPIO DE MAURITI",
    "municipio-de-meruoca": "MUNICÍPIO DE MERUOCA",
    "municipio-de-milagres": "MUNICÍPIO DE MILAGRES",
    "municipio-de-milha": "MUNICÍPIO DE MILHÃ",
    "municipio-de-miraima": "MUNICÍPIO DE MIRAÍMA",
    "municipio-de-missao-velha": "MUNICÍPIO DE MISSÃO VELHA",
    "municipio-de-mombaca": "MUNICÍPIO DE MOMBAÇA",
    "municipio-de-monsenhor-tabosa": "MUNICÍPIO DE MONSENHOR TABOSA",
    "municipio-de-morada-nova": "MUNICÍPIO DE MORADA NOVA",
    "municipio-de-moraujo": "MUNICÍPIO DE MORAÚJO",
    "municipio-de-morrinhos": "MUNICÍPIO DE MORRINHOS",
    "municipio-de-mucambo": "MUNICÍPIO DE MUCAMBO",
    "municipio-de-mulungu": "MUNICÍPIO DE MULUNGU",
    "municipio-de-nova-olinda": "MUNICÍPIO DE NOVA OLINDA",
    "municipio-de-nova-russas": "MUNICÍPIO DE NOVA RUSSAS",
    "municipio-de-novo-oriente": "MUNICÍPIO DE NOVO ORIENTE",
    "municipio-de-ocara": "MUNICÍPIO DE OCARA",
    "municipio-de-oros": "MUNICÍPIO DE ORÓS",
    "municipio-de-pacajus": "MUNICÍPIO DE PACAJUS",
    "municipio-de-pacatuba": "MUNICÍPIO DE PACATUBA",
    "municipio-de-pacoti": "MUNICÍPIO DE PACOTI",
    "municipio-de-pacuja": "MUNICÍPIO DE PACUJÁ",
    "municipio-de-palhano": "MUNICÍPIO DE PALHANO",
    "municipio-de-palmacia": "MUNICÍPIO DE PALMÁCIA",
    "municipio-de-paracuru": "MUNICÍPIO DE PARACURU",
    "municipio-de-paraipaba": "MUNICÍPIO DE PARAIPABA",
    "municipio-de-parambu": "MUNICÍPIO DE PARAMBU",
    "municipio-de-paramoti": "MUNICÍPIO DE PARAMOTI",
    "municipio-de-pedra-branca": "MUNICÍPIO DE PEDRA BRANCA",
    "municipio-de-penaforte": "MUNICÍPIO DE PENAFORTE",
    "municipio-de-pentecoste": "MUNICÍPIO DE PENTECOSTE",
    "municipio-de-pereiro": "MUNICÍPIO DE PEREIRO",
    "municipio-de-pindoretama": "MUNICÍPIO DE PINDORETAMA",
    "municipio-de-piquet-carneiro": "MUNICÍPIO DE PIQUET CARNEIRO",
    "municipio-de-pires-ferreira": "MUNICÍPIO DE PIRES FERREIRA",
    "municipio-de-poranga": "MUNICÍPIO DE PORANGA",
    "municipio-de-porteiras": "MUNICÍPIO DE PORTEIRAS",
    "municipio-de-potengi": "MUNICÍPIO DE POTENGI",
    "municipio-de-potiretama": "MUNICÍPIO DE POTIRETAMA",
    "municipio-de-quiterianopolis": "MUNICÍPIO DE QUITERIANÓPOLIS",
    "municipio-de-quixada": "MUNICÍPIO DE QUIXADÁ",
    "municipio-de-quixelo": "MUNICÍPIO DE QUIXELÔ",
    "municipio-de-quixeramobim": "MUNICÍPIO DE QUIXERAMOBIM",
    "municipio-de-quixere": "MUNICÍPIO DE QUIXERÉ",
    "municipio-de-redencao": "MUNICÍPIO DE REDENÇÃO",
    "municipio-de-reriutaba": "MUNICÍPIO DE RERIUTABA",
    "municipio-de-russas": "MUNICÍPIO DE RUSSAS",
    "municipio-de-salitre": "MUNICÍPIO DE SALITRE",
    "municipio-de-santa-quiteria": "MUNICÍPIO DE SANTA QUITÉRIA",
    "municipio-de-santana-do-acarau": "MUNICÍPIO DE SANTANA DO ACARAÚ",
    "municipio-de-santana-do-cariri": "MUNICÍPIO DE SANTANA DO CARIRI",
    "municipio-de-sao-benedito": "MUNICÍPIO DE SÃO BENEDITO",
    "municipio-de-sao-goncalo-do-amarante": "MUNICÍPIO DE SÃO GONÇALO DO AMARANTE",
    "municipio-de-sao-joao-do-jaguaribe": "MUNICÍPIO DE SÃO JOÃO DO JAGUARIBE",
    "municipio-de-sao-luis-do-curu": "MUNICÍPIO DE SÃO LUÍS DO CURU",
    "municipio-de-senador-pompeu": "MUNICÍPIO DE SENADOR POMPEU",
    "municipio-de-senador-sa": "MUNICÍPIO DE SENADOR SÁ",
    "municipio-de-sobral": "MUNICÍPIO DE SOBRAL",
    "municipio-de-solonopole": "MUNICÍPIO DE SOLONÓPOLE",
    "municipio-de-tabuleiro-do-norte": "MUNICÍPIO DE TABULEIRO DO NORTE",
    "municipio-de-tamboril": "MUNICÍPIO DE TAMBORIL",
    "municipio-de-tarrafas": "MUNICÍPIO DE TARRAFAS",
    "municipio-de-taua": "MUNICÍPIO DE TAUÁ",
    "municipio-de-tejucuoca": "MUNICÍPIO DE TEJUÇUOCA",
    "municipio-de-tiangua": "MUNICÍPIO DE TIANGUÁ",
    "municipio-de-trairi": "MUNICÍPIO DE TRAIRI",
    "municipio-de-tururu": "MUNICÍPIO DE TURURU",
    "municipio-de-ubajara": "MUNICÍPIO DE UBAJARA",
    "municipio-de-umari": "MUNICÍPIO DE UMARI",
    "municipio-de-umirim": "MUNICÍPIO DE UMIRIM",
    "municipio-de-uruburetama": "MUNICÍPIO DE URUBURETAMA",
    "municipio-de-uruoca": "MUNICÍPIO DE URUOCA",
    "municipio-de-varjota": "MUNICÍPIO DE VARJOTA",
    "municipio-de-varzea-alegre": "MUNICÍPIO DE VÁRZEA ALEGRE",
    "municipio-de-vicosa-do-ceara": "MUNICÍPIO DE VIÇOSA DO CEARÁ",
    # Adicione mais mapeamentos conforme necessário
}

# Mapeamento reverso: da API do TJCE para slug
REVERSE_ENTITY_MAPPING = {v: slugify(v) for k, v in ENTITY_MAPPING.items()}


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
    # Esta parte pode não ser ideal se a API do TJCE for sensível a maiúsculas/minúsculas
    # ou acentos de forma diferente do unslugify.
    # O ideal é que TODOS os nomes da API estejam no ENTITY_MAPPING.
    # Considerar logar um aviso se cair aqui.
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
