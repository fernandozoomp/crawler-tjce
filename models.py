from datetime import datetime, date
from typing import Optional, List, Any, Union, Dict
from pydantic import BaseModel, Field, validator
from decimal import Decimal, InvalidOperation
import logging

# Configuração para serialização JSON não é mais em ConfigDict global para V1
# Será definida dentro de cada modelo na classe Config


class EntityMapping(BaseModel):
    official_name: str
    slug: str

    class Config:
        json_encoders = {
            datetime: lambda dt: dt.isoformat() if dt else None,
            date: lambda d: d.isoformat() if d else None,
            Decimal: lambda dec: str(dec) if dec is not None else None,
        }
        populate_by_name = True
        use_enum_values = True


class Precatorio(BaseModel):
    ordem: int = Field(..., ge=0)
    processo: str = Field(..., min_length=1)
    comarca: str = Field(default="-")
    ano_orcamento: int
    natureza: str = Field(default="-")
    data_cadastro: Optional[datetime] = None
    tipo_classificacao: str = Field(default="-")
    valor_original: Decimal = Field(default=Decimal("0.0"))
    valor_atual: Decimal = Field(default=Decimal("0.0"))
    situacao: str = Field(default="-")

    class Config:
        json_encoders = {
            datetime: lambda dt: dt.isoformat() if dt else None,
            date: lambda d: d.isoformat() if d else None,
            Decimal: lambda dec: str(dec) if dec is not None else None,
        }
        populate_by_name = True
        use_enum_values = True
        # Pydantic V1 usa `orm_mode = True` se você estivesse usando com ORMs e quisesse conversão automática.
        # Para este caso, `populate_by_name` e `json_encoders` são os relevantes.

    @validator("processo", pre=True, always=True)
    @classmethod
    def clean_processo(cls, v: Any) -> str:
        if not v or not isinstance(v, str):
            if isinstance(v, (int, float)):
                return f"{v:.0f}"
            raise ValueError("Processo deve ser uma string ou representação de string")

        processed_v = "".join(c for c in v if c.isalnum() or c == "-" or c == ".")
        if not processed_v.strip():
            raise ValueError("Processo não pode ser vazio após limpeza")
        return processed_v.strip()

    @validator(
        "comarca", "natureza", "tipo_classificacao", "situacao", pre=True, always=True
    )
    @classmethod
    def clean_optional_strings(cls, v: Any) -> str:
        if v is None or (isinstance(v, str) and not v.strip()) or v == "-":
            return "-"
        if not isinstance(v, str):
            return str(v).strip()
        return v.strip()

    @validator("data_cadastro", pre=True, always=True)
    @classmethod
    def clean_data_cadastro(cls, v: Any) -> Optional[datetime]:
        if v is None or (isinstance(v, str) and (v.strip() == "-" or not v.strip())):
            return None

        if isinstance(v, datetime):
            return v
        if isinstance(v, date):
            return datetime.combine(v, datetime.min.time())

        if isinstance(v, str):
            if "datetime" in v.lower():
                try:
                    parts_str = v[
                        v.lower().find("datetime(") + len("datetime("): v.rfind(")")
                    ]
                    parts = [int(p.strip()) for p in parts_str.split(",")]
                    return datetime(*parts)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Falha ao parsear string 'datetime' '{v}': {e}")
                    return None
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            except ValueError:
                pass
            try:
                if v.isdigit():
                    num_v = float(v)
                    if num_v > 253402300799: # Limite para timestamps em segundos que podem ser milissegundos
                        return datetime.fromtimestamp(num_v / 1000.0)
                    return datetime.fromtimestamp(num_v)
            except ValueError:
                pass

        if isinstance(v, (int, float)):
            try:
                if v > 253402300799: # Checagem similar para números
                    return datetime.fromtimestamp(v / 1000.0)
                return datetime.fromtimestamp(v)
            except Exception:
                logger.warning(f"Falha ao converter timestamp numérico para data: {v}")
                return None

        logger.warning(f"Formato de data_cadastro não reconhecido: {v}")
        return None

    @validator("ano_orcamento", pre=True, always=True)
    @classmethod
    def validate_ano_orcamento(cls, v: Any) -> int:
        current_year = datetime.now().year
        default_ano = current_year

        if v is None or (isinstance(v, str) and (v.strip() == "-" or not v.strip())):
            return default_ano

        if isinstance(v, str):
            if not v.isdigit():
                # Tentativa de extrair ano de um timestamp em string (geralmente milissegundos)
                if len(v) > 8 and all(c.isdigit() for c in v): # Heurística para timestamp longo em string
                    try:
                        return datetime.fromtimestamp(int(v) / 1000.0).year
                    except ValueError:
                        return default_ano
                return default_ano
            try:
                v_int = int(v)
            except ValueError:
                return default_ano
        elif isinstance(v, (int, float)):
            v_int = int(v)
        else:
            return default_ano

        # Se o valor for um timestamp muito grande (provavelmente ms), converte para ano
        if v_int > 3000 and len(str(v_int)) > 8: # Heurística: se > 3000 e tem muitos dígitos
            try:
                return datetime.fromtimestamp(v_int / 1000.0).year
            except ValueError:
                pass # Continua para a próxima verificação

        if 1900 <= v_int <= current_year + 5:
            return v_int

        logger.warning(
            f"Ano do orçamento '{v_int}' fora do intervalo, usando default {default_ano}."
        )
        return default_ano

    @validator("valor_original", "valor_atual", pre=True, always=True)
    @classmethod
    def clean_decimal_fields(cls, v: Any) -> Decimal:
        if isinstance(v, Decimal):
            return v

        if v is None or (isinstance(v, str) and (v.strip() == "-" or not v.strip())):
            return Decimal("0.0")

        if isinstance(v, str):
            cleaned_v = v.replace("R$", "").strip()

            num_dots = cleaned_v.count(".")
            num_commas = cleaned_v.count(",")

            if num_commas == 1 and num_dots > 1:  # Formato: 1.234.567,89
                cleaned_v = cleaned_v.replace(".", "").replace(",", ".")
            elif num_commas > 1 and num_dots == 1:  # Formato: 1,234,567.89 (americano)
                cleaned_v = cleaned_v.replace(",", "")
            elif num_commas == 1 and num_dots == 0:  # Formato: 1234,56
                cleaned_v = cleaned_v.replace(",", ".")
            # Casos como '1.234' (milhar sem decimal) ou '1234.56' devem ser tratados com cuidado
            # Se len(parte_após_ponto) == 3 e não há vírgula, pode ser milhar. Ex: 1.234
            elif num_dots == 1 and num_commas == 0 and len(cleaned_v.split(".")[-1]) == 3:
                 # Verifica se o ponto é realmente um separador de milhar e não decimal
                 # Ex: "1.234" -> "1234", mas "123.456" (com decimal de 3 casas) -> "123.456"
                 # Esta lógica pode ser complexa. Uma forma mais simples é remover pontos se eles não forem seguidos por 2 decimais.
                 # Se o último ponto for um separador de milhar e não houver vírgula
                 if cleaned_v.count('.') == 1 and len(cleaned_v.split('.')[-1]) == 3 and not any(char.isdigit() for char in cleaned_v.split('.')[-1][:2]): # ex 1.23X
                     pass # não faz nada, pode ser um decimal com 3 casas
                 elif cleaned_v.count('.') >= 1 and len(cleaned_v.split('.')[-1]) != 2 : #  Trata pontos como separadores de milhar se a parte decimal não for XX
                     cleaned_v = cleaned_v.replace(".", "")

            try:
                return Decimal(cleaned_v)
            except InvalidOperation:
                logger.warning(
                    f"Não foi possível converter valor '{v}' para Decimal após limpeza. Usando 0.0."
                )
                return Decimal("0.0")

        if isinstance(v, (int, float)):
            return Decimal(str(v))  # Converte via string para precisão

        logger.warning(
            f"Tipo inesperado para valor Decimal: {type(v)}, valor: {v}. Usando 0.0."
        )
        return Decimal("0.0")


class PrecatorioResponse(BaseModel):
    status: str
    message: str
    data: Optional[List[Dict[str, Any]]] = None
    pinata_url: Optional[str] = None
    num_precatorios_found: int = 0

    class Config:
        json_encoders = {
            datetime: lambda dt: dt.isoformat() if dt else None,
            date: lambda d: d.isoformat() if d else None,
            Decimal: lambda dec: str(dec) if dec is not None else None,
        }
        populate_by_name = True
        use_enum_values = True


class EntidadeResponse(BaseModel):
    status: str
    message: str
    data: Optional[List[EntityMapping]] = None
    pinata_url: Optional[str] = None

    class Config:
        json_encoders = {
            datetime: lambda dt: dt.isoformat() if dt else None,
            date: lambda d: d.isoformat() if d else None,
            Decimal: lambda dec: str(dec) if dec is not None else None,
        }
        populate_by_name = True
        use_enum_values = True


class HealthCheckResponse(BaseModel):
    status: str
    message: str

    class Config:
        populate_by_name = True
        use_enum_values = True


class FetchPrecatoriosQuery(BaseModel):
    entity: str = Field(
        ...,
        description='Slug da entidade para buscar precatórios. Ex: municipio-de-fortaleza',
        example='municipio-de-fortaleza'
    )
    count: Optional[int] = Field(
        None,
        description='Número de registros a serem retornados. Se não fornecido, busca todos.',
        example=10
    )

    class Config:
        populate_by_name = True
        use_enum_values = True


# Adicionar logger no escopo do módulo se não estiver globalmente disponível
logger = logging.getLogger(__name__)
# Configurar o logger se necessário (ex: logging.basicConfig(level=logging.INFO))
# Se o logger já é configurado em outro lugar (ex: utils.logging_utils), esta parte pode não ser necessária aqui.
