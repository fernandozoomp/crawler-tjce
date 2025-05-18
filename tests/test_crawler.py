import pytest
from datetime import datetime
from decimal import Decimal
from unittest.mock import Mock, patch

from crawler import PrecatoriosCrawler
from models import Precatorio


@pytest.fixture
def crawler():
    return PrecatoriosCrawler()


def test_format_currency():
    """Testa a formatação de valores monetários"""
    crawler = PrecatoriosCrawler()
    assert crawler._format_value("1234.56", "currency") == "R$ 1.234,56"
    assert crawler._format_value("invalid", "currency") == "R$ invalid"
    assert crawler._format_value(1234.56, "currency") == "R$ 1.234,56"
    assert crawler._format_value(None, "currency") == "-"


def test_format_date():
    """Testa a formatação de datas"""
    crawler = PrecatoriosCrawler()
    # Timestamp em milissegundos
    assert crawler._format_value(1645564800000, "date") == "23/02/2022"
    assert crawler._format_value("2022-02-23", "date") == "2022-02-23"
    assert crawler._format_value(None, "date") == "-"


def test_format_int():
    """Testa a formatação de inteiros"""
    crawler = PrecatoriosCrawler()
    assert crawler._format_value("123.45", "int") == "123"
    assert crawler._format_value("123L", "int") == "123"
    assert crawler._format_value(123, "int") == "123"
    assert crawler._format_value(None, "int") == "-"


def test_get_value_from_dict():
    """Testa a extração de valores do dicionário"""
    crawler = PrecatoriosCrawler()
    value_dicts = [
        {"G": 0, "N": "R0", "V": "valor1"},
        {"G": 1, "N": "R1", "V": "valor2"},
    ]

    assert crawler._get_value_from_dict(value_dicts, "R0", 0) == "valor1"
    assert crawler._get_value_from_dict(value_dicts, "R1", 1) == "valor2"
    assert crawler._get_value_from_dict(value_dicts, "R2", 2) == ""
    assert crawler._get_value_from_dict([], "R0", 0) == ""
    assert crawler._get_value_from_dict(None, "R0", 0) == ""


@pytest.mark.asyncio
async def test_fetch_page():
    """Testa a busca de uma página da API"""
    with patch("requests.Session") as mock_session:
        mock_response = Mock()
        mock_response.json.return_value = {"results": [{"result": {"data": {}}}]}
        mock_session.return_value.post.return_value = mock_response

        crawler = PrecatoriosCrawler()
        result = crawler._fetch_page("TESTE")

        assert result == {"results": [{"result": {"data": {}}}]}
        mock_session.return_value.post.assert_called_once()


def test_precatorio_model():
    """Testa o modelo Pydantic de Precatório"""
    data = {
        "ordem": 1,
        "processo": "123456",
        "comarca": "FORTALEZA",
        "ano_orcamento": 2022,
        "natureza": "ALIMENTAR",
        "data_cadastro": datetime.now(),
        "tipo_classificacao": "NORMAL",
        "valor_original": Decimal("1000.00"),
        "valor_atual": Decimal("1100.00"),
        "situacao": "ATIVO",
    }

    precatorio = Precatorio(**data)
    assert precatorio.ordem == 1
    assert precatorio.processo == "123456"
    assert precatorio.valor_atual > precatorio.valor_original

    # Testa validações
    with pytest.raises(ValueError):
        Precatorio(**{**data, "ordem": -1})  # ordem não pode ser negativa

    with pytest.raises(ValueError):
        Precatorio(**{**data, "processo": ""})  # processo não pode ser vazio

    with pytest.raises(ValueError):
        Precatorio(
            **{**data, "valor_atual": Decimal("900.00")}
        )  # valor_atual não pode ser menor que original


def test_normalize_to_rows():
    """Testa a normalização dos dados da API"""
    crawler = PrecatoriosCrawler()
    test_data = [
        {
            "results": [
                {
                    "result": {
                        "data": {
                            "dsr": {
                                "DS": [
                                    {
                                        "PH": [
                                            {
                                                "DM0": [
                                                    {"C": ["123456", "1000.00"], "R": 0}
                                                ]
                                            }
                                        ],
                                        "ValueDicts": [
                                            {"G": 0, "N": "R0", "V": "1"},
                                            {"G": 0, "N": "R2", "V": "FORTALEZA"},
                                        ],
                                    }
                                ]
                            }
                        }
                    }
                }
            ]
        }
    ]

    rows = crawler.normalize_to_rows(test_data)
    assert len(rows) > 0
    assert "processo" in rows[0]
    assert "comarca" in rows[0]
