# Crawler de Precatórios TJCE

Crawler para extração de dados de precatórios do TJCE via API do Power BI.

## Funcionalidades

- Extração de dados via API do Power BI
- Paginação automática com RestartTokens
- Formatação de valores monetários e datas
- Validação de dados com Pydantic
- Cache de requisições
- Métricas Prometheus
- Logging estruturado
- Interface CLI e API REST
- Testes unitários

## Requisitos

- Python 3.8+
- Locale pt_BR.UTF-8 (opcional)

## Instalação

1. Clone o repositório:
```bash
git clone https://github.com/seu-usuario/crawler_ceara.git
cd crawler_ceara
```

2. Crie um ambiente virtual:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows
```

3. Instale as dependências:
```bash
pip install -r requirements.txt
```

4. Configure as variáveis de ambiente (opcional):
```bash
cp .env.example .env
# Edite o arquivo .env com suas configurações
```

## Uso

### Via CLI

```bash
# Busca precatórios para uma entidade específica
python main.py --entity "MUNICÍPIO DE FORTALEZA" --output precatorios.csv

# Ajuda
python main.py --help
```

### Via API REST

1. Inicie o servidor:
```bash
python main.py
```

2. Acesse a API:
- Swagger UI: http://localhost:5000/
- Busca de precatórios: http://localhost:5000/fetch?entity=MUNICÍPIO%20DE%20FORTALEZA
- Lista de entidades: http://localhost:5000/entities?output=entidades.csv
- Métricas Prometheus: http://localhost:5000/metrics

#### Endpoints Disponíveis

1. `/fetch` - Busca precatórios de uma entidade
   - Método: GET
   - Parâmetros:
     - `entity`: Nome da entidade (opcional, default: MUNICÍPIO DE FORTALEZA)
     - `output`: Nome do arquivo CSV de saída (opcional, default: precatorios.csv)
   - Resposta: JSON com status, mensagem e dados dos precatórios

2. `/entities` - Lista todas as entidades disponíveis
   - Método: GET
   - Parâmetros:
     - `output`: Nome do arquivo CSV de saída (opcional, default: entidades.csv)
   - Resposta: JSON com status, mensagem e lista de entidades

## Configuração

O crawler pode ser configurado através de variáveis de ambiente ou arquivo `.env`:

- `API_URL`: URL da API do Power BI
- `BATCH_SIZE`: Tamanho do lote de registros por página (default: 500)
- `POWERBI_RESOURCE_KEY`: Chave de recurso do Power BI

## Estrutura do Projeto

```
crawler_ceara/
├── config_new.py     # Configurações (dataclasses)
├── crawler.py        # Implementação do crawler
├── logger.py         # Configuração de logging
├── main.py          # CLI e API REST
├── metrics.py       # Métricas Prometheus
├── models.py        # Modelos Pydantic
└── tests/           # Testes unitários
```

## Métricas Disponíveis

- `crawler_requests_total`: Total de requisições à API
- `crawler_request_duration_seconds`: Duração das requisições
- `crawler_records_processed_total`: Total de registros processados
- `crawler_errors_total`: Total de erros
- `crawler_active_requests`: Número de requisições ativas

## Testes

```bash
# Executa os testes
pytest

# Com cobertura
pytest --cov=.
```

## Contribuindo

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -am 'Adiciona nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Crie um Pull Request

## Licença

Este projeto está licenciado sob a MIT License - veja o arquivo [LICENSE](LICENSE) para detalhes. 