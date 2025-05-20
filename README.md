[![Python Version](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE.md)

# Crawler de Precatórios TJCE

Crawler para extração de dados de precatórios do TJCE via API do Power BI, com interface web e API REST para consulta e download dos dados, incluindo upload para IPFS via Pinata.

## Funcionalidades

- Extração de dados via API do Power BI do TJCE.
- Interface web amigável para consulta e download de CSVs: [http://fernandobzr.pythonanywhere.com/](http://fernandobzr.pythonanywhere.com/)
- API REST para integração e acesso programático aos dados.
- Upload automático dos CSVs gerados para o Pinata (IPFS).
- Paginação automática (quando aplicável pela API fonte).
- Formatação de valores monetários e datas.
- Validação de dados com Pydantic.
- Cache de requisições para otimizar o desempenho.
- Métricas Prometheus para monitoramento.
- Logging estruturado para facilitar a depuração.
- Interface CLI para operações básicas de crawling (legado ou para testes específicos).

## Requisitos

- Python 3.10+ (verificar `requirements.txt` para detalhes de pacotes)
- Locale `pt_BR.UTF-8` (opcional, para formatação monetária local)
- Para funcionalidade de upload para o Pinata: uma conta Pinata e um JWT (JSON Web Token) de API.

## Instalação Local

1.  Clone o repositório:
    ```bash
    git clone https://github.com/seu-usuario/crawler-tjce.git # Substitua pelo URL correto do seu repositório
    cd crawler-tjce
    ```

2.  Crie e ative um ambiente virtual:
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate  # Linux/macOS
    # ou
    # .venv\Scripts\activate  # Windows
    ```

3.  Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```

4.  Configure as variáveis de ambiente:
    Crie um arquivo `.env` na raiz do projeto (copiando de `.env.example` se existir) ou defina as variáveis diretamente no seu ambiente.
    Variáveis importantes:
    *   `PINATA_API_JWT`: Seu JWT da API do Pinata (obrigatório para upload de CSVs para o Pinata).
    *   `FLASK_DEBUG_MODE`: Defina como `True` para desenvolvimento (ex: `FLASK_DEBUG_MODE=True`).
    *   `FLASK_PORT`: Porta para o servidor Flask (default: `5000`).
    *   (Outras variáveis de `config.py` como `API_URL_TJCE`, `POWERBI_RESOURCE_KEY_TJCE` se você precisar sobrescrever os defaults).

## Uso Local

### Interface Web (Recomendado para Usuários)

1.  Inicie o servidor Flask:
    ```bash
    python3 main.py
    ```
2.  Acesse a interface no seu navegador:
    *   **Página Principal:** [http://localhost:5000/](http://localhost:5000/)
        *   Permite selecionar a entidade devedora.
        *   Permite especificar a quantidade de precatórios a buscar (opcional).
        *   Possui um botão para "Buscar Precatórios" que informa a quantidade encontrada.
        *   Possui um botão para "Baixar CSV" (habilitado após a busca) que faz o download do arquivo (via Pinata se configurado e o upload ocorrer).

### API REST (Local)

1.  Inicie o servidor Flask (se ainda não estiver rodando):
    ```bash
    python3 main.py
    ```

2.  Acesse a documentação interativa da API (Swagger UI):
    *   [http://localhost:5000/docs](http://localhost:5000/docs)

3.  Exemplos de Endpoints (base URL local: `http://localhost:5000`):

    *   **Listar Entidades Disponíveis:**
        `GET /api/entities`
        *   Retorna a lista de entidades e, se configurado, a URL do Pinata para o CSV com todas as entidades.

    *   **Buscar Precatórios para uma Entidade:**
        `GET /api/fetch?entity=<slug-da-entidade>&count=<quantidade>`
        *   Exemplo: `GET /api/fetch?entity=municipio-de-fortaleza&count=10`
        *   Exemplo sem quantidade (busca todos): `GET /api/fetch?entity=municipio-de-fortaleza`
        *   Parâmetros:
            *   `entity` (obrigatório): Slug da entidade (ex: `municipio-de-fortaleza`, `estado-do-ceara`). Use o endpoint `/api/entities` para obter os slugs corretos.
            *   `count` (opcional): Número de registros a serem retornados.
        *   Retorna os dados dos precatórios, a contagem (`num_precatorios_found`) e, se configurado, a URL do Pinata para o CSV gerado.

    *   **Verificar Saúde da API:**
        `GET /api/health`
        *   Retorna o status da aplicação.

    *   **Métricas Prometheus:**
        `GET /metrics`
        *   Expõe métricas para monitoramento com Prometheus.

### Via CLI (Uso Limitado/Legado)

O uso principal é via interface web ou API REST. A CLI pode ser usada para testes de crawling direto, mas não interage com Pinata ou a interface web.

```bash
# Busca precatórios para uma entidade específica (usando nome oficial, não slug)
# python3 main.py --entity "MUNICÍPIO DE FORTALEZA" --output precatorios_cli.csv

# Ajuda
# python3 main.py --help
```

## Configuração Detalhada

As configurações principais são gerenciadas pelo arquivo `config.py` e podem ser sobrescritas por variáveis de ambiente.

**Variáveis de Ambiente Importantes:**

*   `PINATA_API_JWT`: **Obrigatório** para a funcionalidade de upload dos CSVs para o Pinata. Sem este JWT, os arquivos CSV serão gerados localmente (temporariamente) mas não serão enviados para o Pinata, e as URLs do Pinata não aparecerão na resposta da API.
*   `PINATA_GATEWAY_URL`: Opcional. Gateway do Pinata a ser usado para construir as URLs públicas. Default: `gateway.pinata.cloud`.
*   `FLASK_DEBUG_MODE`: `True` ou `False`. Ativa/desativa o modo de debug do Flask. Default: `False`.
*   `FLASK_PORT`: Porta em que o servidor Flask rodará. Default: `5000`.
*   `API_URL_TJCE`: URL da API do PowerBI do TJCE. (Default definido em `config.py`)
*   `POWERBI_RESOURCE_KEY_TJCE`: Chave do recurso do PowerBI do TJCE. (Default definido em `config.py`)
*   `CACHE_DEFAULT_TIMEOUT`: Tempo de vida do cache para respostas da API. (Default definido em `config.py`)
*   `CACHE_TIMEOUT_ENTITIES`: Tempo de vida específico do cache para o endpoint `/api/entities`.
*   `LOG_LEVEL`: Nível de log (ex: `INFO`, `DEBUG`).

(Verifique `config.py` para a lista completa de configurações e seus valores default).

## Deploy no PythonAnywhere

A aplicação está disponível em: [https://fernandobzr.pythonanywhere.com/](https://fernandobzr.pythonanywhere.com/)

Dicas para deploy ou manutenção no PythonAnywhere:

1.  **Ambiente Virtual:**
    *   Crie um ambiente virtual no PythonAnywhere usando a versão do Python compatível com o projeto (ex: Python 3.10 ou 3.11).
    *   Exemplo no console Bash do PythonAnywhere: `mkvirtualenv --python=/usr/bin/python3.10 venv-nome-do-app` (substitua `venv-nome-do-app` e a versão do Python conforme necessário).
    *   Ative o virtualenv: `workon venv-nome-do-app`.

2.  **Código Fonte:**
    *   Faça o upload do seu código para o PythonAnywhere (via `git clone` no console Bash é o mais recomendado) ou use a funcionalidade de upload de arquivos.

3.  **Dependências:**
    *   Navegue até o diretório do seu projeto no console Bash e instale as dependências no virtualenv ativo:
        ```bash
        pip install -r requirements.txt
        ```

4.  **Configuração da Web App:**
    *   No painel do PythonAnywhere, vá para a aba "Web".
    *   Crie uma nova Web App ou edite uma existente.
    *   Selecione o framework "Flask".
    *   Especifique o caminho para o seu arquivo WSGI. Por padrão, o PythonAnywhere cria um arquivo como `/var/www/seuusuario_pythonanywhere_com_wsgi.py`. Você precisará editá-lo.
    *   Aponte para o virtualenv correto na seção "Virtualenv".

5.  **Arquivo WSGI:**
    *   Edite o arquivo WSGI gerado pelo PythonAnywhere. Um exemplo básico para Flask:
        ```python
        import sys

        # Adiciona o caminho do projeto ao sys.path
        path = '/home/seuusuario/caminho-para-seu-projeto/crawler-tjce' # Altere para o seu caminho
        if path not in sys.path:
            sys.path.insert(0, path)

        # Importa a aplicação Flask (o objeto 'app' do seu main.py)
        from main import app as application
        ```
    *   Certifique-se de que `seuusuario` e `caminho-para-seu-projeto/crawler-tjce` correspondem aos seus diretórios no PythonAnywhere.

6.  **Variáveis de Ambiente:**
    *   Na aba "Web" do PythonAnywhere, role para baixo até a seção "Environment variables".
    *   Adicione as variáveis necessárias (ex: `PINATA_API_JWT`, `FLASK_DEBUG_MODE=False`).

7.  **Diretório de Trabalho (Working Directory):**
    *   Na aba "Web", você pode definir o "Working directory" para o diretório raiz do seu projeto. Isso pode ajudar com caminhos relativos para templates ou arquivos de dados.

8.  **Logs:**
    *   Verifique os logs de erro e de servidor do PythonAnywhere se encontrar problemas. Eles estão disponíveis na aba "Web".

9.  **Arquivos Estáticos (CSS/JS da Interface Web):**
    *   Para a interface web (`index.html` e seus assets), se você tiver arquivos CSS ou JS separados, precisará configurar o mapeamento de arquivos estáticos na aba "Web" do PythonAnywhere.
    *   Para o `index.html` atual que usa CSS e JS inline ou dentro de `templates/`, o Flask (`render_template`) já cuida disso.

10. **Recarregar Web App:**
    *   Após qualquer alteração na configuração ou no código, clique no botão "Reload" na sua aba "Web" no PythonAnywhere.

## Estrutura do Projeto (Principais Componentes)

```
crawler-tjce/
├── .venv/                  # Ambiente virtual Python
├── data/                   # Diretório para CSVs gerados localmente (ex: entidades)
├── templates/
│   └── index.html          # Página HTML da interface web
├── config.py               # Configurações da aplicação (valores default, carrega .env)
├── crawler.py              # Lógica principal de extração e normalização dos precatórios
├── entity_mapping.py       # Mapeamento e utilitários para nomes/slugs de entidades
├── entity_mapping_crawler.py # Crawler específico para obter a lista de entidades
├── logger.py               # Configuração do logging estruturado
├── main.py                 # Aplicação Flask (API REST, interface web) e CLI
├── metrics.py              # Métricas Prometheus
├── models.py               # Modelos Pydantic para validação de dados
├── pinata_uploader.py      # Lógica para upload de arquivos para o Pinata
├── requirements.txt        # Dependências Python
├── README.md               # Este arquivo
└── .env.example            # Exemplo de arquivo de variáveis de ambiente
```

## Métricas Disponíveis

- `crawler_requests_total{entity}`: Total de requisições feitas à API, por entidade.
- `crawler_request_duration_seconds{entity}`: Duração das requisições, por entidade.
- `crawler_records_processed_total{entity}`: Total de registros processados, por entidade.
- `crawler_errors_total{type, entity}`: Total de erros ocorridos, por tipo de erro e entidade.
- `crawler_active_requests{entity}`: Número de requisições ativas, por entidade.

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

Este projeto está licenciado sob a MIT License - veja o arquivo [LICENSE.md](LICENSE.md) para detalhes. 