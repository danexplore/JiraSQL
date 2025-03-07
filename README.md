# Jira SQL Integration

This project integrates Jira with a MySQL database to manage and analyze Jira issues. The script fetches data from Jira, processes it, and stores it in a MySQL database. It also includes functionality to update the database schema and create views for data analysis.

## Features

- Fetches Jira issues using the Jira API.
- Processes and normalizes data.
- Inserts data into a MySQL database.
- Updates database schema and creates views for analysis.
- Displays pop-up notifications for successful operations.

## Requirements

- Python 3.8+
- MySQL
- The following Python packages (listed in `requirements.txt`):
  - certifi
  - cffi
  - charset-normalizer
  - colorama
  - cryptography
  - idna
  - iniconfig
  - packaging
  - pluggy
  - pycparser
  - PyMySQL
  - pytest
  - python-dotenv
  - requests
  - tqdm
  - urllib3

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/jira_sql.git
    cd jira_sql
    ```

2. Create a virtual environment and activate it:
    ```sh
    python -m venv .venv
    .venv\Scripts\activate  # On Windows
    source .venv/bin/activate  # On macOS/Linux
    ```

3. Install the required packages:
    ```sh
    pip install -r requirements.txt
    ```

4. Create a `.env` file in the project root directory with the following content:
    ```env
    JIRA_USERNAME=your_jira_username
    JIRA_API_TOKEN=your_jira_api_token
    COOKIE=your_jira_cookie
    DB_HOST=your_db_host
    DB_USER=your_db_user
    DB_PASSWORD=your_db_password
    DB_NAME=your_db_name
    ```

## Usage

1. Run the script:
    ```sh
    python jira_sql.py
    ```

2. The script will fetch data from Jira, process it, and insert it into the MySQL database. It will also update the database schema and create views for analysis.

## Functions

- `exibir_popup(mensagem)`: Displays a pop-up notification with the given message.
- `conectar_banco()`: Connects to the MySQL database.
- `obter_data_criacao_mais_recente()`: Retrieves the most recent creation date from the database.
- `obter_primeiro_coordenador(coordenador)`: Normalizes and extracts the first coordinator from a string.
- `obter_coordenador_id(cursor, coordenador)`: Retrieves the coordinator ID from the database.
- `salvar_dados_mysql(dados)`: Inserts data into the MySQL database.
- `realizar_requisicao(url, headers, params, max_retentativas=3)`: Makes a request to the Jira API with retries.
- `processar_status_subtarefas(tipo_de_item, subtasks)`: Processes subtasks to extract specific statuses and identifies the course type.
- `obter_dados_jira(data_criacao_inicio=None)`: Fetches data from the Jira API.
- `atualizar_estrutura_tabela()`: Updates the database schema and populates the courses and coordinators tables.
- `extrair_entidade(entidade_curso)`: Extracts the entity from the `entidade_curso` field.
- `main()`: Main function that orchestrates the data fetching, processing, and database insertion.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.