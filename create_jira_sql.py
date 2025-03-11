import os
import time
import pymysql
import base64
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import datetime

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configurações da API Jira
username = os.getenv("JIRA_USERNAME")
api_token = os.getenv("JIRA_API_TOKEN")

# Gerar o cabeçalho de autenticação
auth_str = f"{username}:{api_token}"
auth_bytes = auth_str.encode("utf-8")
auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")

headers = {
    "Accept": "application/json",
    "Authorization": f"Basic {auth_base64}",
    'Cookie': os.getenv("COOKIE")
}

# Configurações do Banco de Dados MySQL
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = int(os.getenv("DB_PORT"))

ano_atual = datetime.now().year
mes_atual = datetime.now().month

last_updated = open('last_updated.txt', 'r').read().strip()
update_time = time.time.now().format('%Y-%m-%d')

# Atualizar o CREATE_TABLE_SQL para incluir a nova estrutura
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS coordenadores (
    id INT AUTO_INCREMENT PRIMARY KEY,
    coordenador VARCHAR(255) UNIQUE NOT NULL,
    coordenador_master VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS cursos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nome_curso VARCHAR(255) NOT NULL,
    entidade VARCHAR(50) NOT NULL,
    coordenador_id INT,
    FOREIGN KEY (coordenador_id) REFERENCES coordenadores(id)
);

CREATE TABLE IF NOT EXISTS db_dpc_jira (
    chave VARCHAR(50) PRIMARY KEY,
    link_jira VARCHAR(255),
    rotulos TEXT,
    data_para_ficar_pronto DATE,
    data_criacao DATE,
    data_atualizacao DATE,
    data_de_lancamento DATE,
    date_launch_jira VARCHAR(50),
    ano VARCHAR(4),
    mes VARCHAR(2),
    status_launch VARCHAR(20),
    resumo TEXT,
    descricao TEXT,
    versoes_corrigidas TEXT,
    tipo_de_item VARCHAR(50),
    situacao VARCHAR(50),
    cpf_conteudista VARCHAR(50),
    conteudista TEXT,
    coordenador TEXT,
    coordenador_master TEXT,
    entidade_curso TEXT,
    entidade VARCHAR(50),
    migracao VARCHAR(10),
    curso TEXT,
    curso_id INT,
    coordenador_id INT,
    status_contrato VARCHAR(50),
    status_conteudos TEXT,
    status_videos TEXT,
    FOREIGN KEY (curso_id) REFERENCES cursos(id),
    FOREIGN KEY (coordenador_id) REFERENCES coordenadores(id)
);

CREATE OR REPLACE VIEW vw_analise_producao AS
WITH status_count AS (
    SELECT 
        c.id AS curso_id,
        c.nome_curso AS curso,
        c.entidade AS entidade,
        c.coordenador_id AS coordenador_id,
        MIN(d.data_criacao) AS primeira_data_criacao,
        COUNT(*) AS total_disciplinas,
        SUM(CASE 
            WHEN d.tipo_de_item = 'SR-Completa' AND d.status_conteudos = 'Fechado' THEN 1 
            ELSE 0 
        END) AS Conteudo_Fechado,
        SUM(CASE 
            WHEN d.tipo_de_item = 'SR-Reuso' THEN 1 
            ELSE 0 
        END) AS disciplinas_reuso,
        SUM(CASE 
            WHEN d.tipo_de_item = 'SR-Completa' AND d.status_videos = 'Fechado' THEN 1
            ELSE 0 
        END) AS Video_Fechado,
        SUM(CASE 
            WHEN d.tipo_de_item = 'SR-Reuso' AND d.status_videos = 'Fechado' THEN 1 
            ELSE 0 
        END) AS Video_Reuso
    FROM db_dpc_jira d
    JOIN cursos c ON d.curso_id = c.id
    WHERE c.nome_curso IS NOT NULL AND c.entidade != "Pós-Graduação" AND c.coordenador_id IS NOT NULL
    GROUP BY c.id, c.nome_curso, c.entidade, c.coordenador_id
)
SELECT 
    curso_id,
    coordenador_id,
    curso,
    entidade,
    primeira_data_criacao,
    total_disciplinas,
    Conteudo_Fechado,
    Video_Fechado,
    disciplinas_reuso,
    ROUND(
        CASE 
            WHEN total_disciplinas > 0 THEN (Conteudo_Fechado * 100.0 / (total_disciplinas-disciplinas_reuso)) 
            ELSE 0 
        END, 2
    ) AS prod_conteudo,
    ROUND(
        CASE 
            WHEN total_disciplinas > 0 THEN (Video_Fechado * 100.0 / (total_disciplinas-disciplinas_reuso)) 
            ELSE 0 
        END, 2
    ) AS prod_video,
    ROUND(
        CASE 
            WHEN total_disciplinas > 0 THEN 
                (((Conteudo_Fechado + disciplinas_reuso) * 100.0 / total_disciplinas) + 
                 ((Video_Fechado + disciplinas_reuso) * 100.0 / total_disciplinas)) / 2
            ELSE 0 
        END, 2
    ) AS prod_curso,
    CASE 
        WHEN total_disciplinas = 0 THEN 'Não Iniciado'
        WHEN (total_disciplinas - disciplinas_reuso) = Conteudo_Fechado 
             AND (total_disciplinas - disciplinas_reuso) = Video_Fechado THEN 'Completo'
        WHEN Conteudo_Fechado > 0 OR Video_Fechado > 0 THEN 'Em Andamento'
        ELSE 'Não Iniciado'
    END AS status_producao
FROM status_count
where total_disciplinas > 5
ORDER BY curso;
"""

print('connecting to database')
sql_client = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
    port=DB_PORT
)
print('connected to database')

def atualizar_estrutura_tabela():
    """
    Atualiza a estrutura da tabela existente e popula as tabelas de cursos e coordenadores
    """
    sql_client
    cursor = sql_client.cursor()
    
    try:
        # Criar/atualizar as tabelas
        for comando in CREATE_TABLE_SQL.split(';'):
            if comando.strip():
                cursor.execute(comando)
       
        
        # Popula a tabela de cursos
        cursor.execute(f"""
            INSERT IGNORE INTO cursos (nome_curso, entidade)
            SELECT DISTINCT
                curso,
                entidade
            FROM db_dpc_jira 
            WHERE curso IS NOT NULL AND entidade IS NOT NULL AND data_atualizacao >= '{last_updated}'
        """)
        
        # Atualiza os IDs dos cursos na tabela principal
        cursor.execute("SET SQL_SAFE_UPDATES = 0")  
        cursor.execute("""
            UPDATE db_dpc_jira d
            JOIN cursos c ON d.curso = c.nome_curso AND d.entidade = c.entidade
            SET d.curso_id = c.id
            WHERE d.chave IS NOT NULL
        """)

        # Popula a tabela de coordenadores com os dados existentes, utilizando primeiro_coordenador
        cursor.execute("""
            INSERT INTO coordenadores (coordenador, coordenador_master)
            SELECT 
                TRIM(SUBSTRING_INDEX(
                    REPLACE(
                        REPLACE(
                            REPLACE(REPLACE(CONVERT(coordenador USING utf8mb4), ' / ', '/'), 
                        ' /', '/'),
                    '/ ', '/'), 
                '’', ''''), 
                '/', 1)) AS primeiro_coordenador,
                
                CASE 
                    WHEN d.coordenador_master = 'InsBE' THEN 'INSBE'
                    WHEN d.coordenador_master = 'IBREAD' THEN 'IBREAD'
                    WHEN d.coordenador_master = 'André Luiz Cecil Vaz de Carvalho' THEN 'INSBE'
                    WHEN d.coordenador_master = 'Marcel Hasslocher' THEN 'IBREAD'
                    WHEN d.coordenador_master = 'Jackson Santos dos Reis' THEN 'Jackson Santos dos Reis'
                    ELSE 'None'
                END AS coordenador_master

            FROM (
                SELECT DISTINCT 
                    CASE 
                        WHEN coordenador LIKE 'Coord. Geral dos Cursos em Direito%' THEN 'Coordenação Geral dos Cursos de Direito'
                        WHEN coordenador LIKE 'Renata Alcione de Faria Rodrigues%' THEN 'Renata Alcione de Faria Villela de Araújo'
                        ELSE TRIM(REPLACE(CONVERT(coordenador USING utf8mb4), UNHEX('C2A0'), ''))
                    END AS coordenador,
                    coordenador_master
                FROM db_dpc_jira 
                WHERE coordenador IS NOT NULL
            ) d
            ORDER BY primeiro_coordenador
            ON DUPLICATE KEY UPDATE coordenador_master = VALUES(coordenador_master);
        """)

        # Atualiza os IDs dos coordenadores na tabela principal utilizando "primeiro_coordenador"
        cursor.execute("""
            UPDATE db_dpc_jira d
            JOIN (
                SELECT id, coordenador 
                FROM coordenadores
            ) c ON TRIM(SUBSTRING_INDEX(
                REPLACE(
                    REPLACE(
                        REPLACE(REPLACE(CONVERT(d.coordenador USING utf8mb4), ' / ', '/'), 
                    ' /', '/'),
                '/ ', '/'), 
            '’', ''''), 
            '/', 1)) = c.coordenador
            SET d.coordenador_id = c.id
            WHERE d.chave IS NOT NULL
        """)
        
        # Atualiza os IDs de coordenadores na tabela cursos
        cursor.execute("SET SQL_SAFE_UPDATES = 0")  
        cursor.execute("""
            UPDATE cursos c
            JOIN db_dpc_jira d ON c.nome_curso = d.curso AND c.entidade = d.entidade
            SET c.coordenador_id = d.coordenador_id
            WHERE d.coordenador_id IS NOT NULL
        """)
        cursor.execute("SET SQL_SAFE_UPDATES = 1")  
        
        sql_client.commit()
        print("Estrutura da tabela atualizada com sucesso!")
        
    except Exception as e:
        sql_client.rollback()
        print(f"Erro ao atualizar estrutura da tabela: {str(e)}")
        raise e
    finally:
        cursor.close()

def main():
    atualizar_estrutura_tabela()

if __name__ == "__main__":
    main()
    sql_client.close()
    open('last_updated.txt', 'w').write(update_time)