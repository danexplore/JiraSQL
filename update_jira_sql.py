import os
import re
import time
import requests
import pymysql
import base64
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import datetime
from urllib.parse import quote
import unicodedata
import re   

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

print('connecting to database')
sql_client = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
    port=DB_PORT
)
print('connected to database')

update_time = datetime.now().strftime("%Y-%m-%d")
last_updated = open("last_updated.txt", "r").read().strip()
print(last_updated)
if last_updated == update_time:
    print("Já foi atualizado hoje.")
    exit()

def obter_primeiro_coordenador(coordenador):
    if not coordenador:
        return None
    
    # Normaliza espaços invisíveis e caracteres especiais
    coordenador = unicodedata.normalize("NFKC", coordenador).strip()
    
    # Remove espaços invisíveis como CHAR(160) (U+00A0)
    coordenador = coordenador.replace("\u00A0", "")

    # Substitui variações de separadores "/ " e " /" por "/"
    coordenador = re.sub(r"\s*/\s*", "/", coordenador)

    # Converte apóstrofos curvos para apóstrofos normais
    coordenador = coordenador.replace("’", "'")

    # Obtém o primeiro coordenador antes da primeira "/"
    primeiro_coordenador = coordenador.split("/", 1)[0].strip()
    
    return primeiro_coordenador

# Função para salvar dados no banco com transação
def salvar_dados_mysql(dados):
    cursor = sql_client.cursor()
    
    try:
        # Inserir dados principais
        start_commit_time = time.time()
        print("***-*-*-*-*-*-*-*-*-******-*-*-*-*-*-*-*-*-***")

        progress_bar = tqdm(total=len(dados), desc="Salvando chamados", unit="chamado")
        
        for issue in dados:
            # Inserir na tabela principal
            cursor.execute(
            """
            INSERT INTO db_dpc_jira (
                chave, link_jira, rotulos, data_para_ficar_pronto, data_criacao, data_atualizacao, 
                data_de_lancamento, date_launch_jira, ano, mes, status_launch, 
                resumo, versoes_corrigidas, tipo_de_item, situacao, cpf_conteudista,
                conteudista, coordenador, coordenador_master, entidade_curso, entidade,
                migracao, curso, status_contrato, status_conteudos, 
                status_videos, descricao
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                 %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                rotulos = VALUES(rotulos),
                link_jira = VALUES(link_jira),
                data_para_ficar_pronto = VALUES(data_para_ficar_pronto),
                data_criacao = VALUES(data_criacao),
                data_atualizacao = VALUES(data_atualizacao),
                data_de_lancamento = VALUES(data_de_lancamento),
                date_launch_jira = VALUES(date_launch_jira),
                ano = VALUES(ano),
                mes = VALUES(mes),
                status_launch = VALUES(status_launch),
                resumo = VALUES(resumo),
                versoes_corrigidas = VALUES(versoes_corrigidas),
                tipo_de_item = VALUES(tipo_de_item),
                situacao = VALUES(situacao),
                cpf_conteudista = VALUES(cpf_conteudista),
                conteudista = VALUES(conteudista),
                coordenador = VALUES(coordenador),
                coordenador_master = VALUES(coordenador_master),
                entidade_curso = VALUES(entidade_curso),
                entidade = VALUES(entidade),
                migracao = VALUES(migracao),
                curso = VALUES(curso),
                status_contrato = VALUES(status_contrato),
                status_conteudos = VALUES(status_conteudos),
                status_videos = VALUES(status_videos),
                descricao = VALUES(descricao)
            """,
            (
                issue['chave'], issue['link_jira'], issue['rotulos'], 
                issue['data_para_ficar_pronto'], issue['data_criacao'], issue['data_atualizacao'], 
                issue['data_de_lancamento'], issue['date_launch_jira'], 
                issue['ano'], issue['mes'], issue['status_launch'], 
                issue['resumo'], issue['versoes_corrigidas'], 
                issue['tipo_de_item'], issue['situacao'], 
                issue['cpf_conteudista'], issue['conteudista'], 
                issue['coordenador'], issue['coordenador_master'], 
                issue['entidade_curso'], issue['entidade'],
                issue['migracao'], issue['curso'], 
                issue['status_contrato'], issue['status_conteudos'], 
                issue['status_videos'], issue['descricao']
            )
            )
            progress_bar.update(1)
        
        sql_client.commit()
        progress_bar.close()
        end_commit_time = time.time()
        print("***-*-*-*-*-*-*-*-*-******-*-*-*-*-*-*-*-*-***\n")
        print(f"Chamados salvos com sucesso!\nTempo gasto para salvar os chamados: {end_commit_time - start_commit_time:.2f} segundos\n***-*-*-*-*-*-*-*-*-******-*-*-*-*-*-*-*-*-***")
        
    except Exception as e:
        sql_client.rollback()
        print(f"Erro ao salvar dados: {str(e)}")
        raise e
    finally:
        cursor.close()

# Função para realizar uma requisição com retentativas
def realizar_requisicao(url, headers, params, max_retentativas=3):
    for tentativa in range(max_retentativas):
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            return response
        print(f"Tentativa {tentativa + 1} falhou. Retentando em 2 segundos...")
        time.sleep(2)
    raise Exception(f"Falha ao conectar à API após {max_retentativas} tentativas.")

# Função para pegar os status das subtasks (Video e Conteúdo)
def processar_status_subtarefas(tipo_de_item, subtasks):
    """
    Processa as subtarefas para extrair os status específicos e identifica o tipo de curso.
    """
    status_contrato = []
    status_conteudos = []
    status_videos = []
    tem_video = False
    
    # Inicializa as variáveis de status final
    final_status_contrato = None
    final_status_conteudos = None
    final_status_videos = None
    
    for subtask in subtasks:
        fields = subtask['fields']
        summary = fields['summary'].upper()
        status = fields['status']['name']
        
        # Verifica se há menção a vídeo nas subtarefas
        if "VIDEO" in summary or "VÍDEO" in summary:
            tem_video = True

        if tipo_de_item == "SR-Reuso":
            final_status_conteudos = "REUSO"
            final_status_videos = "REUSO"
            final_status_contrato = "REUSO"
            break  # Se é reuso, não precisa continuar o loop
        else:
            if "CONTRATO - ELABORAR" in summary:
                status_contrato.append(status)
            elif "CONTEÚDO - ENTREGAR" in summary:
                status_conteudos.append(status)
            elif "VÍDEO - GRAVAR" in summary:
                status_videos.append(status)
                tem_video = True

    # Só processa os status se não for REUSO
    if tipo_de_item != "SR-Reuso":
        # Processamento do status do contrato
        if "Aberto" in status_contrato:
            final_status_contrato = "Aberto"
        elif any(s in ["Resolvido", "Fechado/Aprovado"] for s in status_contrato):
            final_status_contrato = "Fechado"
        elif "Em Resolução" in status_contrato:
            final_status_contrato = "Em Resolução"
        elif any(s in ["Reopen", "Pendente"] for s in status_contrato):
            final_status_contrato = "Pendente"

        # Processamento do status dos conteúdos
        if "Aberto" in status_conteudos:
            final_status_conteudos = "Aberto"
        elif any(s in ["Resolvido", "Fechado/Aprovado"] for s in status_conteudos):
            final_status_conteudos = "Fechado"
        elif "Em Resolução" in status_conteudos:
            final_status_conteudos = "Em Resolução"
        elif any(s in ["Reopen", "Pendente"] for s in status_conteudos):
            final_status_conteudos = "Pendente"

        # Processamento do status dos vídeos
        if "Aberto" in status_videos:
            final_status_videos = "Aberto"
        elif any(s in ["Resolvido", "Fechado/Aprovado"] for s in status_videos):
            final_status_videos = "Fechado"
        elif "Em Resolução" in status_videos:
            final_status_videos = "Em Resolução"
        elif any(s in ["Reopen", "Pendente"] for s in status_videos):
            final_status_videos = "Pendente"
    
    return final_status_contrato, final_status_conteudos, final_status_videos, tem_video

# Função para extrair a entidade do curso
def extrair_entidade(entidade_curso):
    """
    Extrai a entidade do campo entidade_curso.
    """
    if not entidade_curso:
        return None
        
    # Divide a string no primeiro ' - ' se existir
    partes = entidade_curso.split(' - ', 1)
    primeira_parte = partes[0]
    
    # Extrai a parte após qualquer "Fac. Unyleya | "
    if "Fac. Unyleya | " in primeira_parte:
        # Pega tudo após "Fac. Unyleya | "
        texto_apos_prefixo = primeira_parte.split("Fac. Unyleya | ")[1]
        
        # Se tem mais divisões com |, pega a última parte
        if " | " in texto_apos_prefixo:
            entidade = texto_apos_prefixo.split(" | ")[-1]
        else:
            entidade = texto_apos_prefixo
            
        return entidade
    
    return None

# Função para obter dados da API Jira
def obter_dados_jira():
    # Lista de prefixos válidos para Entidade e Curso
    entidades_validas = [
        "Fac. Unyleya | CETEC",
        "Fac. Unyleya | CEAB",
        "Fac. Unyleya | CECA",
        "Fac. Unyleya | CECaV",
        "Fac. Unyleya | CECOMEX",
        "Fac. Unyleya | CECONF",
        "Fac. Unyleya | CEDAC",
        "Fac. Unyleya | CEDUC",
        "Fac. Unyleya | CEENG",
        "Fac. Unyleya | CEGEP",
        "Fac. Unyleya | CEJUR",
        "Fac. Unyleya | CEPÓS",
        "Fac. Unyleya | CES",
        "Fac. Unyleya | NEPIC",
        "Fac. Unyleya | Pós | 3R Capacita",
        "Fac. Unyleya | Pós | Bioforense",
        "Fac. Unyleya | Pós-Graduação",
        "Fac. Unyleya | YMED",
        "Fac. Unyleya | YODONTO",
        "Fac. Unyleya | YVET"
    ]
    
    start_at = 0
    max_results = 1000
    all_issues = []

    jql_query = (
        'project = PROCONTEUD AND "Entidade e Curso" != "Fac. Unyleya | Graduação"'
        ' AND issuetype in (SR-Completa, SR-Reuso)'
        ' AND status in (Reopen, Closed, Done, "In Progress", "To Do", Pending)'
        f' AND updated >= {last_updated} ORDER BY duedate DESC'
    )

    jql_encoded = quote(jql_query, safe=":=,()")
    base_url = f"https://jira.unyleya.com.br/rest/api/2/search?jql={jql_encoded}"

    params = {
        "fields": ",".join([
            "total",
            "customfield_10808",
            "labels",
            "description",
            "customfield_10803",
            "customfield_10804",
            "created",
            "updated",
            "fixVersions",
            "duedate",
            "summary",
            "issuetype",
            "status",
            "customfield_11303",
            "customfield_10802",
            "subtasks",
        ]),
        "startAt": start_at,
        "maxResults": max_results
    }
    response = realizar_requisicao(base_url, headers, params)

    data = response.json()
    total_issues = data["total"]  # Total issues from Jira API
    progress_bar = tqdm(total=total_issues, desc="Processando tarefas", unit="tarefa")

    while True:
        params["startAt"] = start_at
        response = realizar_requisicao(base_url, headers, params)
        data = response.json()
        issues = data.get("issues", [])
        if not issues:
            break

        for issue in issues:
            fields = issue["fields"]

            # Campo Entidade e Curso
            entidade_curso_data = fields.get("customfield_10808")
            if entidade_curso_data:
                main_value = entidade_curso_data.get("value", "")
                child_value = entidade_curso_data.get("child", {}).get("value", "")
                entidade_curso = f"{main_value} - {child_value}" if child_value else main_value
                entidade = extrair_entidade(entidade_curso)
            else:   
                entidade_curso = None
                entidade = None
            
            # Verifica se a entidade_curso começa com algum dos prefixos válidos
            if not entidade_curso or not any(entidade_curso.startswith(prefix) for prefix in entidades_validas):
                continue
            
            if "Fac. Unyleya | Graduação" in main_value:
                continue
            
            chave = issue["key"]
            rotulos = fields.get("labels", [])
            descricao = fields.get("description")
            coordenador = fields.get("customfield_10803")
            coordenador_master = fields.get("customfield_10804")
            versoes_corrigidas = ", ".join(v["name"] for v in fields.get("fixVersions", []))
            padrao = r'\d{6}-[A-Z]+'
            date_launch = re.search(padrao, versoes_corrigidas)
            date_launch_result = date_launch.group(0) if date_launch else "Sem Previsão"
            if date_launch_result:
                date_launch_result = date_launch_result.split("-")[0]
                ano = date_launch_result[-4:].strip() if date_launch_result != "Sem Previsão" else None
                mes = date_launch_result[:2].strip() if date_launch_result != "Sem Previsão" else None

            status_launch = (
                "Sem Previsão" if date_launch_result == "Sem Previsão"
                else "Lançado" if (int(ano) < ano_atual) or (int(ano) == ano_atual and int(mes) <= mes_atual)
                else "Em breve"
            )

            link_jira = f"https://jira.unyleya.com.br/browse/{chave}"

            if child_value:
                curso = child_value
            elif descricao:
                if "}" in descricao and "{" in descricao:
                    try:
                        curso = descricao.split("}", 1)[1].split("{", 1)[0].strip()
                    except IndexError:
                        continue
                else:
                    continue
            if curso:
                if curso.startswith("CURSO: "):
                    curso = curso.split("CURSO: ",1)[1].strip()
                elif curso.startswith("Bom dia") or curso.startswith('*') or curso.startswith("OBS:"):
                    continue
            else:
                continue

            # Data de Criação
            created_datetime = fields.get("created", "")
            created_date = created_datetime.split("T")[0] if created_datetime else None
            updated_datetime = fields.get("updated", "")
            updated_date = updated_datetime.split("T")[0] if updated_datetime else None

            release_dates = [v["releaseDate"] for v in fields.get("fixVersions", []) if "releaseDate" in v]
            if len(release_dates) > 1:
                release_date = min(release_dates)
            else:
                release_date = release_dates[0] if release_dates else None

            # Tipo de item
            tipo_de_item = fields.get("issuetype", {}).get("name")

            # Processa as subtarefas
            subtasks = fields.get("subtasks", [])
            status_contrato, status_conteudos, status_videos, tem_video = processar_status_subtarefas(tipo_de_item, subtasks)

            migracao = "SV>CV" if any("SV>CV" in label for label in rotulos) else "CV" if tem_video else "SV"

            # Adicionando os dados processados
            all_issues.append({
                "chave": chave,
                "link_jira": link_jira,
                "rotulos": ", ".join(rotulos) or None,
                "data_para_ficar_pronto": fields.get("duedate") or None,
                "data_criacao": created_date,
                "data_atualizacao": updated_date,
                "data_de_lancamento": release_date,
                "date_launch_jira": date_launch_result,
                "ano": ano,
                "mes": mes,
                "status_launch": status_launch,
                "resumo": fields.get("summary"),
                "versoes_corrigidas": versoes_corrigidas,
                "tipo_de_item": tipo_de_item,
                "situacao": fields.get("status", {}).get("name"),
                "cpf_conteudista": fields.get("customfield_11303") or None,
                "conteudista": fields.get("customfield_10802") or None,
                "coordenador": coordenador,
                "coordenador_master": coordenador_master,
                "entidade_curso": entidade_curso,
                "entidade": entidade,
                "migracao": migracao,
                "curso": curso,
                "status_contrato": status_contrato,
                "status_conteudos": status_conteudos,
                "status_videos": status_videos,
                "descricao": descricao,
            })

        progress_bar.update(len(issues))

        if start_at >= total_issues:
            break

        start_at += max_results

    return all_issues

def popular_atualizar_cursos_coordenadores():
    """
    Atualiza a estrutura da tabela existente e popula as tabelas de cursos e coordenadores
    """
    cursor = sql_client.cursor()
    
    start_commit_time = time.time()
    print("***-*-*-*-*-*-*-*-*-******-*-*-*-*-*-*-*-*-***\n Populando e atualizando estrutura da tabela...")

    try:        
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
        cursor.execute(f"""
            UPDATE db_dpc_jira d
            JOIN cursos c ON d.curso = c.nome_curso AND d.entidade = c.entidade
            SET d.curso_id = c.id
            WHERE d.chave IS NOT NULL AND d.data_atualizacao >= '{last_updated}'
        """)

        # Popula a tabela de coordenadores com os dados existentes, utilizando primeiro_coordenador
        cursor.execute(f"""
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
                WHERE coordenador IS NOT NULL AND data_atualizacao >= '{last_updated}'
            ) d
            ORDER BY primeiro_coordenador
            ON DUPLICATE KEY UPDATE coordenador_master = VALUES(coordenador_master);
        """)

        # Atualiza os IDs dos coordenadores na tabela principal utilizando "primeiro_coordenador"
        cursor.execute(f"""
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
            WHERE d.chave IS NOT NULL AND d.data_atualizacao >= '{last_updated}'
        """)
        
        # Atualiza os IDs de coordenadores na tabela cursos
        cursor.execute("SET SQL_SAFE_UPDATES = 0")  
        cursor.execute(f"""
            UPDATE cursos c
            JOIN db_dpc_jira d ON c.nome_curso = d.curso AND c.entidade = d.entidade
            SET c.coordenador_id = d.coordenador_id
            WHERE d.coordenador_id IS NOT NULL AND d.data_atualizacao >= '{last_updated}'
        """)
        cursor.execute("SET SQL_SAFE_UPDATES = 1")  
        
        sql_client.commit()

        end_commit_time = time.time()
        print("***-*-*-*-*-*-*-*-*-******-*-*-*-*-*-*-*-*-***\n")
        print(f"Estrutura da tabela atualizada com sucesso!\nTempo gasto para atualizar: {end_commit_time - start_commit_time:.2f} segundos\n***-*-*-*-*-*-*-*-*-******-*-*-*-*-*-*-*-*-***")
        
    except Exception as e:
        sql_client.rollback()
        print(f"Erro ao atualizar estrutura da tabela: {str(e)}")
        raise e
    finally:
        cursor.close()

# Modificar a função main para incluir a atualização da estrutura
def main():    
    dados_jira = obter_dados_jira()

    if dados_jira:
        salvar_dados_mysql(dados_jira)
    else:
        print("Nenhum dado a ser salvo.")
    
    popular_atualizar_cursos_coordenadores()

if __name__ == "__main__":
    main()
    sql_client.close()
    with open("last_updated.txt", "w") as f:
        f.write(update_time)