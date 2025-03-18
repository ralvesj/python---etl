import os
import glob
from dotenv import load_dotenv
import psycopg2

#Config Gerais

pasta = '../dados/'
arquivos = glob.glob(os.path.join(pasta, "*.csv"))
bd_bf = '../dados/dados_bf.csv'

load_dotenv()

# Informações de conexão do banco de dados PostgreSQL
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

#1 checar se pasta existe

if not os.path.exists(pasta):
    print(f"Pasta '{pasta}' não encontrada. Verifique o caminho.")    
    exit(1) 
    
print("Pasta encontrada -- OK")

#2 checar se existem arquivos na pasta

if not arquivos:
    print(f"Não há arquivos CSV na pasta '{pasta}'.")
    exit(1)

print(f"Arquivos CSV encontrados: {arquivos} -- OK ")

#3 checar conexão com BD

def test_postgres_connection():
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password
        )
        
        # Se a conexão for bem-sucedida, retornar uma mensagem
        print("Conexão bem-sucedida com o banco de dados -- OK")

        # Fechar a conexão
        conn.close()
    
    except Exception as e:
        print(f"Erro ao tentar conectar ao banco de dados: {e}")

#4 Testar a conexão
test_postgres_connection()



