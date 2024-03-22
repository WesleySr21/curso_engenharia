from datetime import datetime, timedelta #Função para configuração na dag
from airflow.decorators import dag, task 
from airflow.providers.postgres.hooks.postgres import PostgresHook #para referenciar o provider do postgress
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
 

#Dicionario de paramentos par dag 
default_args = {
    'owner': 'airflow', #proprietario da dag
    'depends_on_past': False, #caso não rode aldo passado não vai ter dependencia 
    'start_date': datetime(2024, 1, 1), #data inicial
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, #numero de tentativas
    'retry_delay': timedelta(minutes=1) #tempo para tentar de novo se retry for diferente de 0
}

#uso do decorator dag 
@dag(
    dag_id='postgres_to_snowflake', #idenficador da dag, nome da dag
    default_args=default_args,
    description='Load data incrementally from Postgres to Snowflake',
    schedule_interval=timedelta(days=1),#de quanto rm quanto tempo essa dag vai rodar
    catchup=False #define se ele vai executar todas as opções que ele deixou para tras
)

#Criar a função
def postgres_to_snowflake_etl():
    table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'clientes', 'vendas']
 
    #Loop para acessar as tabelas
    for table_name in table_names:
        @task(task_id=f'get_max_id_{table_name}') #Definir o nome da task
        def get_max_primary_key(table_name: str): #pegar o valor maximo da chave da tabela
            with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as conn: #implementar a função com hook, que é um conector do snowflake
                with conn.cursor() as cursor: #abrir o cursos
                    cursor.execute(f"SELECT MAX(ID_{table_name}) FROM {table_name}") #comando do SQL 
                    max_id = cursor.fetchone()[0] #salva o resultado, uso de fetchone porque o retorno é de apenas um registro
                    return max_id if max_id is not None else 0 #confirmar se não é nulo
 
 