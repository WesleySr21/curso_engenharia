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

        #SEGUNDA TASK
        @task(task_id=f'load_data_{table_name}') #Carregar a tabela que subimos
        def load_incremental_data(table_name: str, max_id: int): #função 
            with PostgresHook(postgres_conn_id='postgres').get_conn() as pg_conn: #conexão com o banco postgress
                with pg_conn.cursor() as pg_cursor: #inicia o cursor
                    #construir a função de forma dinamica
                    primary_key = f'ID_{table_name}' 
                    #executar a query 
                    pg_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
                    #Obetendo o resultado das colunas
                    columns = [row[0] for row in pg_cursor.fetchall()] #loop de zero até onde tiver colunas
                    columns_list_str = ', '.join(columns) #concatenando com virgula para separar as colunas do resultado 
                    placeholders = ', '.join(['%s'] * len(columns)) #onde o valor deve ser inserido de acordo com o tamanho da coluna
                    
                    #Lecture thumbnail
                    
                    #selec na lista de colunas 
                    pg_cursor.execute(f"SELECT {columns_list_str} FROM {table_name} WHERE {primary_key} > {max_id}")
                    
                    #Lecture thumbnail
                    
                    #armazenar os resultados
                    rows = pg_cursor.fetchall()
                    
                    #Conectar de novo com SnowFlake
                    with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as sf_conn:
                        with sf_conn.cursor() as sf_cursor: #cursor para acesso
                            #função de inserção
                            insert_query = f"INSERT INTO {table_name} ({columns_list_str}) VALUES ({placeholders})"
                            #for de inserção
                            for row in rows:
                                sf_cursor.execute(insert_query, row) #inserção "final"
 
        max_id = get_max_primary_key(table_name) #para chamar a task
        load_incremental_data(table_name, max_id) #chama a segunda task
 
postgres_to_snowflake_etl_dag = postgres_to_snowflake_etl() #chamar a função
  