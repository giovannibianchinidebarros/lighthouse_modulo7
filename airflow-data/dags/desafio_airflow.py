# ========================================================
# Imports:
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import pandas as pd
import sqlite3
import os


# ========================================================
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# ========================================================
# Caminho do diretório onde o DAG está localizado
dag_directory = os.path.dirname(os.path.abspath(__file__))
# Caminho para o diretório raiz (duas pastas acima do DAG)
root_directory = os.path.abspath(os.path.join(dag_directory, '../..'))
# Caminho para o banco de dados
db_path = os.path.join(root_directory, 'data', 'Northwind_small.sqlite')


# ========================================================
# Função para extrair dados da tabela 'Order'
def extract_orders():
    """
    Extrai dados da tabela 'Order' do banco de dados SQLite e exporta para um arquivo CSV.
    """
    try:
        # Estabelece uma conexão com o banco de dados SQLite
        # Consulta os dados da tabela Order e grave em um DataFrame Pandas
        with sqlite3.connect(db_path) as conn:
            query = "SELECT * FROM [Order];"
            df = pd.read_sql(query, conn)
        # Exporta o DataFrame para um arquivo CSV
        df.to_csv('output_orders.csv', index=False)
        print("Orders exported to output_orders.csv")

    except sqlite3.Error as sql_err:
        print(f"Database error: {sql_err}")
    except Exception as e:
        print(f"An error occurred: {e}")


# ========================================================
# Função para contar as quantidades vendidas para o Rio de Janeiro
def count_orders_in_rio():
    """
    Lê os dados do arquivo 'output_orders.csv' e faz o JOIN com a tabela 'OrderDetail'
    do banco de dados SQLite para calcular a quantidade total vendida (Quantity) para 
    a cidade do Rio de Janeiro. O resultado é salvo no arquivo 'count.txt'.
    """
    try:
        # Lê o arquivo CSV gerado pela tarefa anterior
        orders_df = pd.read_csv('output_orders.csv')
        # Renomeia a coluna 'Id' para 'OrderId' para garantir que o merge funcione corretamente
        orders_df = orders_df.rename(columns={'Id': 'OrderId'})
        # Conectando ao banco de dados para ler a tabela OrderDetail
        with sqlite3.connect(db_path) as conn:
            query = "SELECT * FROM OrderDetail;"
            order_detail_df = pd.read_sql(query, conn)
        # Fazendo o JOIN entre as duas tabelas usando 'OrderId'
        merged_df = pd.merge(orders_df, order_detail_df, on='OrderId')
        # Calculando a soma da quantidade onde o ShipCity é Rio de Janeiro
        total_quantity = merged_df[
            merged_df['ShipCity'] == 'Rio de Janeiro']['Quantity'].sum()
        # Exportando o resultado para count.txt
        with open('count.txt', 'w') as f:
            f.write(str(total_quantity))
        print("Total quantity sold to Rio de Janeiro written to count.txt")

    except FileNotFoundError as fnf_err:
        print(f"File error: {fnf_err}")
    except sqlite3.Error as sql_err:
        print(f"Database error: {sql_err}")
    except Exception as e:
        print(f"An error occurred: {e}")


# ========================================================
## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt", "w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##


# ========================================================
#
with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """

    extract_orders_task = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders,
    )

    count_orders_in_rio_task = PythonOperator(
        task_id='count_orders_in_rio',
        python_callable=count_orders_in_rio,
    )

    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

    extract_orders_task >> count_orders_in_rio_task >> export_final_output
