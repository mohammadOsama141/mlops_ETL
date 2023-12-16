from bs4 import BeautifulSoup
import requests
import os
import json


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess
from datetime import datetime, timedelta


#extraction :text
def scrape_wikipedia_did_you_know():
    url = "https://en.wikipedia.org/wiki/Main_Page"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    dyk_section = soup.find('div', id='mp-dyk')
    list_elements = dyk_section.find_all('li')
    did_you_know = [li.text for li in list_elements if li.text not in ['Archive', 'Start a new article', 'Nominate an article']]
    did_you_know = [sentence.replace('(pictured)', '').strip(' ?').capitalize() for sentence in did_you_know]
    return did_you_know


#transform data and save in a json file
def convert_and_save_to_json(ti):
    extracted_data = ti.xcom_pull(task_ids='extract')
    transformed_data = json.dumps(extracted_data)
    ti.xcom_push(key='transformed_data', value=transformed_data)

def persist_json_data(ti):
    data = ti.xcom_pull(key='transformed_data', task_ids='transform')
    with open('/mnt/d/airflow/data/modified_data.json', 'w') as file:
        file.write(data)

#DVC
def version_and_push_data():
    os.chdir('/mnt/d/airflow/data')
    subprocess.run(['dvc', 'add', 'modified_data.json'])
    subprocess.run(['git', 'add', '.'])
    subprocess.run(['git', 'commit', '-m', 'Update modified data'])
    subprocess.run(['dvc', 'push'])
    subprocess.run(['git', 'push'])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'modified_etl_dag',
    default_args=default_args,
    description='A different ETL DAG structure',
    schedule_interval=timedelta(days=1),
    catchup=False
)

#E : extraction
scrape_task = PythonOperator(
    task_id='extract',
    python_callable=scrape_wikipedia_did_you_know,
    dag=dag,
)

#T : Transform
convert_task = PythonOperator(
    task_id='transform',
    python_callable=convert_and_save_to_json,
    dag=dag,
)

#L : load
save_task = PythonOperator(
    task_id='load',
    python_callable=persist_json_data,
    dag=dag,
)

version_task = PythonOperator(
    task_id='version_data',
    python_callable=version_and_push_data,
    dag=dag,
)

# Set task dependencies
scrape_task >> convert_task >> save_task >> version_task

