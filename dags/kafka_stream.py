from datetime import datetime
from airflow import DAG
# PERBAIKAN 1: Menggunakan jalur import baru sesuai rekomendasi Airflow terbaru
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00) 
}

def get_data():
    import requests

    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res

def format_data(res):
    data = {}
    data['id'] = res['login']['uuid']
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():    
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            data = format_data(res)
            producer.send('users_created', json.dumps(data).encode('utf-8'))
        except Exception as e:
            logging.error(f'Error: {e}')
            continue

# --- BAGIAN DAG AIRFLOW ---
# Jika ingin dijalankan lewat scheduler Airflow, hilangkan tanda pagar (#) di bawah ini:

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        # PERBAIKAN 2: Mengubah dari string 'stream_data' menjadi objek fungsi stream_data
        python_callable=stream_data 
    )


# Jalankan langsung secara lokal jika dieksekusi via terminal:
# if __name__ == "__main__":
#     stream_data()