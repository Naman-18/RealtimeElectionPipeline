from kafka import KafkaConsumer
import simplejson as json
import pandas as pd
import psycopg2

def load_config(config_path):
    '''
    Function to load configuration from a JSON file
    '''
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config

def get_db_connection(database_credentials):
    '''
    Function to establish a connection to the PostgreSQL database
    '''
    try:
        DB_CONN_STRING = "dbname='{db_name}' port='{port}' user='{user}' password='{password}' host='{host}'".format(
            db_name=database_credentials['db_name'],
            host=database_credentials['host'],
            port=database_credentials['port'],
            user=database_credentials['username'],
            password=database_credentials['password']
        )
        conn = psycopg2.connect(DB_CONN_STRING)
        cur = conn.cursor()
        return conn, cur
    except Exception as exp:
        print(f"Error connecting to the database: {exp}")
        return

def fetch_voting_stats():
    config = load_config('config.json')
    try:
        conn, cur = get_db_connection(config['database'])
        cur.execute("""SELECT count(*) voters_count FROM voters""")
        voters_count = cur.fetchone()[0]
        cur.execute("""SELECT count(*) candidates_count FROM candidates""")
        candidates_count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return voters_count, candidates_count
    except Exception as exp:
        print(f"An error occurred: {exp}")

def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = [sub_message.value for message in messages.values() for sub_message in message]
    return pd.DataFrame(data)

