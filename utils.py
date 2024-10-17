import json
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