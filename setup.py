import os
import json
import psycopg2
import requests
from utils import load_config, get_db_connection

def create_tables(conn, cur, tables):
    '''
    Function to create database tables from SQL files
    '''
    for table in tables:
        query_file_path = os.path.join("queries", f"{table}.sql")
        try:
            with open(query_file_path, 'r') as file:
                query = file.read()
            cur.execute(query)
        except Exception as exp:
            print(f"Error creating table {table}: {exp}")
            raise
    conn.commit()

def generate_candidate_row(user_data, party_affiliation):
    return {
            "candidate_id": user_data['login']['uuid'],
            "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "party_affiliation": party_affiliation,
            "date_of_birth": user_data['dob']['date'],
            "biography": "A brief bio of the candidate.",
            "campaign_platform": "Key campaign promises or platform.",
            "photo_url": user_data['picture']['large']
        }

def generate_candidate_data(conn, cur, base_url, parties, total_candidates):
    for i in range(total_candidates):
        response = requests.get(base_url + '&gender=' + ('female' if total_candidates % 3 == 1 else 'male'))
        if response.status_code == 200:
            user_data = response.json()['results'][0]
            party_affiliation = parties[i % len(parties)]
            candidate = generate_candidate_row(user_data, party_affiliation)
            query = open('queries/candidates_insert.sql', 'r').read()
            params = candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'], candidate['date_of_birth'], candidate['biography'], candidate['campaign_platform'], candidate['photo_url'] 
            try:
                cur.execute(query, params)
            except Exception as exp:
                print(f"Error inserting in table candiates: {exp}")
            conn.commit()
        else:
            return "Error fetching data"

def generate_voters_row(user_data):
    return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }

def generate_voters_data(conn, cur, base_url, total_voters):
    for i in range(total_voters):
        response = requests.get(base_url + '&gender=' + ('female' if total_voters % 3 == 1 else 'male'))
        if response.status_code == 200:
            user_data = response.json()['results'][0]
            voter = generate_voters_row(user_data)
            query = open('queries/voters_insert.sql', 'r').read()
            params = (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
            voter['nationality'], voter['registration_number'], voter['address']['street'],
            voter['address']['city'], voter['address']['state'], voter['address']['country'],
            voter['address']['postcode'], voter['email'], voter['phone_number'],
            voter['cell_number'], voter['picture'], voter['registered_age'])
            try:
                cur.execute(query, params)
            except Exception as exp:
                print(f"Error inserting in table voters: {exp}")
            conn.commit()
        else:
            return "Error fetching data"


def setup_system(conn, cur, config):
    # 1. Create tables
    print("Setup tables...")
    create_tables(conn, cur, config['tables'])

    # 2. Generate candidates data
    print("Setup candidates...")
    generate_candidate_data(conn, cur, config['randomuser_url'], config['parties'], config['total_candidates'])

    # 3. Generate voters data
    print('Setup voters ...')
    generate_voters_data(conn, cur, config['randomuser_url'], config['total_voters'])
    
    print('Setup completed')

def main():
    config = load_config('config.json')
    
    # Connect to Postgres
    try:
        conn, cur = get_db_connection(config['database'])
        setup_system(conn, cur, config)
        cur.close()
        conn.close()
    except Exception as exp:
        print(f"An error occurred: {exp}")

if __name__ == '__main__':
    main()
