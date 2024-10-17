import random
from datetime import datetime
import time
from utils import load_config, get_db_connection, kafka_delivery_report
from confluent_kafka import SerializingProducer
import json

producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})

def get_candidates(conn, cur):
    candidates_query = cur.execute("""SELECT row_to_json(t) FROM ( SELECT candidate_id FROM candidates) t;""")
    candidates = cur.fetchall()
    conn.commit()
    candidates = [candidate[0] for candidate in candidates]
    if len(candidates) == 0:
        raise Exception("No candidates found in database")
    else:
        return candidates

def get_voters(conn, cur):
    voters_query = cur.execute("""SELECT row_to_json(t) FROM ( SELECT voter_id FROM voters) t;""")
    voters = cur.fetchall()
    conn.commit()
    voters = [voter[0] for voter in voters]
    if len(voters) == 0:
        raise Exception("No voters found in database")
    else:
        return voters

def start_voting_app(conn, cur, config, candidates, voters):
    for voter in voters:
        chosen_candidate = random.choice(candidates)
        vote = voter | chosen_candidate | {"voting_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "vote": 1}
        print(vote)
        query = open('queries/votes_insert.sql', 'r').read()
        try:
            cur.execute(query, (vote['voter_id'], vote['candidate_id'], vote['voting_time'], vote['vote']))
            conn.commit()
            producer.produce(
                config['kafka_topics']['votes_topic'],
                key=vote["voter_id"],
                value=json.dumps(vote),
                on_delivery=kafka_delivery_report
                )
            producer.poll(0)
        except Exception as exp:
            print(f"Error: {exp}")
        time.sleep(config['voting_interval'])


if __name__ == '__main__':
    config = load_config('config.json')
    try:
        conn, cur = get_db_connection(config['database'])
        candidates = get_candidates(conn, cur)
        voters = get_voters(conn, cur)
        start_voting_app(conn, cur, config, candidates, voters)
        cur.close()
        conn.close()
    except Exception as exp:
        print(f"An error occurred: {exp}")
