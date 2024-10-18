import streamlit as st
import altair as alt
import streamlit_option_menu
from streamlit_autorefresh import st_autorefresh
from streamlit_option_menu import option_menu
from utils import load_config, get_db_connection
import time
import pandas as pd
from kafka import KafkaConsumer
import simplejson as json

@st.cache_data
def fetch_voting_stats():
    config = load_config('config.json')
    try:
        conn, cur = get_db_connection(config['database'])
        # Fetch total number of voters
        cur.execute("""SELECT count(*) voters_count FROM voters""")
        voters_count = cur.fetchone()[0]
        # Fetch total number of candidates
        cur.execute("""SELECT count(*) candidates_count FROM candidates""")
        candidates_count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return voters_count, candidates_count     
    except Exception as exp:
        print(f"An error occurred: {exp}")


# Function to create a Kafka consumer
def create_kafka_consumer(topic_name):
    # Set up a Kafka consumer with specified topic and configurations
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

# Function to fetch data from Kafka
def fetch_data_from_kafka(consumer):
    # Poll Kafka consumer for messages within a timeout period
    messages = consumer.poll(timeout_ms=1000)
    data = []

    # Extract data from received messages
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return pd.DataFrame(data)


def streamlit_home(candidates_results, location_results):
    # Create a row layout
    c01, c02 = st.columns(2)
    voters_count, candidates_count = fetch_voting_stats()
    c01.metric("Total Voters", voters_count)
    c02.metric("Total Candidates", candidates_count) 
    st.divider()

    results = candidates_results.groupby('party_affiliation').agg(total_votes=('total_votes', 'sum')).reset_index()
    
    c1 = st.container()
    with c1:
        chart = alt.Chart(results).mark_bar().encode(
            x='party_affiliation',
            y=alt.Y('total_votes', scale=alt.Scale(domain=[0, results['total_votes'].max()])),
            color=alt.Color('party_affiliation', legend=None)  # This assigns a unique color to each party
        )
        st.altair_chart(chart, use_container_width=True) 
    
    location_results = location_results.loc[location_results.groupby('state')['count'].idxmax()]
    c2 = st.container()
    with c2:
        chart = alt.Chart(location_results).mark_bar().encode(
            x='state',
            y=alt.Y('count', scale=alt.Scale(domain=[0, results['total_votes'].max()])),
            color=alt.Color('state')  # This assigns a unique color to each party
        )
        st.altair_chart(chart, use_container_width=True)
                 

def streamlit_leaderboard(results):    
    # Identify the leading candidate
    results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    leading_candidate = results.loc[results['total_votes'].idxmax()]

    # Display leading candidate information
    st.subheader('Leading Candidate')
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=200)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party_affiliation'])
        st.subheader("Total Votes: {}".format(leading_candidate['total_votes']))
    
    st.divider()     
    
    results = results.sort_values(by='total_votes', ascending=False).reset_index(drop=True)    
    if not results.empty:
        st.subheader('All Candidates Details')
        st.write(" ")
        num_candidates = len(results)        
        for i in range(0, num_candidates, 3):
            cols = st.columns(3)
            for j in range(3):
                index = i + j
                if index < num_candidates:
                    with cols[j]:
                        # Display the candidate's photo
                        st.image(results['photo_url'].iloc[index])
                        st.write(f"**Name:** {results['candidate_name'].iloc[index]}")
                        st.write(f"**Party:** {results['party_affiliation'].iloc[index]}")
                        st.write(f"**Total Votes:** {results['total_votes'].iloc[index]}")
                        st.divider()
    else:
        st.write("No candidates available.")

if __name__ == '__main__':
    
    # Header
    st.header('Realtime Election Dashboard')    
    st.markdown("""---""")
    
    # Sidebar
    with st.sidebar:
        selected = option_menu(
        menu_title = "Main Menu",
        options = ["Home", "Leaderboard", "Contact Us"],
        icons = ["house", "trophy", "envelope"],
        menu_icon = "cast",
        default_index = 0,
        )
        
        st.markdown("""---""")
        if st.session_state.get('last_update') is None:
            st.session_state['last_update'] = time.time()
        
        # Slider to control refresh interval
        refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
        st_autorefresh(interval=refresh_interval * 1000, key="auto")
        last_refresh = st.empty()
        last_refresh.text(f"Last refreshed at:\n{time.strftime('%Y-%m-%d %H:%M:%S')}")
        st.markdown("""---""")
        
    candidates_consumer = create_kafka_consumer("aggregated_votes_per_candidate")
    candidates_results = fetch_data_from_kafka(candidates_consumer)
    
    location_consumer = create_kafka_consumer("aggregated_turnout_by_location")
    location_results = fetch_data_from_kafka(location_consumer)
    
    # Menubar
    if selected == "Home":
        streamlit_home(candidates_results, location_results)
    elif selected == "Leaderboard":
        streamlit_leaderboard(candidates_results)