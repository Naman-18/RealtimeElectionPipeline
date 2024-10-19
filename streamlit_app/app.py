import streamlit as st
from streamlit_autorefresh import st_autorefresh
from streamlit_option_menu import option_menu
from app_utils import fetch_voting_stats, create_kafka_consumer, fetch_data_from_kafka
from charts import display_party_chart, display_location_chart, display_gender_chart
import time

def streamlit_home(parties_results, location_results, gender_results):
    c01, c02 = st.columns(2)
    voters_count, candidates_count = fetch_voting_stats()
    c01.metric("Total Voters", voters_count)
    c02.metric("Total Candidates", candidates_count)
    st.divider()    
    
    c1 = st.container()
    with c1:
        parties_results = parties_results.loc[parties_results.groupby('party_affiliation')['total_votes'].idxmax()]
        display_party_chart(parties_results)
    
    st.divider()
    c21, c22 = st.columns(2)
    with c21:
        location_results = location_results.loc[location_results.groupby('address_state')['total_votes'].idxmax()]
        display_location_chart(location_results)
    
    with c22:
        gender_results = gender_results.loc[gender_results.groupby('gender')['total_votes'].idxmax()]
        display_gender_chart(gender_results)
        
    
def display_all_candidates(results):
    num_candidates = len(results)
    for i in range(0, num_candidates, 3):
        cols = st.columns(3)
        for j in range(3):
            index = i + j
            if index < num_candidates:
                with cols[j]:
                    st.image(results['photo_url'].iloc[index])
                    st.write(f"**Name:** {results['candidate_name'].iloc[index]}")
                    st.write(f"**Party:** {results['party_affiliation'].iloc[index]}")
                    st.write(f"**Total Votes:** {results['total_votes'].iloc[index]}")
                    st.divider()

def streamlit_leaderboard(results):
    results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    leading_candidate = results.loc[results['total_votes'].idxmax()]

    st.subheader('Leading Candidate')
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=200)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party_affiliation'])
        st.subheader(f"Total Votes: {leading_candidate['total_votes']}")
    st.divider()
    results = results.sort_values(by='total_votes', ascending=False).reset_index(drop=True)
    if not results.empty:
        st.subheader('All Candidates Details')
        display_all_candidates(results)


if __name__ == '__main__':
    st.header('Realtime Election Dashboard')    
    st.markdown("""---""")
    
    with st.sidebar:
        selected = option_menu(
            menu_title="Main Menu",
            options=["Home", "Leaderboard", "Contact Us"],
            icons=["house", "trophy", "envelope"],
            menu_icon="cast",
            default_index=0,
        )
        refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
        st_autorefresh(interval=refresh_interval * 1000, key="auto")
        last_refresh = st.empty()
        last_refresh.text(f"Last refreshed at:\n{time.strftime('%Y-%m-%d %H:%M:%S')}")
            
    candidates_consumer = create_kafka_consumer("aggregated_votes_per_candidate")
    candidates_results = fetch_data_from_kafka(candidates_consumer)

    location_consumer = create_kafka_consumer("aggregated_turnout_by_location")
    location_results = fetch_data_from_kafka(location_consumer)
    
    parties_consumer = create_kafka_consumer("aggregated_votes_per_party")
    parties_results = fetch_data_from_kafka(parties_consumer)
    
    gender_consumer = create_kafka_consumer("aggregated_turnout_by_gender")
    gender_results = fetch_data_from_kafka(gender_consumer)

    if selected == "Home":
        streamlit_home(parties_results, location_results, gender_results)
    elif selected == "Leaderboard":
        streamlit_leaderboard(candidates_results)
