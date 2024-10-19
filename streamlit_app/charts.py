import altair as alt
import streamlit as st

def display_party_chart(results):
    chart = alt.Chart(results).mark_bar().encode(
        x='party_affiliation',
        y=alt.Y('total_votes', scale=alt.Scale(domain=[0, results['total_votes'].max()])),
        color=alt.Color('party_affiliation', legend=None),
    )
    st.altair_chart(chart, use_container_width=True)


def display_location_chart(results):
    # Pie chart for location results
    total_votes = results['total_votes'].sum()
    results['percentage'] = (results['total_votes'] / total_votes) * 100
    chart = alt.Chart(results).mark_arc().encode(
        theta=alt.Theta(field="total_votes", type="quantitative"),
        color=alt.Color(field="address_state", type="nominal", legend=None),
        tooltip=['address_state', 'total_votes', 'percentage']
    ).properties(
        title = "Distribution by State"
    )
    st.altair_chart(chart, use_container_width=True)

def display_gender_chart(results):
    # Calculate total votes for percentage calculation
    total_votes = results['total_votes'].sum()
    results['percentage'] = (results['total_votes'] / total_votes) * 100
    chart = alt.Chart(results).mark_arc().encode(
        theta=alt.Theta(field="total_votes", type="quantitative"),
        color=alt.Color(field="gender", type="nominal"),
        tooltip=['gender', 'total_votes', 'percentage']  # Include percentage in tooltip
    ).properties(
        title="Distribution by Gender"
    )
    st.altair_chart(chart, use_container_width=True)