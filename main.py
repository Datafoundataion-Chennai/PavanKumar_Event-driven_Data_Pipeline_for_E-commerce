import streamlit as st
import pandas as pd
import time
import plotly.express as px
from google.cloud import bigquery
from loggingModule import logger
# from RealEventGeneration import EVENT_TYPES
import ConnectBigQuery as BQ

EVENT_TYPES = [
    "page_view", "search_product", "view_product", "add_to_cart",
    "remove_from_cart", "wishlist_add", "wishlist_remove", "apply_coupon",
    "checkout", "purchase", "login", "logout"
]
def fetch_realtime_events():
    """Fetches real-time event data from BigQuery."""
    client = bigquery.Client()
    query = f"""
        SELECT event_id, user_id, event_type, product_id, price, timestamp
        FROM `{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.events`
        ORDER BY timestamp DESC
        LIMIT 1000
    """
    df = client.query(query).to_dataframe()
    return df

def main():
    # Streamlit App UI
    st.set_page_config(layout="wide")
    st.title("Event-driven Data Pipeline for E-commerce")
    logger.info("Streamlit app started.")



    # Initialize session state for view selection if not already set
    if "view_option" not in st.session_state:
        st.session_state.view_option = "Overview"

    # Outcome Button
    if st.button("📈 Project Outcome & Learnings"):
        st.info("Learnings from this project include real-time event processing, sessionization for customer journey analysis, and building data marts for e-commerce metrics like shopping funnels, product performance, and conversion rates. The project also involves dimensional modeling in BigQuery, optimizing query performance for event-driven analytics, and creating a Streamlit dashboard for conversion tracking and e-commerce insights.")

    # # Buttons to switch between views
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if st.button("📊 Overview"):
            st.session_state.button_clicked = "📊 Overview selected!"
    
    with col2:
        if st.button("📦 Order Summary"):
            st.session_state.button_clicked = "📦 Order Summary selected!"
    
    with col3:
        if st.button("📈 Event Analytics"):
            st.session_state.button_clicked = "📈 Event Analytics selected!"
    
    col4, = st.columns(1)
    with col4:
        if st.button("⚡ Live Event Feed"):
            st.session_state.button_clicked = "⚡ Live Event Feed selected!"
    
    # Display clicked button info
    # if st.session_state.button_clicked:
    #     st.write(st.session_state.button_clicked)
        
    if st.session_state.view_option == "Overview":
        st.header("📊 Overview")
        st.info("Select a section above to view detailed insights.")
    elif st.session_state.view_option == "Order Summary":
        st.header("📦 Order Summary")
        st.warning("Order summary data to be implemented.")
    elif st.session_state.view_option == "Event Analytics":
        st.header("📈 Event Analytics")
        st.warning("Event analytics data to be implemented.")
    elif st.session_state.view_option == "Live Event Feed":
        st.header("⚡ Live Event Feed (Last 10 Minutes)")
        st.success("Real-time event data will be displayed here.")
        df = fetch_realtime_events()
        
        # Interactive Metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Events", len(df))
        col2.metric("Unique Users", df['user_id'].nunique())
        col3.metric("Total Revenue", f"${df['price'].sum():,.2f}")

        # Visual Representations
        fig1 = px.bar(df.groupby('event_type').size().reset_index(name='count'), x='event_type', y='count', title='Event Type Distribution', color='event_type')
        fig2 = px.pie(df, names='event_type', title='Event Type Proportions')
        fig3 = px.line(df.sort_values('timestamp'), x='timestamp', y='price', title='Revenue Over Time')

        st.plotly_chart(fig1, use_container_width=True)
        st.plotly_chart(fig2, use_container_width=True)
        st.plotly_chart(fig3, use_container_width=True)

        # Top Filters
        col1, col2, col3 = st.columns([1, 1, 1])

        event_type_filter = col1.multiselect("Event Type", EVENT_TYPES)
        user_id_filter = col2.text_input("User ID")
        product_id_filter = col3.text_input("Product ID")
        
        filter_checkbox = st.checkbox("Apply Filters")
        
        if filter_checkbox:
            # Apply filters
            if event_type_filter:
                df = df[df['event_type'].isin(event_type_filter)]
            if user_id_filter:
                df = df[df['user_id'].str.contains(user_id_filter, na=False)]
            if product_id_filter:
                df = df[df['product_id'].str.contains(product_id_filter, na=False)]

        # Display DataFrame
        st.dataframe(df, height=500)

        # Auto-refresh
        if st.button("🔄 Refresh Data"):
            st.rerun()

        # Auto-refresh every 10 seconds
        time.sleep(10)
        st.rerun()
    # Display selected view
    if st.session_state.view_option == "Overview":
        st.header("📊 Overview")
        st.info("Select a section above to view detailed insights.")

    elif st.session_state.view_option == "Order Summary":
        st.header("📦 Order Summary")
        st.warning("Order summary data to be implemented.")

    elif st.session_state.view_option == "Event Analytics":
        st.header("📈 Event Analytics")
        st.warning("Event analytics data to be implemented.")

    elif st.session_state.view_option == "Live Event Feed":
        st.header("⚡ Live Event Feed (Last 10 Minutes)")
        df = fetch_realtime_events()
        
        # Interactive Metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Events", len(df))
        col2.metric("Unique Users", df['user_id'].nunique())
        col3.metric("Total Revenue", f"${df['price'].sum():,.2f}")

        # Visual Representations
        fig1 = px.bar(df.groupby('event_type').size().reset_index(name='count'), x='event_type', y='count', title='Event Type Distribution', color='event_type')
        fig2 = px.pie(df, names='event_type', title='Event Type Proportions')
        fig3 = px.line(df.sort_values('timestamp'), x='timestamp', y='price', title='Revenue Over Time')

        st.plotly_chart(fig1, use_container_width=True)
        st.plotly_chart(fig2, use_container_width=True)
        st.plotly_chart(fig3, use_container_width=True)

        # Top Filters
        col1, col2, col3 = st.columns([1, 1, 1])

        event_type_filter = col1.multiselect("Event Type", EVENT_TYPES)
        user_id_filter = col2.text_input("User ID")
        product_id_filter = col3.text_input("Product ID")
        
        filter_checkbox = st.checkbox("Apply Filters")
        
        if filter_checkbox:
            # Apply filters
            if event_type_filter:
                df = df[df['event_type'].isin(event_type_filter)]
            if user_id_filter:
                df = df[df['user_id'].str.contains(user_id_filter, na=False)]
            if product_id_filter:
                df = df[df['product_id'].str.contains(product_id_filter, na=False)]

        # Display DataFrame
        st.dataframe(df, height=500)

        # Auto-refresh
        if st.button("🔄 Refresh Data"):
            st.rerun()

        # Auto-refresh every 10 seconds
        time.sleep(10)
        st.rerun()
    
    
if __name__ == "__main__":
    main()        
