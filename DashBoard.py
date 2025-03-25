import streamlit as st
# import ConnectBigQuery as BQ
from google.cloud import bigquery
import pandas as pd
import time
import plotly.express as px
import Queries as Q
client = bigquery.Client()
from loggingModule import logger
import archivedFiles.create as c

EVENT_TYPES = [
    "page_view", "search_product", "view_product", "add_to_cart",
    "remove_from_cart", "wishlist_add", "wishlist_remove", "apply_coupon",
    "checkout", "purchase", "login", "logout"
]

@st.cache_data
def fetch_batch_data(query):
    return client.query(query).to_dataframe()

def fetch_realtime_events(i):
    query = f"""
    SELECT event_id EventId, user_id UserId, REPLACE(INITCAP(event_type),"_","") as EventType, product_id ProductId, price as Price, timestamp as TimeStamp 
    FROM {Q.EVENT_TABLE}
    WHERE PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TimeStamp) 
    >= TIMESTAMP_SUB(TIMESTAMP(DATETIME(CURRENT_TIMESTAMP(), "Asia/Kolkata")), INTERVAL {int(i)} MINUTE)
    ORDER BY TimeStamp DESC;
    """
    return client.query(query).to_dataframe()
def fetch_all_events():
    query = Q.REAL_EVENTS
    return client.query(query).to_dataframe()



def main(role):
    st.markdown("""
    <style>
        .title-banner {
            background: linear-gradient(to right, #00b4d8, #90e0ef);
            padding: 20px;
            text-align: center;
            color: white;
            font-size: 32px;
            font-weight: bold;
            border-radius: 10px;
        }
    </style>
    <div class="title-banner">
        Event-Driven Data Pipeline for E-commerce
    </div>
    """, unsafe_allow_html=True)
    st.markdown("")
    col1, col2, col3, col4,col5 = st.columns(5)
    if 'view_option' not in st.session_state:
        st.session_state.view_option = None
    
    with col1:
        if st.button("📊 Overview"):
            st.session_state.view_option = "Overview"
    
    with col2:
        if st.button("📦 Order Summary"):
            st.session_state.view_option = "Order Summary"
    
    with col3:
        if st.button("📈 Event Analytics"):
            st.session_state.view_option = "Event Analytics"
    
    with col4:
        if st.button("⚡ Live Event Feed"):
            st.session_state.view_option = "Live Event Feed"
    with col5:
        if role ==1:
            if st.button("Create New Table"):
                st.session_state.view_option = "Create New Table"
    
    if st.session_state.view_option == "Overview":
        st.header("Overview")
        logger.info("Overview page Viewed")
        kpi_data = fetch_batch_data(Q.ALLSTATS).iloc[0]
        kpi_data["total_orders"] = int(kpi_data["total_orders"])
        kpi_data["total_Revenue"] = float(kpi_data["total_Revenue"])  
        kpi_data["active_customers"] = int(kpi_data["active_customers"])
        kpi_data["total_products_sold"] = int(kpi_data["total_products_sold"])
        kpi_data["avg_order_value"] = float(kpi_data["avg_order_value"])
        def animate_metric(start, end, unit="", delay=0.02):
            end = int(end)
            placeholder = st.empty()
            for value in range(start, end + 1, max(1, (end - start) // 30)): 
                placeholder.markdown(f'<div class="kpi-value">{unit}{value:,}</div>', unsafe_allow_html=True)
                time.sleep(delay)
            placeholder.markdown(f'<div class="kpi-value">{unit}{end:,}</div>', unsafe_allow_html=True)

        st.markdown(
            """
            <style>
                .kpi-box {
                    width: 90%;
                    height: 120px;
                    border-radius: 15px;
                    padding: 20px;
                    text-align: center;
                    font-weight: bold;
                    font-size: 22px;
                    color: white;
                    display: flex;
                    flex-direction: column;
                    justify-content: center;
                    align-items: center;
                    margin: 10px;
                }
                .kpi-value {
                    font-size: 32px;
                    font-weight: bold;
                    margin-top: 5px;
                    text-align: center;
                }
                .gradient1 { background: linear-gradient(135deg, #FF6B6B, #FFA07A); }
                .gradient2 { background: linear-gradient(135deg, #6BFFB8, #36CFC9); }
                .gradient3 { background: linear-gradient(135deg, #6B83FF, #36A2FF); }
                .gradient4 { background: linear-gradient(135deg, #FFC36B, #FFA500); }
                .gradient5 { background: linear-gradient(135deg, #D36BFF, #A536FF); }
            </style>
            """,
            unsafe_allow_html=True,
        )
        col1, col2, col3,col4,col5 = st.columns(5)

        with col1:
            st.markdown('<div class="kpi-box gradient1">Total Order</div>', unsafe_allow_html=True)
            animate_metric(0, kpi_data["total_orders"])

        with col2:
            st.markdown('<div class="kpi-box gradient2">Total Revenue</div>', unsafe_allow_html=True)
            animate_metric(0, round(kpi_data["total_Revenue"], 3), unit="Rs.")  

        with col3:
            st.markdown('<div class="kpi-box gradient3">Active Customers</div>', unsafe_allow_html=True)
            animate_metric(0, kpi_data["active_customers"])

        with col4:
            st.markdown('<div class="kpi-box gradient4">Total Products Sold</div>', unsafe_allow_html=True)
            animate_metric(0, kpi_data["total_products_sold"])

        with col5:
            st.markdown('<div class="kpi-box gradient5">Avg Order Value</div>', unsafe_allow_html=True)
            animate_metric(0, round(kpi_data["avg_order_value"], 2), unit="Rs.")  
            
        st.markdown("<br><br>", unsafe_allow_html=True)
        customer_behavior_data = fetch_batch_data(Q.customer_behavior_query).iloc[0]
        review_data = fetch_batch_data(Q.review_query).iloc[0]

        col6, col7 = st.columns([1,1])
        with col6:
            returning_ratio = (customer_behavior_data['returning_customers'] / customer_behavior_data['total_customers']) * 100
            st.metric("Returning vs. New Customers Ratio", f"{returning_ratio:.2f}% Returning Customers")
        with col7:
            st.metric("Average Customer Rating", f"{review_data['avg_rating']:.2f}/5")
        
        st.markdown("<br><br>", unsafe_allow_html=True)
        
        tab1, tab2 = st.tabs(["📈 Chart", "🗃 Tables"])
        with tab1:
            tab1.subheader("All ChartS")
            top_products_data = fetch_batch_data(Q.TOP_PRODUCTS)
            worst_products_data = fetch_batch_data(Q.WORST_PRODUCTS)

            col1, col2 = st.columns(2)

            with col1:
                st.subheader("📈 Top 10 Product Categories by Revenue")
                fig_top_products = px.bar(
                    top_products_data, 
                    x="ProductCategory", 
                    y="Revenue", 
                    title="Top 10 Product Categories",
                    color="Revenue",
                    color_continuous_scale="Blues",
                )
                st.plotly_chart(fig_top_products, use_container_width=True)

            with col2:
                st.subheader("📉 Worst 10 Product Categories by Revenue")
                fig_worst_products = px.bar(
                    worst_products_data, 
                    x="ProductCategory", 
                    y="Revenue", 
                    title="Worst 10 Product Categories",
                    color="Revenue",
                    color_continuous_scale="Reds",
                )
                st.plotly_chart(fig_worst_products, use_container_width=True) 
            seller_Revenue_data = fetch_batch_data(Q.seller_Revenue_query)
            fig_sellers = px.bar(seller_Revenue_data, x="SellerID",
            y="Revenue",
            title="Top 10 Sellers by Revenue",
            color="Revenue",
            color_continuous_scale="Plasma",
            animation_frame="date" if "date" in seller_Revenue_data.columns else None 
            )
            st.plotly_chart(fig_sellers)
            customer_growth_data = fetch_batch_data(Q.customer_growth_query)
            fig_growth = px.line(customer_growth_data, x='OrderDate', y='NewCustomers', title="Customer Growth Over Time", color_discrete_sequence=["#00CC96"])
            st.plotly_chart(fig_growth)
        with tab2:  
            def to_camel_case(name):
                words = name.split("_")
                return "".join(word.capitalize() for word in words)
            def get_table_names():
                query = f"SELECT table_name FROM `{client.project}.{Q.BQ_DATASET}.INFORMATION_SCHEMA.TABLES`"
                df = client.query(query).to_dataframe()
                
                table_names = df["table_name"].tolist()
                formatted_names = [to_camel_case(name) for name in table_names]

                return table_names, formatted_names
            def fetch_bath_data(table_name):
                query = f"SELECT * FROM `{client.project}.{Q.BQ_DATASET}.{table_name}` LIMIT 1000"
                df = client.query(query).to_dataframe()
                df.columns = [to_camel_case(col) for col in df.columns]
                return df

            def update_record(table_name, row_data, row_index):
                updates = ", ".join([f"{col}='{val}'" for col, val in row_data.items()])
                query = f"UPDATE `{client.project}.{Q.BQ_DATASET}.{table_name}` SET {updates} WHERE id = {row_data['Id']}"
                client.query(query).result()
                st.success(f"Row {row_index + 1} updated successfully!")
            def delete_record(table_name, row_id):
                query = f"DELETE FROM `{client.project}.{Q.BQ_DATASET}.{table_name}` WHERE id = {row_id}"
                client.query(query).result()
                st.warning(f"Row {row_id} deleted successfully!")
            def add_new_row(table_name, new_row_data):
                for key, value in new_row_data.items():
                    if key.lower() == "id": 
                        try:
                            new_row_data[key] = int(value)
                        except ValueError:
                            st.error(f"Invalid value for ID: {value}. Must be an integer.")
                            return

                columns = ", ".join(new_row_data.keys())
                values = ", ".join([f"'{val}'" if isinstance(val, str) else str(val) for val in new_row_data.values()])

                query = f"INSERT INTO `{client.project}.{Q.BQ_DATASET}.{table_name}` ({columns}) VALUES ({values})"
                
                try:
                    client.query(query).result()
                    st.success("New row added successfully!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error inserting row: {e}")
            table_names, formatted_names = get_table_names()
            tab2.subheader("All Tables Data")
            if table_names:
                tabs = st.tabs(formatted_names)
                for tab, table_name, formatted_name in zip(tabs, table_names, formatted_names):
                    with tab:  
                        df = fetch_bath_data(table_name)
                        col1, col2 = st.columns(2)
                        with col1:
                            search_query = st.text_input(f"Search in {formatted_name}", key=f"{table_name}_search")

                            if search_query:
                                df = df[df.apply(lambda row: row.astype(str).str.contains(search_query, case=False, na=False).any(), axis=1)]
                        with col2:
                            rows_per_page = st.selectbox("Rows per page", [25, 50, 75, 100], index=0, key=f"{table_name}_rows")
                            total_pages = max(1, (len(df) // rows_per_page) + (1 if len(df) % rows_per_page > 0 else 0))
                        if f"{table_name}_current_page" not in st.session_state:
                            st.session_state[f"{table_name}_current_page"] = 1

                        start_idx = (st.session_state[f"{table_name}_current_page"] - 1) * rows_per_page
                        end_idx = start_idx + rows_per_page
                        paginated_df = df.iloc[start_idx:end_idx]
                        if role == 1:
                            paginated_df["Edit"] = ["✏️ Edit"] * len(paginated_df)
                            paginated_df["Delete"] = ["🗑️ Delete"] * len(paginated_df)

                        edited_df = st.data_editor(paginated_df, height=500, key=f"{table_name}_editor")
                        if role == 1:
                            for index, row in edited_df.iterrows():
                                row_id = row.get("Id")
                                
                                if row.get("Edit") == "Clicked":
                                    update_record(table_name, row.to_dict(), index)

                                if row.get("Delete") == "Clicked":
                                    delete_record(table_name, row_id)
                        pagination_container = st.columns([1, 3, 1])

                        with pagination_container[0]:
                            if st.button("⬅️ Previous", use_container_width=True, disabled=(st.session_state[f"{table_name}_current_page"] == 1), key=f"{table_name}_prev"):
                                st.session_state[f"{table_name}_current_page"] -= 1

                        with pagination_container[1]:
                            st.markdown(f"<h5 style='text-align: center;'>Page {st.session_state[f'{table_name}_current_page']} of {total_pages}</h5>", unsafe_allow_html=True)

                        with pagination_container[2]:
                            if st.button("Next ➡️", use_container_width=True, disabled=(st.session_state[f"{table_name}_current_page"] == total_pages), key=f"{table_name}_next"):
                                st.session_state[f"{table_name}_current_page"] += 1
                        if role == 1:
                            st.subheader(f"Add New Row to {formatted_name}")

                            new_row = {}
                            for col in df.columns:
                                new_row[col] = st.text_input(f"Enter {col}", key=f"new_{table_name}_{col}")

                            if st.button("Add Row", key=f"add_{table_name}"):
                                add_new_row(table_name, new_row)

            else:
                st.error("No tables found in the dataset.")
    elif st.session_state.view_option == "Order Summary":
        st.header("📦 Order Summary")
        logger.info("Order page Viewed")
        st.success("Order summary data Analysis.")
        order_status_data = fetch_batch_data(Q.QUERY_ORDER_STATUS)
        daily_orders_data = fetch_batch_data(Q.QUERY_DAILY_ORDERS)
        tab1, tab2 = st.tabs(["📈 Charts", "🗃 Tables"])
        with tab1:
            st.subheader("Order Status Distribution")
            chart_type = st.radio("Select Visualization Type:", ["Both","Bar Chart", "Pie Chart"], horizontal=True)
            fig_bar = px.bar(order_status_data, x="OrderStatus", y="Count", title="Order Status Breakdown", text='Count')
            fig_bar.update_layout(
                width=200,
                height=600,
                margin=dict(l=50, r=50, t=200, b=10)            )
            fig_bar.update_traces(textposition="outside")

            order_status_data = order_status_data.sort_values(by="Count", ascending=True) 
            fig_pie = px.pie(order_status_data, names="OrderStatus", values="Count", title="Order Status Breakdown", hole=0.3)
            fig_pie.update_layout(
                width=200, 
                height=800,
                margin=dict(l=50, r=50, t=200, b=10)
            )
            fig_pie.update_traces(
                textinfo="label+percent",
                insidetextorientation="radial",
            )
            if chart_type == "Bar Chart":
                st.plotly_chart(fig_bar, use_container_width=True)
            elif chart_type == "Pie Chart":
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                col1, col2 = st.columns(2)
                with col1:
                    st.plotly_chart(fig_bar, use_container_width=True)
                with col2:
                    st.plotly_chart(fig_pie, use_container_width=True)
            st.subheader("Daily Orders Trend")
            daily_orders_data["OrderDate"] = pd.to_datetime(daily_orders_data["OrderDate"])

            available_years = sorted(daily_orders_data["OrderDate"].dt.year.unique())
            available_years.insert(0, "All") 

            col1, col2 = st.columns([1, 2])

            with col1:
                st.write("### Select Year")
                selected_year = st.selectbox("", available_years, key="year_filter") 

            with col2:
                st.write("### Chart Type")
                chart_type = st.radio("", ["Both","Line Chart", "Area Chart"], horizontal=True, key="chart_type")

            # Filter data based on selected year
            filtered_data = daily_orders_data if selected_year == "All" else daily_orders_data[daily_orders_data["OrderDate"].dt.year == selected_year]

            st.subheader(f"Daily Orders Trend {'for the Year ' + str(selected_year) if selected_year != 'All' else 'Over All Years'}")

            fig_line = px.line(
                filtered_data, x="OrderDate", y="orders", 
                title=f"Daily Orders {'in ' + str(selected_year) if selected_year != 'All' else 'Over Time'}"
            )
            fig_area = px.area(
                filtered_data, x="OrderDate", y="orders", 
                title=f"Daily Orders {'in ' + str(selected_year) if selected_year != 'All' else 'Over Time'}",
                color_discrete_sequence=["#1f77b4"] 
            )

            fig_area.update_traces(fillcolor="rgba(31, 119, 180, 0.3)", line_color="#1f77b4")  # Gradient effect

            if chart_type == "Line Chart":
                st.plotly_chart(fig_line, use_container_width=True)
            elif chart_type == "Area Chart":
                st.plotly_chart(fig_area, use_container_width=True)
            else:
                col3, col4 = st.columns(2)
                with col3:
                    st.plotly_chart(fig_line, use_container_width=True)
                with col4:
                    st.plotly_chart(fig_area, use_container_width=True)
        with tab2:
            st.subheader("Order Tables")
            
            df = fetch_batch_data(Q.ORDER_TABLE_DETAILS)
            col1,col2 = st.columns(2)
            with col1:
                search_query = st.text_input("Search across all columns")

                if search_query:
                    df = df[df.apply(lambda row: row.astype(str).str.contains(search_query, case=False, na=False).any(), axis=1)]

            with col2:
                rows_per_page = st.selectbox("Rows per page", [25, 50, 75, 100], index=0)
                total_pages = max(1, (len(df) // rows_per_page) + (1 if len(df) % rows_per_page > 0 else 0))

            if "current_page" not in st.session_state:
                st.session_state.current_page = 1
            
            start_idx = (st.session_state.current_page - 1) * rows_per_page
            end_idx = start_idx + rows_per_page
            paginated_df = df.iloc[start_idx:end_idx]
            st.dataframe(paginated_df, height=500)
            pagination_container = st.columns([1, 3, 1])

            with pagination_container[0]:
                if st.button("⬅️ Previous", use_container_width=True, disabled=(st.session_state.current_page == 1)):
                    st.session_state.current_page -= 1

            with pagination_container[1]:
                st.markdown(f"<h5 style='text-align: center;'>Page {st.session_state.current_page} of {total_pages}</h5>", unsafe_allow_html=True)

            with pagination_container[2]:
                if st.button("Next ➡️", use_container_width=True, disabled=(st.session_state.current_page == total_pages)):
                    st.session_state.current_page += 1
    elif st.session_state.view_option == "Event Analytics":
        st.header("📈 Event Analytics")
        st.success("All Events Details ")  
        logger.info("Event page Viewed")
        tab1, tab2 = st.tabs(["📈 Charts", "🗃 Tables"])
        with tab1:
            df = fetch_all_events()
            # Interactive Metrics
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Events", len(df))
            col2.metric("Unique Users", df['UserId'].nunique())
            col3.metric("Total Revenue", f"${df['Price'].sum():,.2f}")

            # Visual Representations
            fig1 = px.bar(df.groupby('EventType').size().reset_index(name='Count'), x='EventType', y='Count', title='Conversion Funnel Breakdown', color='EventType')
            fig2 = px.pie(df, names='EventType', title='Event Type Proportions')
            fig3 = px.line(df.sort_values('TimeStamp'), x='TimeStamp', y='Price', title='Revenue Over Time')

            st.plotly_chart(fig1, use_container_width=True)
            st.plotly_chart(fig2, use_container_width=True)
            st.plotly_chart(fig3, use_container_width=True)
        with tab2:
            df = fetch_all_events()

            # Top Filters
            col1, col2, col3 = st.columns([1, 1, 1])

            event_type_filter = col1.multiselect("Event Type", EVENT_TYPES)
            user_id_filter = col2.text_input("User ID")
            product_id_filter = col3.text_input("Product ID")
            
            filter_checkbox = st.checkbox("Apply Filters")
            
            if filter_checkbox:
                if event_type_filter:
                    df = df[df['EventType'].isin(event_type_filter)]
                if user_id_filter:
                    df = df[df['UserId'].str.contains(user_id_filter, na=False)]
                if product_id_filter:
                    df = df[df['product_id'].str.contains(product_id_filter, na=False)]
            col1,col2 = st.columns(2)
            with col1:
                search_query = st.text_input("Search across all columns")

                if search_query:
                    df = df[df.apply(lambda row: row.astype(str).str.contains(search_query, case=False, na=False).any(), axis=1)]

            with col2:
                rows_per_page = st.selectbox("Rows per page", [25, 50, 75, 100], index=0)
                total_pages = max(1, (len(df) // rows_per_page) + (1 if len(df) % rows_per_page > 0 else 0))

            if "current_page" not in st.session_state:
                st.session_state.current_page = 1
            
            start_idx = (st.session_state.current_page - 1) * rows_per_page
            end_idx = start_idx + rows_per_page
            paginated_df = df.iloc[start_idx:end_idx]
            st.dataframe(paginated_df, height=500)
            pagination_container = st.columns([1, 3, 1])

            with pagination_container[0]:
                if st.button("⬅️ Previous", use_container_width=True, disabled=(st.session_state.current_page == 1)):
                    st.session_state.current_page -= 1

            with pagination_container[1]:
                st.markdown(f"<h5 style='text-align: center;'>Page {st.session_state.current_page} of {total_pages}</h5>", unsafe_allow_html=True)

            with pagination_container[2]:
                if st.button("Next ➡️", use_container_width=True, disabled=(st.session_state.current_page == total_pages)):
                    st.session_state.current_page += 1
    elif st.session_state.view_option == "Live Event Feed":
        st.header("⚡ Live Event Feed (Default Last 10 Minutes)")
        logger.info("Live Event page Viewed")
        time_range = st.selectbox("Select Time Range (Minutes)", [10, 20, 30, 40, 50, 60], index=0)
        st.success(f"Real-time event data for the last {time_range} minutes.")
        df = fetch_realtime_events(time_range)
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Events", len(df))
        col2.metric("Unique Users", df['UserId'].nunique())
        col3.metric("Total Revenue", f"${df['Price'].sum():,.2f}")
        tab1, tab2 = st.tabs(["📈 Charts", "🗃 Tables"])
        with tab1:
            fig1 = px.bar(df.groupby('EventType').size().reset_index(name='Count'), x='EventType', y='Count', title='Event Type Distribution', color='EventType')
            fig2 = px.pie(df, names='EventType', title='Event Type Proportions')
            fig3 = px.line(df.sort_values('TimeStamp'), x='TimeStamp', y='Price', title='Revenue Over Time')

            st.plotly_chart(fig1, use_container_width=True)
            st.plotly_chart(fig2, use_container_width=True)
            st.plotly_chart(fig3, use_container_width=True)
        with tab2:
            col1, col2, col3 = st.columns([1, 1, 1])

            event_type_filter = col1.multiselect("Event Type", EVENT_TYPES)
            user_id_filter = col2.text_input("User ID")
            product_id_filter = col3.text_input("Product ID")
            
            filter_checkbox = st.checkbox("Apply Filters")
            
            if filter_checkbox:
                if event_type_filter:
                    df = df[df['EventType'].isin(event_type_filter)]
                if user_id_filter:
                    df = df[df['UserId'].str.contains(user_id_filter, na=False)]
                if product_id_filter:
                    df = df[df['product_id'].str.contains(product_id_filter, na=False)]
            col1,col2 = st.columns(2)
            with col1:
                search_query = st.text_input("Search across all columns")
                if search_query:
                    df = df[df.apply(lambda row: row.astype(str).str.contains(search_query, case=False, na=False).any(), axis=1)]

            with col2:
                rows_per_page = st.selectbox("Rows per page", [25, 50, 75, 100], index=0)
                total_pages = max(1, (len(df) // rows_per_page) + (1 if len(df) % rows_per_page > 0 else 0))

            if "current_page" not in st.session_state:
                st.session_state.current_page = 1
            
            start_idx = (st.session_state.current_page - 1) * rows_per_page
            end_idx = start_idx + rows_per_page
            paginated_df = df.iloc[start_idx:end_idx]
            st.dataframe(paginated_df, height=500)
            pagination_container = st.columns([1, 3, 1])

            with pagination_container[0]:
                if st.button("⬅️ Previous", use_container_width=True, disabled=(st.session_state.current_page == 1)):
                    st.session_state.current_page -= 1

            with pagination_container[1]:
                st.markdown(f"<h5 style='text-align: center;'>Page {st.session_state.current_page} of {total_pages}</h5>", unsafe_allow_html=True)

            with pagination_container[2]:
                if st.button("Next ➡️", use_container_width=True, disabled=(st.session_state.current_page == total_pages)):
                    st.session_state.current_page += 1
        time.sleep(10)
        st.rerun()

    elif st.session_state.view_option == "Create New Table":
        st.title("Create New Table")
        dataset_id = Q.BQ_DATASET
        table_name = st.text_input("Enter Table Name")
        num_fields = st.number_input("Number of Columns", min_value=1, step=1, value=1)

        fields = []
        st.subheader("Define Columns")
        for i in range(num_fields):
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                field_name = st.text_input(f"Column {i+1} Name", key=f"name_{i}")
            with col2:
                field_type = st.selectbox(
                    f"Column {i+1} Type", 
                    ["STRING", "INTEGER", "FLOAT", "BOOLEAN", "TIMESTAMP", "DATE"], 
                    key=f"type_{i}"
                )
            with col3:
                constraint = st.selectbox(
                    f"Constraint {i+1}", 
                    ["", "NOT NULL","PRIMARY KEY"],
                    key=f"constraint_{i}"
                )
            with col4:
                default_value = st.text_input(
                    f"Default Value {i+1} (optional)", key=f"default_{i}"
                )
            field_def = f"{field_name} {field_type}"
            if constraint:
                field_def += f" {constraint}"
            if default_value:
                field_def += f" DEFAULT {default_value}"

            fields.append(field_def)
        if st.button("Create Table"):
            if dataset_id and table_name and fields:
                table_ref = f"{Q.BQ_PROJECT}.{dataset_id}.{table_name}"
                create_query = f"CREATE TABLE `{table_ref}` ({', '.join(fields)});"
                try:
                    client.query(create_query).result()
                    st.success(f"Table `{table_name}` created successfully in `{dataset_id}`!")
                    logger.info(f"Table `{table_name}` created successfully in `{dataset_id}`!")
                    
                except Exception as e:
                    st.error(f"Error: {e}")
            else:
                st.error("Please enter dataset ID, table name, and define fields properly.")
                logger.error("Please enter dataset ID, table name, and define fields properly.")
        
st.set_page_config(layout="wide")
if __name__ == "__main__":
    try: 
        main(1)
    except Exception as e:
        logger.warning(e)
        st.markdown(f"Error Occurred\n This is the Error Description: {e}")
