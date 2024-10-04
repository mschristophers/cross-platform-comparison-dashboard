# streamlit_app.py

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import altair as alt

# Set page configuration
st.set_page_config(
    page_title="WBTC Price Comparison",
    page_icon=":chart_with_upwards_trend:",
    layout="wide"
)

# App title and description
st.title('WBTC Price Comparison Across Platforms')

st.write("""
This app displays a line chart comparing the prices of WBTC against USDT across multiple platforms: **Binance** (baseline), **UniswapX**, **Hashflow**, and **Cowswap**.
Each platform is represented with a different color in the line chart, with **Binance** being the "baseline."
Hover over the graph to see the WBTC prices from all platforms at a specific time.
""")

# Initialize BigQuery client using Streamlit secrets
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Fetch the min and max date from the data
@st.cache_data(ttl=600)
def get_min_max_date():
    query = """
    SELECT 
        MIN(fill_time) AS min_date, 
        MAX(fill_time) AS max_date 
    FROM `tristerotrading.cross_platform_comparisons.wbtc_usdt_comparison`
    """
    query_job = client.query(query)
    result = query_job.result().to_arrow().to_pandas()
    return result['min_date'].iloc[0].date(), result['max_date'].iloc[0].date()

# Get the date range
min_date, max_date = get_min_max_date()

# Create date range selectors with the fetched min and max dates
start_date = st.date_input('Start date', min_value=min_date, max_value=max_date, value=min_date)
end_date = st.date_input('End date', min_value=min_date, max_value=max_date, value=max_date)

# Ensure that start_date is before end_date
if start_date > end_date:
    st.error('Error: Start date must be before end date.')
    st.stop()

# Button to generate graph
if st.button('Generate Graph'):
    # Define the SQL query with the selected date range
    query_data = f"""
    WITH comparison_data AS (
      SELECT 
        platform, 
        wbtc_price, 
        fill_time
      FROM `tristerotrading.cross_platform_comparisons.wbtc_usdt_comparison`
      WHERE fill_time BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
    ),
    binance_data AS (
      SELECT 
        price, 
        time
      FROM `tristerotrading.binance.wbtc_usdt_swap`
      WHERE time BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
    ),
    closest_binance AS (
      SELECT
        cd.platform,
        cd.fill_time,
        cd.wbtc_price,
        bd.price AS wbtc_price_from_binance,
        bd.time AS binance_time,
        ROW_NUMBER() OVER (PARTITION BY cd.fill_time ORDER BY ABS(TIMESTAMP_DIFF(cd.fill_time, bd.time, SECOND))) AS time_rank
      FROM comparison_data cd
      JOIN binance_data bd
      ON ABS(TIMESTAMP_DIFF(cd.fill_time, bd.time, SECOND)) <= 1800
    )
    SELECT 
      platform,
      fill_time,
      wbtc_price,
      wbtc_price_from_binance,
      binance_time
    FROM closest_binance
    WHERE time_rank = 1
    ORDER BY fill_time;
    """

    # Fetch data using the run_query function
    @st.cache_data(ttl=600)
    def run_query(query):
        query_job = client.query(query)
        table = query_job.result().to_arrow()
        df = table.to_pandas()
        return df

    data = run_query(query_data)

    # Check if data is not empty
    if data.empty:
        st.warning("No data found for the specified time range.")
        st.stop()

    # Convert time columns to datetime
    data['fill_time'] = pd.to_datetime(data['fill_time'])
    data['binance_time'] = pd.to_datetime(data['binance_time'])

    # Filter data based on the selected date range
    mask = (data['fill_time'].dt.date >= start_date) & (data['fill_time'].dt.date <= end_date)
    filtered_data = data.loc[mask]

    if filtered_data.empty:
        st.warning("No data available for the selected date range.")
        st.stop()

    # Prepare data for plotting
    df_platforms = filtered_data[['fill_time', 'wbtc_price', 'platform']].copy()
    df_platforms.rename(columns={'fill_time': 'time', 'wbtc_price': 'price'}, inplace=True)

    df_binance = filtered_data[['fill_time', 'wbtc_price_from_binance']].copy()
    df_binance.rename(columns={'fill_time': 'time', 'wbtc_price_from_binance': 'price'}, inplace=True)
    df_binance['platform'] = 'Binance'

    # Combine dataframes
    df_plot = pd.concat([df_platforms, df_binance], ignore_index=True)
    df_plot.dropna(subset=['time', 'price', 'platform'], inplace=True)
    df_plot['time'] = pd.to_datetime(df_plot['time'])

    platform_order = ['Binance', 'Cowswap', 'Hashflow', 'UniswapX']
    df_plot['platform'] = pd.Categorical(df_plot['platform'], categories=platform_order, ordered=True)

    price_min = df_plot['price'].min() * 0.999
    price_max = df_plot['price'].max() * 1.001

    df_plot.sort_values('time', inplace=True)

    color_mapping = {
        'Binance': '#000000',   # Black
        'Cowswap': '#EF553B',   # Red
        'Hashflow': '#00CC96',  # Green
        'UniswapX': '#AB63FA'   # Purple
    }

    df_pivot = df_plot.pivot(index='time', columns='platform', values='price').reset_index()

    nearest = alt.selection_point(
        fields=['time'],
        nearest=True,
        on='mouseover',
        empty='none',
        clear='mouseout'
    )

    line = alt.Chart(df_plot).mark_line().encode(
        x='time:T',
        y='price:Q',
        color=alt.Color('platform:N', scale=alt.Scale(domain=platform_order, range=[color_mapping.get(p) for p in platform_order])),
        strokeWidth=alt.value(1)
        # strokeWidth=alt.condition(
        #     alt.datum.platform == 'Binance',
        #     alt.value(3),
        #     alt.value(1)
        # )
    )

    rule = alt.Chart(df_pivot).mark_rule(color='gray').encode(
        x='time:T',
        opacity=alt.condition(nearest, alt.value(1), alt.value(0)),
        tooltip=[
            alt.Tooltip('time:T', title='Date (UTC)', format='%d-%m-%Y'),
            alt.Tooltip('time:T', title='Time (UTC)', format='%H:%M'),
            alt.Tooltip('Binance:Q', title='Binance Price', format=',.2f'),
            alt.Tooltip('Cowswap:Q', title='Cowswap Price', format=',.2f'),
            alt.Tooltip('Hashflow:Q', title='Hashflow Price', format=',.2f'),
            alt.Tooltip('UniswapX:Q', title='UniswapX Price', format=',.2f')
        ]
    ).add_params(
        nearest
    )

    chart = alt.layer(line, rule).properties(
        width='container',
        height=500,
        title="WBTC Price Comparison Across Platforms"
    ).interactive()

    st.altair_chart(chart, use_container_width=True)

    with st.expander("Show Raw Data"):
        st.write(filtered_data)
