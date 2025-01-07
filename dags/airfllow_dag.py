from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sqlite3
import json
import pandas as pd
import requests

def get_last_processed_date():
    db_path = "/Users/vee/Desktop/ihc_data_pipeline/data/challenge.db"
    conn = sqlite3.connect(db_path)
    query = "SELECT MAX(event_date) FROM session_sources"
    last_processed_date = pd.read_sql(query, conn).iloc[0, 0]
    conn.close()
    return datetime.strptime(last_processed_date, '%Y-%m-%d') if last_processed_date else datetime(2023, 9, 7)  

last_processed_date = get_last_processed_date()

dag = DAG(
    'attribution_pipeline',
    description='Attribution Model Pipeline',
    schedule_interval='@daily', 
    start_date=datetime(2025, 1, 6),
    catchup=False,
)

# query new data from the database
def query_new_data_from_db(last_processed_date):
    db_path = "/Users/vee/Desktop/ihc_data_pipeline/data/challenge.db"
    conn = sqlite3.connect(db_path)
    
    # Query data from session_sources, only for sessions that occurred after the last processed_date
    query = f"""
        SELECT * FROM session_sources
        WHERE event_date > '{last_processed_date.strftime('%Y-%m-%d')}'
    """
    session_sources = pd.read_sql(query, conn)
    conversions = pd.read_sql("SELECT * FROM conversions", conn)
    session_costs = pd.read_sql("SELECT * FROM session_costs", conn)
    attribution_journey = pd.read_sql("SELECT * FROM attribution_customer_journey", conn)
    
    conn.close()
    
    return session_sources, conversions, session_costs, attribution_journey

# transform the data
def transform_data(session_sources, conversions, session_costs, attribution_journey):
    session_costs['cost'] = session_costs['cost'].fillna(0.0)
    
    # Merge session_sources with conversions on user_id
    merged = session_sources.merge(conversions, on='user_id', suffixes=('_session', '_conversion'))
    
    # Filtering sessions that occurred before conversions
    filtered_sessions = merged[
        (
            (merged['event_date'] < merged['conv_date']) |  
            ((merged['event_date'] == merged['conv_date']) & 
             (merged['event_time'] < merged['conv_time']))
        )
    ].sort_values(by=['conv_id', 'event_date', 'event_time'])
    
    # Group sessions by conv_id
    grouped_journeys = filtered_sessions.groupby("conv_id").apply(lambda x: x.to_dict(orient="records")).to_dict()
    
    # data structure for the API
    grouped_journeys_transformed = {
        conv_id: [
            {
                "conversion_id": conv_id, 
                "session_id": session["session_id"],
                "timestamp": f"{session['event_date']} {session['event_time']}",
                "channel_label": session["channel_name"],
                "holder_engagement": session["holder_engagement"],
                "closer_engagement": session["closer_engagement"],
                "conversion": 1 if session["conv_id"] == conv_id else 0,
                "impression_interaction": session["impression_interaction"],
            }
            for session in sessions
        ]
        for conv_id, sessions in grouped_journeys.items()
    }
    
    file_path = "/Users/vee/Desktop/ihc_data_pipeline/data/customer_journeys.json"
    with open(file_path, 'w') as f:
        json.dump(grouped_journeys_transformed, f)
    
    return file_path  

# chunk the data
def chunk_data(data, chunk_size):
    """Split data into smaller chunks."""
    keys = list(data.keys())  
    for i in range(0, len(keys), chunk_size):
        yield {k: data[k] for k in keys[i:i + chunk_size]}  

# send data to the API
def send_data_to_api(file_path):
    with open(file_path, 'r') as f:
        grouped_journeys_transformed = json.load(f)

    api_url = "https://api.ihc-attribution.com/v1/compute_ihc?conv_type_id=data_challenge"
    api_key = 'IHC_API_KEY'
    
    chunks = list(chunk_data(grouped_journeys_transformed, 100))
    processed_results = []
    
    for idx, chunk in enumerate(chunks, start=1):
        body = {
            "customer_journeys": chunk,
            "redistribution_parameter": {} 
        }
        
        try:
            response = requests.post(
                api_url,
                data=json.dumps(body),
                headers={"Content-Type": "application/json", "x-api-key": api_key}
            )
            response.raise_for_status()
            
            response_data = response.json()
            if "value" in response_data:
                processed_results.extend(response_data["value"])
            
        except requests.exceptions.RequestException as e:
            print(f"Failed to process Chunk {idx}: {e}")
    
    return processed_results

# store results in the database attribution customer journey table
def store_results_in_db(processed_results):
    db_path = "/Users/vee/Desktop/ihc_data_pipeline/data/challenge.db"
    conn = sqlite3.connect(db_path)
    
    results_df = pd.DataFrame(processed_results)
    results_df = results_df.rename(columns={"conversion_id": "conv_id"})
    
    results_df.to_sql("attribution_customer_journey", conn, if_exists="append", index=False)
    
    # Update the last processed date 
    latest_date = results_df['timestamp'].max()  
    last_processed_date = datetime.strptime(latest_date, '%Y-%m-%d %H:%M:%S')  
    
    query = "SELECT * FROM attribution_customer_journey LIMIT 5;"
    inserted_data = pd.read_sql_query(query, conn)
    print(inserted_data)
    
    conn.commit()
    conn.close()

    return last_processed_date  
#  channel reporting
def create_channel_reporting():
    db_path = "/Users/vee/Desktop/ihc_data_pipeline/data/challenge.db"
    conn = sqlite3.connect(db_path)
    
    # data for reporting
    session_sources_df = pd.read_sql_query("SELECT * FROM session_sources", conn)
    session_costs_df = pd.read_sql_query("SELECT * FROM session_costs", conn)
    conversions_df = pd.read_sql_query("SELECT * FROM conversions", conn)
    attribution_journey_df = pd.read_sql_query("SELECT * FROM attribution_customer_journey", conn)
    
    # Merge data
    merged_df = session_sources_df.merge(session_costs_df, on="session_id", how="left")
    merged_df = merged_df.merge(attribution_journey_df, on="session_id", how="inner")
    merged_df = merged_df.merge(conversions_df, on="conv_id", how="inner")
    
    # Group data by channel
    channel_reporting = (
        merged_df.groupby(["channel_name", "event_date"])
        .agg(
            total_cost=("cost", "sum"),
            total_ihc=("ihc", "sum"),
            total_ihc_revenue=("revenue", lambda x: (merged_df.loc[x.index, "ihc"] * x).sum())
        )
        .reset_index()
    )
    
    channel_reporting["CPO"] = channel_reporting["total_cost"] / channel_reporting["total_ihc"]

    channel_reporting["ROAS"] = channel_reporting.apply(
        lambda row: 'N/A' if row["total_cost"] == 0 or row["channel_name"] in ["Organic Traffic", "Direct Traffic"] 
        else row["total_ihc_revenue"] / row["total_cost"], axis=1
    )

    channel_reporting["ROAS"] = channel_reporting.apply(
        lambda row: 'N/A' if row["ROAS"] == 'N/A' else round(row["ROAS"], 2), axis=1
    )
    
    channel_reporting["CPO"] = channel_reporting["CPO"].round(2)

    channel_reporting.to_sql("channel_reporting", conn, if_exists="replace", index=False)
    channel_reporting.to_csv("/Users/vee/Desktop/ihc_data_pipeline/output/channel_reporting.csv", index=False)
    
    conn.close()

# automation task
query_data_task = PythonOperator(
    task_id='query_data_from_db',
    python_callable=query_new_data_from_db,
    op_args=[last_processed_date],
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=['{{ task_instance.xcom_pull(task_ids="query_data_from_db") }}'],
    dag=dag,
)

send_data_task = PythonOperator(
    task_id='send_data_to_api',
    python_callable=send_data_to_api,
    op_args=['{{ task_instance.xcom_pull(task_ids="transform_data") }}'],
    dag=dag,
)

store_results_task = PythonOperator(
    task_id='store_results_in_db',
    python_callable=store_results_in_db,
    op_args=['{{ task_instance.xcom_pull(task_ids="send_data_to_api") }}'],
    dag=dag,
)

create_channel_reporting_task = PythonOperator(
    task_id='create_channel_reporting',
    python_callable=create_channel_reporting,
    dag=dag,
)

# Task dependencies
query_data_task >> transform_data_task >> send_data_task >> store_results_task >> create_channel_reporting_task