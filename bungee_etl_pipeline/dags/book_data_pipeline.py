from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
# from airflow.providers.postgres.
from script.python_func import fetch_books_data, manipulate_data, final_df_data_validation

default_args = {
    'owner': 'Taushique Jafri',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def  fetch_data():
    df = fetch_books_data()
    return df

def transform_data(ti):
    df = ti.xcom_pull(task_ids = 'fetch_api_data')
    final_df = manipulate_data(df)
    return final_df

def validate_date(ti):
    df = ti.xcom_pull(task_ids = 'transform_collected_data')
    final_df_data_validation(df)

def insert_data_postgres(ti):
    book_data = ti.xcom_pull(task_ids = 'transform_collected_data')
    print(type(book_data))
    if isinstance(book_data, pd.DataFrame):
        book_data = book_data.to_dict(orient="records")
        
    print(book_data)
    postgres_hook = PostgresHook(postgres_connection_id = 'postgres_default')
    insert_query = """
        INSERT INTO books ( primary_isbn13, primary_isbn10, title, author, publisher, description, list_published_date,
                       rank, weeks_on_list, number_of_pages, weight, cover_small, cover_large, 
                       url, amazon_buy_link, apple_buy_link, book_shop_buy_link, ingested_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters = [book['primary_isbn13'], book['primary_isbn10'], book['title'], book['author'], book['publisher'], book['description'],
                            book['list_published_date'], book['rank'], book['weeks_on_list'], book['number_of_pages'],
                            book['weight'],  book['cover_small'], book['cover_large'], book['url'],
                            book['amazon_buy_link'], book['apple_buy_link'], book['book_shop_buy_link'], book['ingested_at']                            
                        ])

with DAG(
    'book_data_pipeline',
    default_args=default_args,
    description='ETL pipeline to fetch books data and store them into database',
    schedule_interval='0 9 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    

    task_fetch_data_from_api = PythonOperator(
        task_id = 'fetch_api_data',
        python_callable = fetch_data
    )
    
    task_transform_data = PythonOperator(
        task_id = 'transform_collected_data',
        python_callable = transform_data
    )
    
    task_validate_data = PythonOperator(
        task_id = 'validate_collected_data',
        python_callable = validate_date
    )
    
    task_insert_book_data = PythonOperator(
        task_id = 'insert_book_data',
        python_callable = insert_data_postgres
    )
    
    task_audit_books_data = PostgresOperator(
        task_id='audit_books_data',
        conn_id='postgres_default',
        sql="""
            -- Create the books_audit table if it doesn't exist
            CREATE TABLE IF NOT EXISTS public.books_audit (
                ingestion_date DATE PRIMARY KEY,
                total_rows INT,
                missing_rank INT,
                missing_isbn13 INT,
                missing_isbn10 INT,
                missing_published_date INT,
                negative_weight INT,
                negative_number_of_pages INT,
                unique_ranks INT,
                duplicate_ranks INT,
                invalid_isbn13_length INT,
                invalid_isbn10_length INT,
                invalid_dates INT
            );

            -- Upsert the daily aggregated audit data
            WITH aggregated_data AS (
                SELECT 
                    DATE(ingested_at) AS ingestion_date,
                    COUNT(*) AS total_rows,
                    COUNT(*) FILTER (WHERE rank IS NULL) AS missing_rank,
                    COUNT(*) FILTER (WHERE primary_isbn13 IS NULL) AS missing_isbn13,
                    COUNT(*) FILTER (WHERE primary_isbn10 IS NULL) AS missing_isbn10,
                    COUNT(*) FILTER (WHERE list_published_date IS NULL) AS missing_published_date,
                    COUNT(*) FILTER (WHERE weight < 0) AS negative_weight,
                    COUNT(*) FILTER (WHERE number_of_pages < 0) AS negative_number_of_pages,
                    COUNT(DISTINCT rank) AS unique_ranks,
                    COUNT(rank) - COUNT(DISTINCT rank) AS duplicate_ranks,
                    COUNT(*) FILTER (WHERE LENGTH(primary_isbn13::TEXT) <> 13) AS invalid_isbn13_length,
                    COUNT(*) FILTER (WHERE LENGTH(primary_isbn10::TEXT) <> 10) AS invalid_isbn10_length,
                    COUNT(*) FILTER (WHERE list_published_date::TEXT !~ '^\d{4}-\d{2}-\d{2}$') AS invalid_dates
                FROM public.books
                GROUP BY ingestion_date
            )
            INSERT INTO public.books_audit (
                ingestion_date, total_rows, missing_rank, missing_isbn13, missing_isbn10,
                missing_published_date, negative_weight, negative_number_of_pages,
                unique_ranks, duplicate_ranks, invalid_isbn13_length, invalid_isbn10_length, invalid_dates
            )
            SELECT * FROM aggregated_data
            ON CONFLICT (ingestion_date) 
            DO UPDATE SET 
                total_rows = EXCLUDED.total_rows,
                missing_rank = EXCLUDED.missing_rank,
                missing_isbn13 = EXCLUDED.missing_isbn13,
                missing_isbn10 = EXCLUDED.missing_isbn10,
                missing_published_date = EXCLUDED.missing_published_date,
                negative_weight = EXCLUDED.negative_weight,
                negative_number_of_pages = EXCLUDED.negative_number_of_pages,
                unique_ranks = EXCLUDED.unique_ranks,
                duplicate_ranks = EXCLUDED.duplicate_ranks,
                invalid_isbn13_length = EXCLUDED.invalid_isbn13_length,
                invalid_isbn10_length = EXCLUDED.invalid_isbn10_length,
                invalid_dates = EXCLUDED.invalid_dates;
        """
    )
    
    task_create_table = PostgresOperator(
        task_id = 'create_table',
        conn_id = 'postgres_default',
        sql = """
            CREATE TABLE IF NOT EXISTS books (
                id SERIAL PRIMARY KEY,
                primary_isbn13 VARCHAR(13),
                primary_isbn10 VARCHAR(10),
                title TEXT,
                author TEXT,
                publisher TEXT,
                description TEXT,
                list_published_date DATE,
                rank INT,
                weeks_on_list INT,
                number_of_pages INT,
                weight FLOAT,
                cover_small TEXT,
                cover_large TEXT,
                url TEXT,
                amazon_buy_link TEXT,
                apple_buy_link TEXT,
                book_shop_buy_link TEXT,
                ingested_at TIMESTAMP
            );      
        """
    )

    #Setting the task dependencies
    task_fetch_data_from_api >> task_transform_data >> task_validate_data >> task_create_table >> task_insert_book_data >> task_audit_books_data
