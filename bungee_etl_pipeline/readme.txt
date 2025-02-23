Assignment Submitted by Taushique Jafri

Apache Airflow has been installed inside docker.
Docker version 27.5.1, build 9f9e405

ID , Password for Airflow webserver - airflow , airflow
Airflow running on localhost:8080
ID , Password for pg Admin - admin@admin.com, root
pg admin running on localhost:5050

The API KEYS has been set in the environment

The LOGS from the ETL are stored in the LOG folder
The ETL has been scheduled for UTC 09:00 @daily

********Setup Instructions*********

Run the following command to start the services:
    docker-compose up -d --build

First user account has been created so you do not need to run <docker compose up airflow-init>

The PostgreSQL and default_postgres connection used in airflow has been established for host 172.18.0.3
Inspect the PostgreSQL container to confirm its IP address:
    docker ps
    docker inspect <postgres-container-id>

Ensure that the PostgreSQL container IP is 172.18.0.3, otherwise, tasks 4, 5, and 6 will fail with connection error.

Open localhost:8080 login to Airflow UI with id airflow and password airflow
under DAGS search for book_data_pipeline and trigger it manually
You can check the Tables in pgAdmin under server/books_details_db/books



***********ETL Pipeline Tasks*********

The ETL pipeline consists of six tasks:

1. task_fetch_data_from_api
    Fetches data from the New York Times API.
    Queries the OpenLibrary API using ISBNs.
    Returns a combined DataFrame with data from both sources.
Note: The Google Books API was not used due to Google Play Console requiring payment.

2. task_transform_data
    Transforms the fetched data.
    Filters and selects the required columns.
    Defines and applies a schema.

3. task_validate_data
    Validates the transformed DataFrame.
    Logs key metrics and important validation parameters.

4. task_create_table
    Creates the necessary database table if it does not already exist.

5. task_insert_book_data
    Inserts the processed book data into the database.

6. task_audit_books_data
    Maintains an audit log of each ETL run.
    Creates an audit table in the database to track ingestion details.
    Provides insights into data quality and integrity.


***********Manual Audit Query****************

To manually inspect the audit details, run the following query in PostgreSQL:
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
ORDER BY ingestion_date DESC;

This query provides an overview of data integrity by checking for:
    Missing values in key fields (e.g., ISBNs, published dates, ranks).
    Negative values in numerical fields (e.g., weight, page count).
    Duplicate and invalid entries (e.g., ISBN length mismatches, incorrect date formats).
