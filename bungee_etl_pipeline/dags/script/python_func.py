import json
from datetime import datetime
from venv import logger
import requests
import time
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np

load_dotenv()

NYTIMES_API_KEY = os.getenv('NYTIMES_API_KEY')

config_file_path = os.path.join(os.path.dirname(__file__), '../../config/config.json')
try:
    with open(config_file_path, 'r') as file:
        config = json.load(file)
except FileNotFoundError:
    print(f"Config file not found at: {config_file_path}")
except json.JSONDecodeError as exc:
    print(f"Error parsing JSON file: {exc}")

nytimes_api_url = config['api_urls']['nytimes']
openlibrary_api_url = config['api_urls']['openlibrary']

def exponential_backoff_retry(func, retries=3, initial_delay=1):
    delay = initial_delay
    for i in range(retries):
        try:
            return func()
        except Exception as e:
            print(f"Attempt {i+1} failed: {e}")
            if i < retries - 1:
                time.sleep(delay)
                delay *= 2
    raise Exception("All retry attempts failed.")


#function to fetch details of books from NYTimes
def get_nytimes_books():
    try:
        url = f"{nytimes_api_url}?api-key={'9kAhTTi7W6oQB8JXBxQ44tLkBhifMukc'}"
        response = requests.get(url, timeout=100)
        response.raise_for_status()
        logger.info("Successfully fetched NYTimes books")
        return response.json()
    
    except Exception as e:
        logger.error(f"NYTimes API request failed: {e}")
        return None

#function to get Details of those books from Open Library.
def get_openlibrary_data(isbn):
    try: 
        url = f"{openlibrary_api_url}?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if data:
            logger.info("Successfully fetched open library data for "+ isbn)
        else:
            logger.info("Data not found for "+ isbn) 
        return response.json()
    
    except Exception as e:
        logger.error(f"OpenLibrary API request failed: {e}")
        return None


#function that creates the complete book data.
def fetch_books_data():
    nytimes_data = exponential_backoff_retry(get_nytimes_books)
    books_data = []
    
    
    #previous_published_date
    published_date = nytimes_data['results']['previous_published_date']
    for book in nytimes_data['results']['books']:
        isbn_13 = book['primary_isbn13']
        try:
            openlibrary_data = exponential_backoff_retry(lambda: get_openlibrary_data(isbn_13))
            ol_book = openlibrary_data.get(f'ISBN:{isbn_13}', {})
            book['list_published_date'] = published_date
            book.update(ol_book)
            
            books_data.append(book)
        except Exception as e:
            print(f"Failed to process book with ISBN {isbn_13}: {e}")
    print ('df created')
    return pd.DataFrame(books_data)

def extract_url(links_list, store_name):
    for link in links_list:
        if link['name'] == store_name:
            return link['url']
    return None


data_types = {
    'primary_isbn13': str,
    'primary_isbn10': str,
    'title': str,
    'author': str,
    'publisher': str,
    'description': str,
    'list_published_date': 'datetime64[ns]',  
    'rank': 'Int64',  
    'weeks_on_list': 'Int64',  
    'number_of_pages': 'Int64',  
    'weight': float,
    'cover_small': str,
    'cover_large': str,
    'url': str,
    'amazon_buy_link': str,
    'apple_buy_link': str,
    'book_shop_buy_link': str,
    'ingested_at': 'datetime64[ns]'
}

required_columns = [
    'primary_isbn13', 'primary_isbn10', 'title', 'author', 'publisher', 
    'description', 'list_published_date', 'rank', 'weeks_on_list', 'number_of_pages', 
    'weight', 'cover_small', 'cover_large', 'url', 'amazon_buy_link', 'apple_buy_link', 
    'book_shop_buy_link', 'ingested_at'
]


#Data Transformation function. 
def manipulate_data(df):
    df['primary_isbn13'] = df['primary_isbn13'].astype(str)
    df['primary_isbn10'] = df['primary_isbn10'].astype(str)
    df['cover_small'] = df['cover'].apply(lambda x: x['small'] if isinstance(x, dict) else None)
    df['cover_large'] = df['cover'].apply(lambda x: x['large'] if isinstance(x, dict) else None)
    df['amazon_buy_link'] = df['buy_links'].apply(lambda x: extract_url(x, 'Amazon'))
    df['apple_buy_link'] = df['buy_links'].apply(lambda x: extract_url(x, 'Apple Books'))
    df['book_shop_buy_link'] = df['buy_links'].apply(lambda x: extract_url(x, 'Bookshop.org'))
    df['publisher'] = df['publishers'].apply(lambda x: ', '.join(d['name'] for d in x) if isinstance(x, list) else None)
    df['published_place'] = df['publish_places'].apply(lambda x: x[0]['name'] if isinstance(x, list) else None)
    df["ingested_at"] = datetime.now()
    final_df = df[required_columns]
    final_df = final_df.astype(data_types, errors='ignore')
    final_df = final_df.where(pd.notna(final_df), np.nan)
    return final_df

def final_df_data_validation(df):
    logger.info("published date is "+ str(df['list_published_date'].head(1)))
    logger.info("Checking for Duplicates or extra data \n No of rows must be 15") 
    dataframe_length = len(df.index)
    dataframe_column_length = len(df.columns)
    logger.info("Number of rows in the dataframe were " + str(dataframe_length))
    logger.info("Number of columns in the dataframe were " + str(dataframe_column_length))
    is_isbn10_null = df['primary_isbn10'].notna().any()
    is_isbn13_null = df['primary_isbn13'].notna().any()
    logger.info("is any Null value found in isbn_10" + str(is_isbn10_null))
    logger.info("is any Null value found in isbn_13" + str(is_isbn13_null))
    