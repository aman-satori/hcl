# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 14:31:46 2024

@author: Aman Jaiswar
"""

import os
import sys
import time
import json
import re
import html
import threading
import pandas as pd
import numpy as np
import requests
from datetime import datetime
from requests_futures.sessions import FuturesSession
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from urllib3.util import Retry
from itertools import islice

# Constants
RETRY_STRATEGY = Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[400, 401, 402, 403, 404, 415, 422, 500]
)
ADAPTER = HTTPAdapter(max_retries=RETRY_STRATEGY)
MAX_REQUESTS_PER_SECOND = 6
REQUEST_INTERVAL = 1 / MAX_REQUESTS_PER_SECOND  # Time between requests in seconds

# Semaphore to control the number of concurrent requests
semaphore = threading.Semaphore(MAX_REQUESTS_PER_SECOND)

# Initialize HTTP session
http = requests.Session()
http.mount("http://", ADAPTER)
http.mount("https://", ADAPTER)

v_api_host = hcl.system_variable["hb_api_host"]
v_org_id = hcl.system_variable["organization_id"]

def rate_limited_get(session, url, headers):
    """Handles rate-limited GET requests using a semaphore."""
    with semaphore:
        future = session.get(url, headers=headers)
        time.sleep(REQUEST_INTERVAL)  # Enforce rate limit
        return future


def get_paginated_data(session, url, headers, v_api_host = v_api_host):
    """Fetches paginated data from a URL, handling multiple pages as necessary.
    
    Args:
        session: The requests session object.
        url (str): The API endpoint URL.
        headers (dict): The request headers.
    
    Returns:
        list: A list of data retrieved from the paginated API.
    """
    request_data = []
    # Initial request
    if "?" not in url:
        url = f"{url}?page[size]=100"
    else:
        url = f"{url}&page[size]=100"

    # Process the first response
    get_req = rate_limited_get(session, url, headers)  # Rate-limited request
    result = get_req.result()
    result.raise_for_status()  # Raises HTTPError if the status code is 4xx or 5xx
    if result.status_code == 200:
        data = result.json()
        if isinstance(data.get("data"), list):
            request_data.extend(data.get("data"))
        else:
            request_data.append(data.get("data"))

        # Continue to fetch next pages if available
        while data.get("links") and data["links"].get("next"):
            next_url = f"{v_api_host}{data['links']['next']}"
            get_req = rate_limited_get(session, next_url, headers)  # Rate-limited request
            result = get_req.result()
            result.raise_for_status()
            if result.status_code == 200:
                data = result.json()
                request_data.extend(data.get("data"))
            else:
                raise RuntimeError(f"Failed to fetch data: Code - {result.status_code}, Error - {result.content}")
    
    return request_data


def get_request_multiple(urls, headers, v_api_host = v_api_host, v_org_id = v_org_id):
    """Fetches data from multiple URLs in batches, handling rate limits.
    
    Args:
        urls (list): A list of URLs to fetch data from.
        headers (dict): The request headers.
    
    Returns:
        list: A list of all retrieved data items.
    """
    estimated_max_time = len(urls) * 3 / MAX_REQUESTS_PER_SECOND
    print(f"Estimated maximum time to retrieve all URLs: {estimated_max_time:.2f} seconds")

    session = FuturesSession()
    session.mount("http://", ADAPTER)
    session.mount("https://", ADAPTER)

    responses = []

    def process_batch(batch):
        """Processes a batch of URLs and appends results to responses."""
        futures = [session.executor.submit(get_paginated_data, session, url, headers) for url in batch]
        for future in futures:
            try:
                result = future.result()
                if result:
                    if isinstance(result, list):
                        responses.extend(result)
                    else:
                        responses.append(result)
            except HTTPError as http_err:
                print(f"HTTP error occurred: {http_err}")
            except Exception as err:
                print(f"An error occurred: {err}")

    start_time = time.time()
    batch_size = 6
    it = iter(urls)

    while True:
        batch = list(islice(it, batch_size))
        if not batch:
            break
        process_batch(batch)

    actual_time_taken = time.time() - start_time
    print(f"Actual time taken to retrieve all URLs: {actual_time_taken:.2f} seconds")
    print(f'From {len(urls)} URLs, retrieved {len(responses)} items')
    
    return responses


def get_request(url, headers, v_api_host = v_api_host, v_org_id = v_org_id):
    """
    Formatted URL : f'{v_api_host}/v1/orgs/{v_org_id}/'
    Fetches data from a single URL with pagination.
    
    Args:
        url (str): The API endpoint URL.
        headers (dict): The request headers.
    
    Returns:
        list: A list of data retrieved from the API.
    """
    request_data = []
    page_number = 1
    url = f"{url}?page[size]=100" if "?" not in url else f"{url}&page[size]=100"
    get_req = http.get(url, headers=headers)

    if get_req.status_code == 200:
        data = json.loads(get_req.content)
    else:
        return None

    if isinstance(data["data"], list):
        request_data.extend(data["data"])
    else:
        request_data.append(data["data"])
    sys.stdout.write(f"Page {page_number}\r")
    sys.stdout.flush()

    while data.get("links") is not None:
        if data["links"].get("next") is None:
            break
        else:
            get_req = http.get(f"{v_api_host}{data['links']['next']}", headers=headers)
            if get_req.status_code == 200:
                data = json.loads(get_req.content)
                if isinstance(data["data"], list):
                    request_data.extend(data["data"])
                else:
                    request_data.append(data["data"])
                page_number += 1
                sys.stdout.write(f"Page {page_number}\r")
                sys.stdout.flush()
            else:
                print(f"Failed: Code - {data.status_code}, Error - {data.content}")
                print(url)

    return request_data


def extract_custom_attributes_as_columns(df, key, custom_attribute_column):
    """Extracts custom attributes from a DataFrame and returns them as separate columns.
    
    Args:
        df (DataFrame): The DataFrame containing custom attributes.
        key (str): The column name of the targeted report level.
        custom_attribute_column (str): The column name where custom attributes are stored.
    
    Returns:
        tuple: A DataFrame of extracted attributes and a list of column names.
    """
    normalized_dfs = []
    
    for index, row in df.iterrows():
        attributes_df = pd.json_normalize(row[custom_attribute_column])
        attributes_df[key] = row[key]
        normalized_dfs.append(attributes_df)

    result_df = pd.concat(normalized_dfs, ignore_index=True)
    if result_df.empty:
        result_df[key] = None
        result_df["term"] = None
        result_df["value"] = None

    result_df = result_df.pivot(index=key, columns='term', values='value')
    result_df.columns.name = None
    result_df.reset_index(inplace=True)
    ca_column_names = list(result_df.columns)
    ca_column_names.remove(key)

    return result_df, ca_column_names


def clean(cell):
    """Cleans unescaped characters and HTML tags from a cell value.
    
    Args:
        cell (any): The cell value to clean.
    
    Returns:
        str: The cleaned cell value.
    """
    if cell is not None and isinstance(cell, str):
        cell = html.unescape(cell)
        cell = re.sub(r'<br\s*/?>', '\n', cell)
        cell = re.sub(r'<p\s*/?>', '\n', cell)
        cell = re.sub(r'<li\s*>', '\n• ', cell)
        cell = re.sub(r'</li>', '', cell)
        cell = re.sub(r'<[^>]+>', '', cell)
        cell = cell.strip()
    return cell


def hyperlink(url, text):
    """Creates a hyperlink string from a URL and display text.
    
    Args:
        url (str): The URL to convert into a hyperlink.
        text (str): The display text for the hyperlink.
    
    Returns:
        str: The formatted hyperlink string.
    """
    return f"<a href='{url}' target='_blank'>{text}</a>" if url else None


def from_results(table_id, headers):
    """Fetches records from a results table by ID.
    
    Args:
        table_id (str): The results table ID.
        headers (dict): The request headers.
    
    Returns:
        DataFrame: A DataFrame of retrieved records.
    
    Raises:
        Exception: If the request fails.
    """
    records = http.get(f"{v_api_host}/v1/orgs/{v_org_id}/tables/{table_id}/records", headers=headers)
    if records.status_code == 200:
        records = json.loads(records.content)
        columns = pd.json_normalize(records["columns"])["display_name"].values
        df = pd.json_normalize(records["data"])
        df.columns = columns
        
        # Drop unwanted columns
        drop_columns = ['Priority', 'Status', 'Published', 'Publisher name', 'Assignee', 
                        'Workflow group', 'Updated', 'Closed', 'Record ID', 'Collection', 
                        'Results table']
        df.drop(drop_columns, axis=1, inplace=True)
        return df
    else:
        raise Exception(f"Failed to retrieve results for table ID {table_id}: {records.status_code}, {records.content}")
