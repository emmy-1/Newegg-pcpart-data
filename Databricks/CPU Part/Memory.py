# Databricks notebook source
from bs4 import BeautifulSoup
import requests
from pyspark.sql.types import StructType, StructField, StringType

def get_html(url):
    """Send a GET request to a URL and return the page content as a BeautifulSoup object
    
    Args:
        url (str): The URL to send the GET request 
        
    Returns:
        BeautifulSoup: The page content as a BeautifulSoup object
    """
    response = requests.get(url)
    return response.content

def find_element(html_page, element, class_name):
    return html_page.find(element, class_=class_name)

# Define the schema for the DataFrame
schema = StructType([
    StructField("Title", StringType(), True),
    StructField("Speed", StringType(), True),
    StructField("Module", StringType(), True),
    StructField("Color", StringType(), True),
    StructField("CAS Latency", StringType(), True),
    StructField("Ratings", StringType(), True),
    StructField("Price", StringType(), True),
    StructField("Image URL", StringType(), True),
    StructField("Tips", StringType(), True)
])

data = []

for page in range(1, 20):
    if page == 1:
        Html_ = get_html(f'https://www.newegg.com/global/uk-en/tools/custom-pc-builder/pl/ID-147?diywishlist=0&isCompability=false')
    else:
        Html_ = get_html(f'https://www.newegg.com/global/uk-en/tools/custom-pc-builder/pl/ID-147/Page-{page}?diywishlist=0&isCompability=false')
    
    soup = BeautifulSoup(Html_, 'html.parser')
    table = find_element(soup, 'table', 'table-vertical')

    if table is not None:
        td_elements = table.select('td')

    for td in td_elements:
        div = td.find('div', class_='hid-text')
        span = td.find('span')
        rating_element = td.find('span', class_='item-rating-num')
        title_div = td.find('div', class_='item-title')
        tips = td.find('div', class_='item-tips')
        rating_element = td.find('span', class_='item-rating-num')
        price = td.find('li', class_='price-current')
        image = td.find('div', class_='item-img hover-item-img')
        link = None

        if title_div is not None:
            title_span = title_div.find('span')
            if title_span is not None:
                title = title_span.text.strip()
        

        if rating_element is not None:
            ratings = rating_element.text.strip()

        if price is not None and price.find('strong') is not None:
            price_value = price.find('strong').text.strip()

        if image is not None and image.find('img') is not None:
            image_url = image.find('img')['src']

        if tips is not None:
            tips_value = tips.text.strip()

        if div is not None and span is not None:
            label = div.text.strip()
            value = span.text.strip()

            if label == 'Speed':
                Speed = value
            elif label == 'Module':
                Module = value
            elif label == 'Color':
                Color = value
            elif label == 'CAS Latency':
                CAS_Latency = value

        # Append the data to the list
        data.append((title, Speed, Module, Color, CAS_Latency, ratings, price_value, image_url, tips_value))
        
# Create the DataFrame from the collected data            
data_df = spark.createDataFrame(data, schema=schema)

# Write the DataFrame to a CSV file
data_df.coalesce(1).write.mode("overwrite").csv('abfss://pcpart@neweggdb.dfs.core.windows.net/Dataset/Raw/Memory', header=True)
