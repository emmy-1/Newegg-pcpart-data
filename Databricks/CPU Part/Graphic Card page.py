# Databricks notebook source
from bs4 import BeautifulSoup
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

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
    StructField("Link", StringType(), True),
    StructField("GPU", StringType(), True),
    StructField("Memory", StringType(), True),
    StructField("Suggested PSU", StringType(), True),
    StructField("Length", StringType(), True),
    StructField("Ratings", StringType(), True),
    StructField("Price", StringType(), True),
    StructField("Image URL", StringType(), True),
    StructField("Tips", StringType(), True)
])

data = []

# Initialize the variable Gpu


for page in range(1, 13):
    if page == 1:
        Html_ = get_html(f'https://www.newegg.com/global/uk-en/tools/custom-pc-builder/pl/ID-48?diywishlist=0&isCompability=false')
    else:
        Html_ = get_html(f'https://www.newegg.com/global/uk-en/tools/custom-pc-builder/pl/ID-48/Page-{page}?diywishlist=0&isCompability=false')
    
    soup = BeautifulSoup(Html_, 'html.parser')
    table = find_element(soup, 'table', 'table-vertical')

    if table is not None:
        td_elements = table.select('td')

        for td in td_elements:
            title_div = td.find('div', class_='item-title')
            tips = td.find('div', class_ = 'item-tips')
            rating_element = td.find('span', class_='item-rating-num')
            price = td.find('li', class_ ='price-current')
            image = td.find('div', class_ = 'item-img hover-item-img')
            link = None

            if title_div is not None:
                title_span = title_div.find('span')
                if title_span is not None:
                    title_value = title_span.text.strip()
                    link = title_div.find('a')
                    if link is not None:
                        link = link['href']

            div = td.find('div', class_='hid-text')
            span = td.find('span')

            if rating_element is not None:
                ratings = rating_element.text.strip()

            if price is not None:
                price_value = price.find('strong').text.strip()

            if image is not None:
                image_url = image.find('img')['src']

            if tips is not None:
                tips_value = tips.text.strip()

            if div is not None and span is not None:
                label = div.text.strip()
                value = span.text.strip()

                if label == 'GPU':
                    Gpu = value
                elif label == 'Memory':
                    Memory = value
                elif label == 'Suggested PSU':
                    PSU = value
                elif label == 'Length':
                    length = value               
                # Append the scraped data to the list
                data.append((title_value, link, Gpu, Memory, PSU, length, ratings, price_value, image_url, tips_value))

# Create the DataFrame from the collected data
data_df = spark.createDataFrame(data, schema=schema)
# Write the DataFrame to a CSV file
data_df.coalesce(1).write.mode("overwrite").csv('abfss://pcpart@neweggdb.dfs.core.windows.net/Dataset/Raw/GPU', header=True)
fileName = dbutils.fs.ls('abfss://pcpart@neweggdb.dfs.core.windows.net/Dataset/Raw/GPU')
# create an empty string
name = ''

# find the csv file in the Raw/GPU file system
for file in fileName:
    if file.name.endswith('.csv'):
        name = file.name
# copy csv file if you see it
dbutils.fs.cp('abfss://pcpart@neweggdb.dfs.core.windows.net/Dataset/Raw/GPU/'+ name,'abfss://pcpart@neweggdb.dfs.core.windows.net/Dataset/Bronz_layer/Gpu.csv' )
