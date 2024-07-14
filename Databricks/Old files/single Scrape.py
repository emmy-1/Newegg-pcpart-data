# Databricks notebook source
# MAGIC %md
# MAGIC #Load, Extract and Clean Graphic Card Data for New Egg.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Install Necressary Libarbies 

# COMMAND ----------

# MAGIC %md
# MAGIC # Import necessary libraries:
# MAGIC     # - BeautifulSoup: For parsing HTML content
# MAGIC     # - requests: For sending HTTP requests
# MAGIC     # - pandas: For creating and manipulating DataFrames

# COMMAND ----------

from bs4 import BeautifulSoup
import requests
import pandas as pd

# COMMAND ----------

def get_html(url):
    """Send a GET request to a URL and return the page content as a BeautifulSoup object
    
    Args:
        url (str): The URL to send the GET request 
        
    Returns:
        BeautifulSoup: The page content as a BeautifulSoup object
    """
    reponse = requests.get(url)
    return reponse.content

Html_ = get_html('https://www.newegg.com/global/uk-en/GPUs-Video-Graphics-Cards/SubCategory/ID-48')

# COMMAND ----------

# Create a BeautifulSoup object from the HTML content
Soup = BeautifulSoup(Html_,'html.parser')

# Find all div elements with class 'item-cell'
items = Soup.find('div', class_='item-cell')

# COMMAND ----------


def extract_value_from_specific_table(soup, table_class, target_th_text, table_index=0):
    """
    Extracts and returns the text of a <td> element that corresponds to a specific <th> text within a specific table of a given class.

    :param soup: BeautifulSoup object containing the parsed HTML.
    :param table_class: Class of the table from which to extract the value.
    :param target_th_text: The text of the <th> element for which the corresponding <td> text is desired.
    :param table_index: Index of the table from which to extract the value (default is 0, the first table).
    :return: The text of the corresponding <td> element or 'Not found' if not found.
    """
    tables = soup.find_all('table', class_=table_class)
    if table_index >= len(tables):
        return 'Table index out of range'
    
    table = tables[table_index]
    for row in table.find('tbody').find_all('tr'):
        th_text = row.find('th').text.strip()
        if th_text == target_th_text:
            td_text = row.find('td').text.strip()
            return td_text
    
    return 'Not found'

# COMMAND ----------

from bs4 import BeautifulSoup

for pc in items:
    Gpu_name = items.find('a', class_='item-title').text
    model_no = items.find('ul', class_='item-features').text
    No_rating = items.find('a', class_='item-rating').text
    Product_link = items.find('a', class_='item-title')['href']


    # get the html link for the product
    get_html(Product_link)
    productsoup = BeautifulSoup(get_html(Product_link), 'html.parser')

    # Extract Product information
    ratings = productsoup.select_one('.rating')['title'] if productsoup.select_one('.rating') else 'Null'
    price = productsoup.find('div', class_='price-current')
    stickthrough_price = productsoup.find('span', class_='price-was-data').text if productsoup.find('span', class_='price-was-data') else 'NA'
    brandname = extract_value_from_specific_table(productsoup,'table-horizontal','Brand',0)
    Series = extract_value_from_specific_table(productsoup,'table-horizontal','Series',0)
    interface = extract_value_from_specific_table(productsoup,'table-horizontal','Interface', 1)
    Cpu_manfacturer = extract_value_from_specific_table(productsoup,'table-horizontal','Chipset Manufacturer',2)
    Gpu = extract_value_from_specific_table(productsoup,'table-horizontal','GPU',2)
    core_clock = extract_value_from_specific_table(productsoup,'table-horizontal','Core Clock',2)
    boost_clock = extract_value_from_specific_table(productsoup,'table-horizontal','Boost Clock',2)
    memorysize = extract_value_from_specific_table(productsoup,'table-horizontal','Memory Size',3)
    form_factor = extract_value_from_specific_table(productsoup,'table-horizontal','Form Factor',7)

    print(f"Model_no:{model_no}")
    print(f"Gpu_name:'{Gpu_name}")
    print(f"Num_rating: {No_rating}")
    print(f"ratings:{ratings}")
    print(f"price:{price}")
    print(f"Stickthrough:{stickthrough_price}")
    print(f"Brand: {brandname}")
    print(f"Series:{Series}")
    print(f"interface:{interface}")
    print(f"Chipset Manufacturer:{Cpu_manfacturer}")
    print(f"Gpu: {Gpu}")
    print(f"Product_link:{Product_link}")
    print(f"Core_clock:{core_clock}")
    print(f"Boost_clock:{boost_clock}")
    print(f"Memory Size:{memorysize}")
    print(f"formfactor:{form_factor}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Define Functions
# MAGIC
