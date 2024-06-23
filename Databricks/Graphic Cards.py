# Databricks notebook source
# MAGIC %md
# MAGIC #Load, Extract and Clean Graphic Card Data for New Egg.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Install Necressary Libarbies 

# COMMAND ----------

!pip install beautifulsoup4 requests pandas

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

# MAGIC %md
# MAGIC # Define Functions
# MAGIC

# COMMAND ----------

# Get HTML from a URL

def get_html(url):
    """Send a GET request to a URL and return the page content as a BeautifulSoup object
    Args:
        url (str): The URL to send the GET request 
    """
    html_request = requests.get(url).text
    return BeautifulSoup(html_request, 'lxml')

# Extract text from an element using a tag and class name
def extract_text(element, tag, class_name, default=None):

    """Extract text from an element using a tag and class name"""
    found_element = element.find(tag, class_=class_name)
    return found_element.text.strip() if found_element else default

# extract href link from an element using a tag and class name
def extract_link(element, tag, class_name, default=''):
    """Extract the href link from an element using a tag and class name"""
    found_element = element.find(tag, class_=class_name)
    return found_element['href'] if found_element else default



# COMMAND ----------

def NeweggGraphics():
  # Create an empty DataFrame for storing graphic card details.
    Graphiccard = pd.DataFrame(columns=[
        "Graphiccard", "Brand", "Ratings", "Price", "Model No", "Link","Series", "Interface", "Chipset", "GPU_Series", "GPU","Aritecture", "core_clock", "Boost_Clock", "Memory_Type", "review"
    ])
    # Loop through the pages of the website. The Graphic card page has a total of 7 pages which will be looped through.
    for i in range(1, 8):
        # Define the URL of the page. The page size is set to i because the page size changes for each page.
        html_page = f'https://www.newegg.com/global/uk-en/GPUs-Video-Graphics-Cards/SubCategory/ID-48/Page-{i}?PageSize=36'
        
        # Send a GET request to the page and parse the page content with BeautifulSoup
        soup = get_html(html_page)
        # Find all graphic card items on the page as the loop continues
        pc_part = soup.find_all('div', class_='item-cell')
        print(pc_part)

        # Loop through each graphic card item
        for pc in pc_part:
            # Extract the necessary details from the item

            # Extract the coressponding details from the page using the extract_text and # extract_link functions, element and class name
            Graphiccard_name = extract_text(pc, 'a', 'item-title')
            model_no = extract_text(pc, 'ul', 'item-features')
            ratings = extract_text(pc, 'span', 'item-rating-num', 'Null')
            price = pc.find('li', class_='price-current')
            strongprice = price.find('strong').text if price else 'Null'
            link = extract_link(pc, 'a', 'item-title')

            print(f"Graphiccard: {Graphiccard_name.strip()}")
            print(f"Ratings: {ratings.strip()}")
            print(f"Price: {strongprice.strip()}")
            print(f"Model No: {model_no.strip()}")
            print(f"Link: {link}")
