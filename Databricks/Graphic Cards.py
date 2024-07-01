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

# COMMAND ----------


def extract_value_from_specific_table(soup, table_class, target_th_text, table_index=0):
    """
    Extracts and returns the text of a <td> element that corresponds to a specific <th> text within a specific table of a given class.

    :param soup: BeautifulSoup object containing the parsed HTML.
    :param table_class: Class of the table from which to extract the value.
    :param target_th_text: The text of the <th> element for which the corresponding <td> text is desired.
    :param table_index: Index of the table from which to extract the value (default is 0, the first table).
    :return: The text of the corresponding <td> element or 'Null' if Null.
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
    
    return 'Null'

# COMMAND ----------

from bs4 import BeautifulSoup
for page in range(1,9):
    page_size = 36  # Number of items per page
    if page == 1:
        Html_ = get_html(f'https://www.newegg.com/global/uk-en/GPUs-Video-Graphics-Cards/SubCategory/ID-48')
    else:
        html_ = get_html(f'https://www.newegg.com/global/uk-en/GPUs-Video-Graphics-Cards/SubCategory/ID-48/Page-{page}')
    Soup = BeautifulSoup(Html_,'html.parser')

    # Find all div elements with class 'item-cell'
    items = Soup.find_all('div', class_='item-cell')

    for pc in items:
            Gpu_name = pc.find('a', class_='item-title').text if pc.find('a', class_='item-title') else 'NA'
            model_no = pc.find('ul', class_='item-features').text if pc.find('ul', class_='item-features') else 'NA'
            No_rating = pc.find('a', class_='item-rating').text if pc.find('a', class_='item-rating') else 'NA'
            Product_link = pc.find('a', class_='item-title')['href']
            price = pc.find('li', class_='price-current').text if pc.find('li',class_= 'price-current') else 'NA'
            stickthrough_price = pc.find('span', class_='price-was-data').text if pc.find('span', class_='price-was-data') else 'NA'
            # Extract Product information         
            # get the html link for the product
            get_html(Product_link)
            productsoup = BeautifulSoup(get_html(Product_link), 'html.parser')

            # List of possible class names for rating elements
            rating_classes = ['rating rating-5', 'rating rating-4-5', 'rating rating-4', 'rating rating-3-5']

            # Initialize ratings to 'Null' by default
            ratings = 'Null'

            # Iterate through the list of class names
            for rating_class in rating_classes:
                # Attempt to find the rating element in the first place
                rating_element = pc.find('i', class_=rating_class)
    
                if rating_element:
                # Safely extract the rating value from the 'aria-label' attribute
                    aria_label = rating_element.get('aria-label', '')
                    ratings = aria_label.split(' ')[1] if len(aria_label.split(' ')) > 1 else 'Null'
                    break  # Exit the loop once a rating is found
                else:
                    # If not found, check the second place for ratings
                    ratings_element = productsoup.find('i', class_=rating_class)
                    if ratings_element:
                        # Safely extract the rating from the 'title' attribute
                        title = ratings_element.get('title', '')
                        ratings = title.split(' ')[1] if len(title.split(' ')) > 1 else 'Null'
                        break  # Exit the loop once a rating is found        
                    
            #price = productsoup.find('div', class_='price-current').text if productsoup.find('div', class_='price-current') else 'Null'
            brandname = extract_value_from_specific_table(productsoup, 'table-horizontal', 'Brand', 0)
            Series = extract_value_from_specific_table(productsoup, 'table-horizontal', 'Series', 0)
            gpu_series = extract_value_from_specific_table(productsoup, 'table-horizontal', 'GPU Series', 2) 
            interface = extract_value_from_specific_table(productsoup, 'table-horizontal', 'Interface', 1)
            Cpu_manfacturer = extract_value_from_specific_table(productsoup, 'table-horizontal', 'Chipset Manufacturer', 2)
            Gpu = extract_value_from_specific_table(productsoup, 'table-horizontal', 'GPU', 2)


            # Initialize Core_Clock and Boost_Clock with default values
            Core_Clock = None
            Boost_Clock = None

            # Extract the core clock and boost clock from the product page
            td_elements = productsoup.select('#product-details td')

            # Find all the clock values in the product details
            clocks = [td for td in td_elements if 'MHz' in td.text]
            # Extract the core clock and boost clock values
            if len(clocks) >= 2:
                # Extract the clock values and find the maximum and minimum values
                clock_values = [int(''.join(filter(str.isdigit, clock.text))) for clock in clocks]
                max_clock_index = clock_values.index(max(clock_values))
                min_clock_index = clock_values.index(min(clock_values))
                # Assign the core clock and boost clock values
                Boost_Clock = clocks[max_clock_index].text
                Core_Clock = clocks[min_clock_index].text
            #boost_clock = extract_value_from_specific_table(productsoup, 'table-horizontal', 'Boost Clock', 2)
            memorysize = extract_value_from_specific_table(productsoup, 'table-horizontal', 'Memory Size', 3)
            form_factor = extract_value_from_specific_table(productsoup, 'table-horizontal', 'Form Factor', 7)

            print(f"Model_no: {model_no}")
            #print(f"Gpu_name: {Gpu_name}")
            #print(f"Num_rating: {No_rating}")
            #print(f"ratings: {ratings}")
            #print(f"price: {price}")
            #print(f"Stickthrough: {stickthrough_price}")
            print(f"Brand: {brandname}")
            print(f"Series: {Series}")
            print(f"GPU Series: {gpu_series}")
            print(f"interface: {interface}")
            print(f"Chipset Manufacturer: {Cpu_manfacturer}")
            print(f"Gpu: {Gpu}")
            print(f"Product_link: {Product_link}")
            print(f"Core_clock: {Core_Clock}")
            print(f"Boost_clock: {Boost_Clock}")
            print(f"Memory Size: {memorysize}")
            print(f"formfactor: {form_factor}")
            print("----------------------------")
