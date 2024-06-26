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
for page in range(1,8):
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

            # get the html link for the product
            get_html(Product_link)
            productsoup = BeautifulSoup(get_html(Product_link), 'html.parser')

            # Extract Product information
            ratings = productsoup.select_one('.rating')['title'] if productsoup.select_one('.rating') else 'Null'
            price = productsoup.find('div', class_='price-current').text if productsoup.find('div', class_='price-current') else 'Null'
            stickthrough_price = productsoup.find('span', class_='price-was-data').text if productsoup.find('span', class_='price-was-data') else 'NA'
            brandname = extract_value_from_specific_table(productsoup, 'table-horizontal', 'Brand', 0)
            Series = extract_value_from_specific_table(productsoup, 'table-horizontal', 'Series', 0)
            gpu_series = extract_value_from_specific_table(productsoup, 'table-horizontal', 'GPU Series', 2) 
            interface = extract_value_from_specific_table(productsoup, 'table-horizontal', 'Interface', 1)
            Cpu_manfacturer = extract_value_from_specific_table(productsoup, 'table-horizontal', 'Chipset Manufacturer', 2)
            Gpu = extract_value_from_specific_table(productsoup, 'table-horizontal', 'GPU', 2)
            core_clock = extract_value_from_specific_table(productsoup, 'table-horizontal', 'Core Clock', 2)
            boost_clock = extract_value_from_specific_table(productsoup, 'table-horizontal', 'Boost Clock', 2)
            memorysize = extract_value_from_specific_table(productsoup, 'table-horizontal', 'Memory Size', 3)
            form_factor = extract_value_from_specific_table(productsoup, 'table-horizontal', 'Form Factor', 7)

            print(f"Model_no: {model_no}")
            print(f"Gpu_name: {Gpu_name}")
            #print(f"Num_rating: {No_rating}")
            #print(f"ratings: {ratings}")
            #print(f"price: {price}")
            #print(f"Stickthrough: {stickthrough_price}")
            #print(f"Brand: {brandname}")
            #print(f"Series: {Series}")
            #print(f"GPU Series: {gpu_series}")
            #print(f"interface: {interface}")
            #print(f"Chipset Manufacturer: {Cpu_manfacturer}")
            #print(f"Gpu: {Gpu}")
            print(f"Product_link: {Product_link}")
            #print(f"Core_clock: {core_clock}")
            #print(f"Boost_clock: {boost_clock}")
            #print(f"Memory Size: {memorysize}")
            #print(f"formfactor: {form_factor}")
            print("----------------------------")

        

# COMMAND ----------

from bs4 import BeautifulSoup

def extract_text(element, tag, class_name, default=None):

    """Extract text from an element using a tag and class name"""
    found_element = element.find(tag, class_=class_name)
    return found_element.text.strip() if found_element else default

item_text = extract_text(Soup, 'a', 'item-title')
display(item_text)

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
    return BeautifulSoup(html_request, 'html')

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
        html_page = f'https://www.newegg.com/global/uk-en/GPUs-Video-Graphics-Cards/SubCategory/ID-48/Page-{i}/'
        
        # Send a GET request to the page and parse the page content with BeautifulSoup
        soup = get_html(html_page)
        # Find all graphic card items on the page as the loop continues
        pc_part = soup.find_all('div', class_='item-cell')
        print(pc_part)



# COMMAND ----------


        # Loop through each graphic card item
        for pc in pc_part:
            # Extract the necessary details from the item

            # Extract the corresponding details from the page using the extract_text and extract_link functions, element and class name
            Graphiccard_name = extract_text(pc, 'a', 'item-title')
            model_no = extract_text(pc, 'ul', 'item-features')
            ratings = extract_text(pc, 'span', 'item-rating-num', 'Null')
            price = pc.find('li', class_='price-current')
            strongprice = price.find('strong').text if price else 'Null'
            link = extract_link(pc, 'a', 'item-title')

            # Append the extracted details to the DataFrame
            Graphiccard = Graphiccard.append({
                "Graphiccard": Graphiccard_name.strip(),
                "Ratings": ratings.strip(),
                "Price": strongprice.strip(),
                "Model No": model_no.strip(),
                "Link": link
            }, ignore_index=True)

    # Display the DataFrame with the extracted details
    display(Graphiccard)
