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
    return reponse.text



for page in range(1,9):
    page_size = 36  # Number of items per page
    if page == 1:
        Html_ = get_html(f'https://www.newegg.com/global/uk-en/GPUs-Video-Graphics-Cards/SubCategory/ID-48')
    else:
        Html_ = get_html(f'https://www.newegg.com/global/uk-en/GPUs-Video-Graphics-Cards/SubCategory/ID-48/Page-{page}')
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

        product = requests.get(Product_link).text
        productsoup = BeautifulSoup(product, 'html.parser')  

        Brand = productsoup.select_one('#product-details div table tbody tr td')

        # Extract text from the specific cell, if it exists
        specific_cell_text = Brand.text if Brand else 'NA'

        

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


        print(f"Gpu_name: {Gpu_name}")
        print(f"Num_rating: {No_rating}")
        print(f"ratings: {ratings}")
        print(f"price: {price}")
        print(f"Stickthrough: {stickthrough_price}")
        print(f"Product_link: {Product_link}")
        print(f"Brand: {specific_cell_text}")
        print("-----------------------------------------------------")