# Databricks notebook source
from bs4 import BeautifulSoup
import requests

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

    Html_= get_html(f'https://www.newegg.com/global/uk-en/tools/custom-pc-builder/pl/ID-48?isCompability=false')
    
    # Create a BeautifulSoup object from the HTML content
    soup = BeautifulSoup(Html_, 'html.parser')

    # Find the table element with class 'table-vertical'
    table = find_element(soup, 'table', 'table-vertical')
    
    if table is not None:
        td_elements = table.select('td')

        for td in td_elements:
            div = td.find('div', class_='hid-text')
            span = td.find('span')
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
                    print(f"Title: {title_value}")
                    link = title_div.find('a')
                    if link is not None:
                        link = link['href']
                        print(f"Link: {link}")

            if div is not None and span is not None:
                label = div.text.strip()
                value = span.text.strip()

                if label == 'GPU':
                    Gpu = value
                    print(f"GPU: {Gpu}")
                elif label == 'Memory':
                    Memory = value
                    print(f"Memory: {Memory}")
                elif label == 'Suggested PSU':
                    PSU = value
                    print(f"Suggested PSU: {PSU}")
                elif label == 'Length':
                    length = value
                    print(f"length: {length}")
              
            if rating_element is not None:
                ratings = rating_element.text.strip()
                print(f"Ratings: {ratings}")

            if price is not None:
                price_value = price.find('strong').text.strip()
                print(f"Price: {price_value}")

            if image is not None:
                image_url = image.find('img')['src']
                print(f"Image URL: {image_url}")

            if tips is not None:
                tips_value = tips.text.strip()
                print(f"Tips: {tips_value}")
                print('-----------------------------------')
    else:
        print("Table element not found on the page.")

# COMMAND ----------

from bs4 import BeautifulSoup
import requests

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

    Html_= get_html(f'https://www.newegg.com/global/uk-en/tools/custom-pc-builder/pl/ID-48?isCompability=false')
    
    # Create a BeautifulSoup object from the HTML content
    soup = BeautifulSoup(Html_, 'html.parser')
    print(soup)
