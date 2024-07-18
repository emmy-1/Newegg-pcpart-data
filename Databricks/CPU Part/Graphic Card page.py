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


for page in range(1, 14):
    if page == 1:
        Html_ = get_html(f'https://www.newegg.com/global/uk-en/tools/custom-pc-builder/pl/ID-48?diywishlist=0&isCompability=false')
    else:
        Html_ = get_html(f'https://www.newegg.com/global/uk-en/tools/custom-pc-builder/pl/ID-48/Page-{page}?diywishlist=0&isCompability=false')
    
    soup = BeautifulSoup(Html_, 'html.parser')
    table = find_element(soup, 'table', 'table-vertical')

    if table is not None:
        td_elements = table.select('td')

        for td in td_elements:
            item_data = {}  # Dictionary to store the data for each item

            title = find_element(td, 'div', 'item-title')
            if title is not None:
                title_span = title.find('span')
                if title_span is not None:
                    item_data['Title'] = title_span.text.strip()
                    link = title.find('a')
                    if link is not None:
                        item_data['Link'] = link['href']

            div = td.find('div', class_='hid-text')
            span = td.find('span')
            if div is not None and span is not None:
                label = div.text.strip()
                value = span.text.strip()
                if label == 'GPU':
                    item_data['GPU'] = value
                elif label == 'Memory':
                    item_data['Memory'] = value
                elif label == 'Suggested PSU':
                    item_data['Suggested PSU'] = value
                elif label == 'Length':
                    item_data['Length'] = value

            rating_element = td.find('span', class_='item-rating-num')
            if rating_element is not None:
                item_data['Ratings'] = rating_element.text.strip()

            price = td.find('li', class_='price-current')
            if price is not None:
                item_data['Price'] = price.find('strong').text.strip()

            image = td.find('div', class_='item-img hover-item-img')
            if image is not None:
                item_data['Image URL'] = image.find('img')['src']

            tips = find_element(td, 'div', 'item-tips')
            if tips is not None:
                item_data['Tips'] = tips.text.strip()
                print('----------------------------')

            # Print the data for each item
            for key, value in item_data.items():
                print(f"{key}: {value}")
            print()

# COMMAND ----------

tablep= table.prettify()
print(tablep)
