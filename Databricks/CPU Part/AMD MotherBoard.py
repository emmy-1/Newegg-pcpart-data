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


for page in range(1, 8):
    if page == 1:
        Html_ = get_html(f'https://www.newegg.com/global/uk-en/tools/custom-pc-builder/pl/ID-22?diywishlist=0')
    else:
        Html_ = get_html(f'https://www.newegg.com/global/uk-en/tools/custom-pc-builder/pl/ID-22/Page-{page}?diywishlist=0')

    
    soup = BeautifulSoup(Html_, 'html.parser')
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
                    title = title_span.text.strip()
                print(f"Title: {title}")

            if div is not None and span is not None:
                label = div.text.strip()
                value = span.text.strip()

                if label == 'Socket / CPU':
                    Socket_cpu = value
                    print(f"Socket / CPU: {Socket_cpu}")
                elif label == 'Form Factor':
                    Form_Factor = value
                    print(f"Form Factor: {Form_Factor}")
                elif label == 'Memory Slots':
                    Memory_Slots = value
                    print(f"MemorySlots Type: {Memory_Slots}")
                elif label == 'Memory Max':
                    Memory_max = value
                    print(f"TDP: {Memory_max}")
                elif label == 'Chipset':
                    Chipset = value
                    print(f"Chipset: {Chipset}")
            if rating_element is not None:
                    ratings = rating_element.text.strip()
                    print(f"Ratings: {ratings}")

            if price is not None and price.find('strong') is not None:
                price_value = price.find('strong').text.strip()
                print(f"Price: {price_value}")

            if image is not None and image.find('img') is not None:
                image_url = image.find('img')['src']
                print(f"Image URL: {image_url}")

            if tips is not None:
                tips_value = tips.text.strip()
                print(f"Tips: {tips_value}")
                print('-----------------------------------')
