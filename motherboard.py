# Import necessary libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
from scraper_utils import extract_text, extract_link, get_html, extract_text_from_selector, get_text,extract_detail

def getmotherboard():

    # Create an empty DataFrame to store the scraped data
    df = pd.DataFrame(columns=['name', 'ratings', 'strongprice', 'model_no', 'link', 'brand', 'socket', 
    'model', 'type', 'chipset', 'max_memory', 'memory', 'stanard', 'form_factor', 
    'pci', 'm_2', 'sata', 'onboard_audio', 'wireless', 'windows', 'date'
    ])

    # Loop through the pages of the website
    for i in range(1, 12):
        # Define the URL of the page
        html_page =f'https://www.newegg.com/global/uk-en/p/pl?N=101582667&page={i}'

        # # Send a GET request to the page  and parse the page content with BeautifulSoup
        soup = get_html(html_page)
        part = soup.find_all('div', class_='item-cell')

        # Loop through each product item
        for pc in part:
            model_no = extract_text(pc, 'ul', 'item-features')
            name = extract_text(pc, 'a', 'item-title')
            ratings = extract_text(pc, 'span', 'item-rating-num', '')
            strongprice = extract_text(pc, 'li', 'price-current', '')
            link = extract_link(pc, 'a', 'item-title')

            # Send a GET request to the product detail page and
            product_details = get_html(link)

            brand = extract_detail(product_details, 'Brand')
            socket = extract_detail(product_details, 'CPU Socket Type')
            model = extract_detail(product_details, 'Model')
            type = extract_detail(product_details, 'CPU Type')
            chipset = extract_detail(product_details, 'Chipset')
            max_memory = extract_detail(product_details, 'Maximum Memory Supported')
            memory = extract_detail(product_details, 'Memory Standard')
            stanard = extract_detail(product_details, 'Channel Supported')
            form_factor = extract_detail(product_details, 'Form Factor')
            pci = extract_detail(product_details, 'PCI Express')
            m_2 = extract_detail(product_details, 'M.2')
            sata = extract_detail(product_details, 'SATA 6Gb/s')
            onboard_audio = extract_detail(product_details, 'Audio Channels')
            wireless = extract_detail(product_details, 'Wireless LAN')
            windows = extract_detail(product_details, 'Windows 11')
            date = extract_detail(product_details, 'Date First Available')
                  
            
            # Append the data to the DataFrame
            df = df._append({
            'name': name, 'ratings': ratings, 'strongprice': strongprice, 'model_no': model_no , 'link': link, 
            'brand': brand, 'socket': socket, 'model': model, 'type': type, 'chipset': chipset, 
            'max_memory': max_memory, 'memory': memory, 'stanard': stanard, 'form_factor': form_factor, 
            'pci': pci, 'm_2': m_2, 'sata': sata, 'onboard_audio': onboard_audio, 'wireless': wireless, 
            'windows': windows, 'date': date
            }, ignore_index=True)

    # Write the DataFrame to a CSV file
    df.to_csv('motherboard.csv', index=False)


if __name__ == "__main__":
    getmotherboard()