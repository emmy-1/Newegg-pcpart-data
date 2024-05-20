# Import necessary libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
from scraper_utils import extract_text, extract_link, get_html, extract_text_from_selector, find_first_match, get_text


def NeweggDesktopProcessor():

    # Create an empty DataFrame to store the scraped data
    df = pd.DataFrame(columns=["CPU/Processor", "Ratings", "Price", "Model No", "Link", "Brand", "Desktop_Type", "Processor_Series", "Processor_Model", "Socket", "Cores", "Operating_Frequency", "Threads", "Max Turbo Frequency"])

    # Loop through the pages of the website
    for i in range(1, 3):
        # Define the URL of the page
        html_page =f'https://www.newegg.com/global/uk-en/Processors-Desktops/SubCategory/ID-343/Page-{i}'
        # # Send a GET request to the page  and parse the page content with BeautifulSoup

        soup = get_html(html_page)
        # Find all the product items on the page

        pc_part = soup.find_all('div', class_='item-cell')

        # Loop through each product item
        for pc in pc_part:
            cpu_name = extract_text(pc, 'a', 'item-title')
            model_no = extract_text(pc, 'ul', 'item-features')
            ratings = extract_text(pc, 'span', 'item-rating-num', '')
            strongprice = extract_text(pc, 'li', 'price-current', '')
            link = extract_link(pc, 'a', 'item-title')

            # Send a GET request to the product detail page and
            # Parse the product detail page content with BeautifulSoup
            product_details = get_html(link)

            Brand = extract_text_from_selector(product_details, '#product-details > div:nth-of-type(2) > div:nth-of-type(2) > table:nth-of-type(1) > tbody > tr:nth-of-type(1) > td')
            Desktop_Type = extract_text_from_selector(product_details, '#product-details div table tr:nth-of-type(2) td')
            Processor_Series = extract_text_from_selector(product_details, '#product-details div table tr:nth-of-type(3) td')
            Processor_Model = extract_text_from_selector(product_details, '#product-details div table tr:nth-of-type(4) td')
            Socket = extract_text_from_selector(product_details, '#product-details div table:nth-of-type(2) tr td')            
            # Find the number of threads
            td_elements = product_details.select('#product-details td')
            Threads = find_first_match(td_elements, r'\d+-Threads')
            Cores = find_first_match(td_elements, r'(\d+|Quad)-Core')
            Operating_Frequency = find_first_match(td_elements, r'\d+\.?\d*\s*GHz')

            # Find the max turbo frequency
            Max_Turbo_Frequency = None
            count = 0
            for td in product_details.select('#product-details td'):
                if re.search(r'\d+\.?\d*\s*GHz', td.text):
                    count += 1
                    if count == 2:
                        Max_Turbo_Frequency = td
                        break

            # Append the data to the DataFrame
            df = df._append({
                "Model No": model_no.strip(),
                "Cpu/Processor": cpu_name.strip(),
                "Ratings": ratings.strip(),
                "Price": get_text(strongprice),
                "Link": link,
                "Brand": Brand.strip(),
                "Processor_Type": get_text(Desktop_Type),
                "Processor_Model": get_text(Processor_Model),
                "Processor_Series": get_text(Processor_Series),
                "Socket": get_text(Socket),
                "Cores": get_text(Cores),
                "Operating_Frequency": get_text(Operating_Frequency),
                "no_of_tread": get_text(Threads),
                "Max Turbo Frequency": get_text(Max_Turbo_Frequency)
            }, ignore_index=True)
    df.to_csv('DesktopProcessor.csv', index=False)

if __name__ == "__main__":
    NeweggDesktopProcessor()