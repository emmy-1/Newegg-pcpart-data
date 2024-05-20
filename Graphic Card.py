# Import necessary libraries:
    # - BeautifulSoup: For parsing HTML content
    # - requests: For sending HTTP requests
    # - pandas: For creating and manipulating DataFrames
from bs4 import BeautifulSoup
import requests
import pandas as pd
from scraper_utils import extract_text, extract_link, get_html, extract_text_from_selector, get_text

def NeweggGraphicCard():
# Create an empty DataFrame for storing graphic card details.
    Graphiccard = pd.DataFrame(columns=[
        "Graphiccard", "Brand", "Ratings", "Price", "Model No", "Link",
        "Series", "Interface", "Chipset", "GPU_Series", "GPU",
        "Aritecture", "core_clock", "Boost_Clock", "Memory_Type", "review"
    ])

    # Loop through the pages of the website. The Graphic card page has a total of 7 pages which will be looped through.
    for i in range(1, 7):
        # Define the URL of the page. The page size is set to i because the page size changes for each page.
        html_page = f'https://www.newegg.com/global/uk-en/GPUs-Video-Graphics-Cards/SubCategory/ID-48?PageSize={i}'
        
        # Send a GET request to the page and parse the page content with BeautifulSoup
        soup = get_html(html_page)
        # Find all graphic card items on the page as the loop continues
        pc_part = soup.find_all('div', class_='item-cell')

        # Loop through each graphic card item
        for pc in pc_part:
            # Extract the necessary details from the item

            # Extract the coressponding details from the page using the extract_text and 
            # extract_link functions, element and class name
            Graphiccard_name = extract_text(pc, 'a', 'item-title')
            model_no = extract_text(pc, 'ul', 'item-features')
            ratings = extract_text(pc, 'span', 'item-rating-num', 'Null')
            price = pc.find('li', class_='price-current')
            strongprice = price.find('strong').text if price else 'Null'
            link = extract_link(pc, 'a', 'item-title')

            # # Extract the graphic card name which is located in class 'item-title'
            # Graphiccard_name = pc.find('a', class_='item-title').text

            # # Extract the model number which is located in class 'item-features'
            # model_no = pc.find('ul', class_='item-features').text

            # # Extract the ratings which is located in class 'item-rating-num'
            # ratings_element = pc.find('span', class_='item-rating-num')

            # # Extract the price which is located in class 'price-current'
            # ratings = ratings_element.text if ratings_element else 'Null'

            # # Extract the price which is located in class 'price-current'
            # price = pc.find('li', class_='price-current')

            # # Extract the price which is located in class 'price-current'
            # strongprice = price.find('strong').text if price else 'Null'
            # # Extract the link to the product page which is located in class 'item-title 
            # # with a herf link into the product itself
            # link = pc.find('a', class_='item-title')['href']

            # Send a GET request to the product page to get additional details
            # get_addtional_info = requests.get(link).text
            product_details = get_html(link)
            
            # Extract the additional details from the product page using select_one and xpath selectors.

            Brand = extract_text_from_selector(product_details,'#product-details div table tbody tr td')
            Series = extract_text_from_selector(product_details,'#product-details div table tbody tr:nth-of-type(2) td')
            Interface = extract_text_from_selector(product_details,'#product-details div table:nth-of-type(2) tbody tr td')
            Chipset = extract_text_from_selector(product_details,'#product-details div table:nth-of-type(3) tbody tr:nth-of-type(1) td')
            GPU_Series = extract_text_from_selector(product_details,'#product-details div table:nth-of-type(3) tbody tr:nth-of-type(2) td')
            GPU = extract_text_from_selector(product_details,'#product-details div table:nth-of-type(3) tbody tr:nth-of-type(3) td')


            # Brand = product_details.select_one('#product-details div table tbody tr td')
            # Series = product_details.select_one('#product-details div table tbody tr:nth-of-type(2) td')
            # Interface = product_details.select_one('#product-details div table:nth-of-type(2) tbody tr td')
            # Chipset = product_details.select_one('#product-details div table:nth-of-type(3) tbody tr:nth-of-type(1) td')
            # GPU_Series = product_details.select_one('#product-details div table:nth-of-type(3) tbody tr:nth-of-type(2) td')
            # GPU = product_details.select_one('#product-details div table:nth-of-type(3) tbody tr:nth-of-type(3) td')

            # Extract the core clock and boost clock from the product page
            td_elements = product_details.select('#product-details td')

            # Find all the clock values in the product details
            clocks = [td for td in td_elements if 'MHz' in td.text]
            # Extract the core clock and boost clock values
            if len(clocks) >= 2:
                # Extract the clock values and find the maximum and minimum values
                clock_values = [int(''.join(filter(str.isdigit, clock.text))) for clock in clocks]
                max_clock_index = clock_values.index(max(clock_values))
                min_clock_index = clock_values.index(min(clock_values))
                # assign the core clock and boost clock values
                Boost_Clock = clocks[max_clock_index]
                Core_Clock = clocks[min_clock_index]

            # Extract the memory size from the product page
            td_elements = product_details.select('#product-details td')
            Memory_Size = next((td for td in td_elements if 'GB' in td.text), None)

            # Extract the memory type from the product page
            td_elements_MT = product_details.select('#product-details td')
            Memory_Type = next((mt for mt in td_elements_MT if 'DDR' in mt.text or 'SDRAM' in mt.text), None)

            # Append the data to the DataFrame

            Graphiccard = Graphiccard._append({
                "Graphiccard": Graphiccard_name.strip(), 
                "Brand": Brand.strip(),
                "Ratings": ratings.strip(), 
                "Price": strongprice.strip(), 
                "Model No": model_no.strip(), 
                "Link": link,
                "Series": Series.strip(),
                "Interface": Interface.strip(),
                "Chipset": Chipset.strip(),
                "GPU_Series": GPU_Series.strip(),
                "GPU": get_text(GPU),
                "Core_clock": Core_Clock.text.strip(),
                "Boost_Clock": get_text(Boost_Clock),
                "Memory_Size": get_text(Memory_Size),
                "Memory_Type": get_text(Memory_Type),
            }, ignore_index=True)

    # Save the DataFrame to a CSV file
    Graphiccard.to_csv('graphiccards.csv', index=False)
# Run the function if the script is executed
if __name__ == "__main__":
    NeweggGraphicCard()