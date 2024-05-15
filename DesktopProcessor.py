from bs4 import BeautifulSoup
import requests
import pandas as pd

from bs4 import BeautifulSoup
import requests
import pandas as pd

# Create an empty DataFrame
df = pd.DataFrame(columns=["CPU/Processor", "Ratings", "Price", "Model No", "Link"])

for i in range(1, 3):
    html_page =f'https://www.newegg.com/global/uk-en/Processors-Desktops/SubCategory/ID-343/Page-{i}'
    html_request = requests.get(html_page).text
    soup = BeautifulSoup(html_request, 'lxml')
    pc_part = soup.find_all('div', class_='item-cell')

    for pc in pc_part:
        cpu_name = pc.find('a', class_='item-title').text
        model_no = pc.find('ul', class_='item-features').text
        ratings_element = pc.find('span', class_='item-rating-num')
        ratings = ratings_element.text if ratings_element else 'No ratings'
        price = pc.find('li', class_='price-current')
        strongprice = price.find('strong').text if price else 'No price'
        link = pc.find('a', class_='item-title')['href']
        
        # Append the data to the DataFrame
        df = df._append({"CPU/Processor": cpu_name.strip(), "Ratings": ratings.strip(), "Price": strongprice.strip(), "Model No": model_no.strip(), "Link": link}, ignore_index=True)

# Write the DataFrame to a CSV file
df.to_csv('pc_parts.csv', index=False)