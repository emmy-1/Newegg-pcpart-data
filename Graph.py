from bs4 import BeautifulSoup
import requests
import pandas as pd

# Create an empty DataFrame
Graphiccard = pd.DataFrame(columns=["Graphiccard", "Ratings", "Price", "Model No", "Link"])

for i in range(1, 7):
    html_page =f'https://www.newegg.com/global/uk-en/GPUs-Video-Graphics-Cards/SubCategory/ID-48?PageSize={i}'
    html_request = requests.get(html_page).text
    soup = BeautifulSoup(html_request, 'lxml')
    pc_part = soup.find('div', class_='item-cell')

    for pc in pc_part:
        Graphiccard_name = pc.find('a', class_='item-title').text
        model_no = pc.find('ul', class_='item-features').text
        ratings_element = pc.find('span', class_='item-rating-num')
        ratings = ratings_element.text if ratings_element else 'No ratings'
        price = pc.find('li', class_='price-current')
        strongprice = price.find('strong').text if price else 'No price'
        link = pc.find('a', class_='item-title')['href']

        # Get additional information from the link in the product page
        get_addtional_info = requests.get(link).text
        product_details = BeautifulSoup(get_addtional_info, 'lxml')
        model_spec = pc.find('td', class_='table-horizontal')


        print(f"Graphiccard: {Graphiccard_name.strip()}")
        print(f"model_spec: {model_spec.strip()}")
        #print(f"Ratings: {ratings.strip()}")
        #print(f"Price: {strongprice.strip()}")
        #print(f"Model No: {model_no.strip()}")
        #print(f"Link: {link}")



        # Append the data to the DataFrame
        #Graphiccard = Graphiccard._append({"Graphiccard": Graphiccard_name.strip(), "Ratings": ratings.strip(), "Price": strongprice.strip(), "Model No": model_no.strip(), "Link": link}, ignore_index=True)


# Write the DataFrame to a CSV file
#Graphiccard.to_csv('graphiccard.csv', index=False)