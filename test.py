from bs4 import BeautifulSoup
import requests
import pandas as pd

# Create an empty DataFrame
Graphiccard = pd.DataFrame(columns=["Graphiccard", "Ratings", "Price", "Model No", "Link"])


html_page =f'https://www.newegg.com/global/uk-en/GPUs-Video-Graphics-Cards/SubCategory/ID-48?PageSize=%7Bi%7D'
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
    Brand = product_details.select_one('#product-details div table tbody tr td')
    Series = product_details.select_one('#product-details div table tbody tr:nth-of-type(2) td')
    Interface = product_details.select_one('#product-details div table:nth-of-type(2) tbody tr td')
    Chipset = product_details.select_one('#product-details div table:nth-of-type(3) tbody tr:nth-of-type(1) td')
    GPU_Series = product_details.select_one('#product-details div table:nth-of-type(3) tbody tr:nth-of-type(2) td')
    GPU = product_details.select_one('#product-details div table:nth-of-type(3) tbody tr:nth-of-type(3) td')
    Aritecture = product_details.select_one('#product-details div table:nth-of-type(3) tbody tr:nth-of-type(4) td')
    core_clock = product_details.select_one('#product-details div table:nth-of-type(3) tbody tr:nth-of-type(5) td')
    Boost_Clock = product_details.select_one('#product-details div table:nth-of-type(3) tbody tr:nth-of-type(7) td')
    Memory_Size = product_details.select_one('#product-details div table:nth-of-type(4) tbody tr:nth-of-type(2) td')
    Memory_Type = product_details.select_one('#product-details div table:nth-of-type(4) tbody tr:nth-of-type(4) td')
    review = product_details.select_one('#product-details div:nth-of-type(3) div:nth-of-type(6) div:nth-of-type(1) div:nth-of-type(2) div div:nth-of-type(2) span:nth-of-type(1)')
    
    print(f"Graphiccard: {Graphiccard_name.strip()}")
    print(f"Brand: {Brand.text.strip()}")
    print(f"Ratings: {ratings.strip()}")
    print(f"Price: {strongprice.strip()}")
    print(f"Model No: {model_no.strip()}")
    print(f"Link: {link}")
    print(f"Series: {Series.text.strip()}")
    print(f"Interface: {Interface.text.strip()}")
    print(f"Chipset: {Chipset.text.strip()}")
    print(f"GPU_Series: {GPU_Series.text.strip()}")
    print(f"GPU: {GPU.text.strip()}")
    print(f"Aritecture: {Aritecture.text.strip()}")
    print(f"core_clock: {core_clock.text.strip()}")
    print(f"Boost_Clock: {Boost_Clock.text.strip()}")
    print(f"Memory_Size: {Memory_Size}")
    print(f"Memory_Type: {Memory_Type.text.strip()}")
    print(f"review: {review}")
   



   # Append the data to the DataFrame
   #Graphiccard = Graphiccard._append({"Graphiccard": Graphiccard_name.strip(), "Ratings": ratings.strip(), "Price": strongprice.strip(), "Model No": model_no.strip(), "Link": link}, ignore_index=True)


# Write the DataFrame to a CSV file
#Graphiccard.to_csv('graphiccard.csv', index=False)


# Desktop

from bs4 import BeautifulSoup
import requests
import pandas as pd
import re


# Create an empty DataFrame
df = pd.DataFrame(columns=["CPU/Processor", "Ratings", "Price", "Model No", "Link", "Brand", "Desktop_Type", "Processor_Series", "Processor_Model", "Socket", "Cores", "Operating_Frequency", "Threads", "Max Turbo Frequency"])

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

        get_additional_info = requests.get(link).text
        product_details = BeautifulSoup(get_additional_info, 'lxml')

        Brand = Brand = product_details.select_one('#product-details > div:nth-of-type(2) > div:nth-of-type(2) > table:nth-of-type(1) > tbody > tr:nth-of-type(1) > td').text
        Desktop_Type = product_details.select_one('#product-details div table tr:nth-of-type(2) td')
        Processor_Series = product_details.select_one('#product-details div table tr:nth-of-type(3) td')
        Processor_Model = product_details.select_one('#product-details div table tr:nth-of-type(4) td')
        Socket = product_details.select_one('#product-details div table:nth-of-type(2) tr td')
        no_of_tread = Processor_Model = product_details.select_one('#product-details > div:nth-of-type(2) > div:nth-of-type(2) > table:nth-of-type(2) > tbody > tr:nth-of-type(4) > td')
        # FINDING THE NUMBER OF THREADS
        Threads = None
        for td in product_details.select('#product-details td'):
            if re.search(r'\d+-Threads', td.text):
                Threads = td
                break



        #no_of_cores
        Cores = None
        for td in product_details.select('#product-details td'):
            if re.search(r'(\d+|Quad)-Core', td.text):
                Cores = td
                break
        

        #operating frequency
        Operating_Frequency = None
        for td in product_details.select('#product-details td'):
            if re.search(r'\d+\.?\d*\s*GHz', td.text):
                Operating_Frequency = td
                break
        
        
        
    

        Max_Turbo_Frequency = None
        count = 0
        for td in product_details.select('#product-details td'):
            if re.search(r'\d+\.?\d*\s*GHz', td.text):
                count += 1
                if count == 2:
                    Max_Turbo_Frequency = td
                    break
        
        
        print("Cpu/Processor: ", cpu_name.strip())
        print("Ratings: ", ratings.strip())
        print("Price: ", strongprice.strip())
        print("Model No: ", model_no.strip())
        print("Link: ", link)
        print(f"Brand: {Brand.strip()}")
        print(f"Processor_Type: {Desktop_Type.text.strip()}")
        print(f"Processor_Series: {Processor_Series.text.strip()}")
        print(f"Processor_Model: {Processor_Model.text.strip()}")
        print(f"Socket: {Socket.text.strip()}")
        if Cores is not None:
            print(f"Cores: {Cores.text.strip()}")
        else:
            print("Cores: ")

        if Operating_Frequency is not None:
            print(f"Operating_Frequency: {Operating_Frequency.text.strip()}")
        else:
            print("Operating_Frequency: ")
        print(f"Brand: {Brand}")
        if Threads is not None:
            print(f"no_of_tread: {Threads.text.strip()}")
        else:
            print("Threads: ")

        if Max_Turbo_Frequency is not None:
            print(f"Max Turbo Frequency: {Max_Turbo_Frequency.text.strip()}")
        else:
            print("Max Turbo Frequency: ")
        # Append the data to the DataFrame
        # df = df._append({
        #     "CPU/Processor": cpu_name.strip(),
        #     "Ratings": ratings.strip(),
        #     "Price": strongprice.strip(),
        #     "Model No": model_no.strip(),
        #     "Link": link,
        #     "Brand": Brand.strip(),
        #     "Desktop_Type": Desktop_Type.text.strip(),
        #     "Processor_Series": Processor_Series.text.strip(),
        #     "Processor_Model": Processor_Model.text.strip(),
        #     "Socket": Socket.text.strip(),
        #     "Cores": Cores.text.strip() if Cores else "",
        #     "Operating_Frequency": Operating_Frequency.text.strip() if Operating_Frequency else "",
        #     "Threads": Threads.text.strip() if Threads else "",
        #     "Max Turbo Frequency": Max_Turbo_Frequency.text.strip() if Max_Turbo_Frequency else ""        
        # }, ignore_index=True)

# # Write the DataFrame to a CSV file
# df.to_csv('pc_parts.csv', index=False)




def safe_get_text(element):
    if element is not None:
        return element.text.strip()
    else:
        return "Not found"

# Use the function like this:
print(f"Processor_Type: {safe_get_text(Desktop_Type)}")
print(f"Socket: {safe_get_text(Socket)}")
print(f"Cores: {safe_get_text(Cores)}")
print(f"Operating_Frequency: {safe_get_text(Operating_Frequency)}")
print(f"Brand: {safe_get_text(Brand)}")
print(f"no_of_tread: {safe_get_text(Threads)}")
print(f"Max Turbo Frequency: {safe_get_text(Max_Turbo_Frequency)}")