from bs4 import BeautifulSoup
import requests
import pandas as pd
import re

def extract_text(element, tag, class_name, default=None):

    """Extract text from an element using a tag and class name"""
    found_element = element.find(tag, class_=class_name)
    return found_element.text.strip() if found_element else default

def extract_link(element, tag, class_name, default=''):
    """Extract the href link from an element using a tag and class name"""
    found_element = element.find(tag, class_=class_name)
    return found_element['href'] if found_element else default

def get_html(url):
    """Send a GET request to a URL and return the page content as a BeautifulSoup object
    Args:
        url (str): The URL to send the GET request 
    
    """

    html_request = requests.get(url).text
    return BeautifulSoup(html_request, 'lxml')

def extract_text_from_selector(product_details, selector):
    """Extract text from a BeautifulSoup object using a CSS selector and return the text
    Args:
        product_details (BeautifulSoup): The BeautifulSoup object to extract the text from
        selector (str): The CSS selector to use to extract the text"""
    element = product_details.select_one(selector)
    return element.text.strip() if element else ''

def find_first_match(elements, regex):
    """Find the first element that matches a regular expression in a list of elements
    Args:
        elements (list): A list of BeautifulSoup elements to search
        regex (str): The regular expression to search for """
    for element in elements:
        if re.search(regex, element.text):
            return element
    return None

def get_text(element):
    """Extract text from a BeautifulSoup element and return the text
    Args:
        element (BeautifulSoup): The BeautifulSoup element to extract the text from
        """
    if isinstance(element, str):
        return element.strip()
    else:
        return element.text.strip() if element is not None else ""

def extract_detail(soup, word):
    # Find all table elements
    tables = soup.find_all('table')

    for table in tables:
        # Find all tr elements in the table
        rows = table.find_all('tr')

        for row in rows:
            # Find the th element in the row
            th = row.find('th')

            # If the th element was found and its text matches the word
            if th and word in th.text:
                # Find the td element in the row
                td = row.find('td')

                # If the td element was found, return its text
                if td:
                    return td.text.strip()

    # If no matching th element was found, return None
    return None