# Databricks notebook source
from bs4 import BeautifulSoup
import requests
import pandas as pd

def get_html(url):
    """Send a GET request to a URL and return the page content as a BeautifulSoup object
    
    Args:
        url (str): The URL to send the GET request 
        
    Returns:
        BeautifulSoup: The page content as a BeautifulSoup object
    """
    response = requests.get(url)
    return response.content

html = get_html('https://www.newegg.com/global/uk-en/tools/custom-pc-builder/pl/ID-343?diywishlist=0&isCompability=false')

# Create a BeautifulSoup object from the HTML content
soup = BeautifulSoup(html, 'html.parser')

# Find all div elements with class 'item-cell'
items = soup.find('div', class_='item-cell')

for pc in items:
    gpu_name = pc.find('a', class_='item-title')
print(gpu_name)
