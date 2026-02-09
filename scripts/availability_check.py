# check availability of data

import requests
from datetime import datetime, timedelta

import os
from pathlib import Path
from dotenv import load_dotenv


load_dotenv()

def test_copernicus_auth():
    """
    Check on connection 
    """
    username = os.getenv("COPERNICUS_USR")
    password = os.getenv("COPERNICUS_PWD")
    
    
    
    token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password"
    }
    
    response = requests.post(token_url, data=data, timeout=30)
    if response.status_code == 200:
        token = response.json()["access_token"]
        print(f"Copernicus auth successful")
        return token
    else:
        print(f"Auth failed: {response.status_code}")
        print(response.text)
        return None
    
def search_sentinel5p(token):
    """Search Sentinel-5P CH4 products over Alberta"""
    date_end = datetime.now()
    date_start = date_end - timedelta(days=10)
    
    search_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
    
    filter_query = (
        f"Collection/Name eq 'SENTINEL-5P' and "
        f"contains(Name,'L2__CH4___') and "
        f"ContentDate/Start gt {date_start.strftime('%Y-%m-%dT%H:%M:%S.000Z')} and "
        f"ContentDate/Start lt {date_end.strftime('%Y-%m-%dT%H:%M:%S.000Z')} and "
        f"OData.CSC.Intersects(area=geography'SRID=4326;POLYGON((-120 49,-110 49,-110 60,-120 60,-120 49))')"
    )
    
    params = {"$filter": filter_query, "$top": 5}
    
    response = requests.get(search_url, params=params, timeout=60)
    
    if response.status_code == 200:
        products = response.json().get('value', [])
        print(f"Found {len(products)} Sentinel-5P products")
        if products:
            print(f"   First: {products[0]['Name']}")
            print(f"   Date: {products[0]['ContentDate']['Start']}")
        return True
    else:
        print(f"ERROR Search failed: {response.status_code}")
        return False

if __name__ == "__main__":
    print("Testing Copernicus authentication...")
    token = test_copernicus_auth()

    if token:
        print("Searching Sentinel-5P products...")
        search_sentinel5p(token)