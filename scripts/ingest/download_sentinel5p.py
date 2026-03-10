import os
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

load_dotenv()


class CopernicusDownloader:
    def __init__(self):
        self.username = os.getenv("COPERNICUS_USERNAME")
        self.password = os.getenv("COPERNICUS_PASSWORD")
        self.token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
        self.catalogue_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
        self.download_url = "https://zipper.dataspace.copernicus.eu/odata/v1/Products"
        self.token = None
        self.token_expiry = None

    def get_token(self):
        if self.token and self.token_expiry and datetime.now() < self.token_expiry:
            print("Using cached token")
            return self.token

        print("Requesting new OAuth2 token...")
        data = {
            "client_id": "cdse-public",
            "username": self.username,
            "password": self.password,
            "grant_type": "password",
        }


        response = requests.post(self.token_url, data=data, timeout=30)
        response.raise_for_status()

        token_data = response.json()
        self.token = token_data["access_token"]
        self.token_expiry = datetime.now() + timedelta(minutes=9)

        print("SUCCESS: Token obtained")
        return self.token

    def search_products(self, start_date, end_date, bbox=None, max_results=10):
        if bbox is None:
            bbox = (-120, 49, -110, 60)

        filter_parts = [
            "Collection/Name eq 'SENTINEL-5P'",
            "contains(Name,'L2__CH4___')",
            f"ContentDate/Start gt {start_date.strftime('%Y-%m-%dT00:00:00.000Z')}",
            f"ContentDate/Start lt {end_date.strftime('%Y-%m-%dT23:59:59.999Z')}",
        ]

        min_lon, min_lat, max_lon, max_lat = bbox
        polygon = f"POLYGON(({min_lon} {min_lat},{max_lon} {min_lat}, \
            {max_lon} {max_lat},{min_lon} {max_lat},{min_lon} {min_lat}))"
        filter_parts.append(f"OData.CSC.Intersects(area=geography'SRID=4326;{polygon}')")

        filter_query = " and ".join(filter_parts)

        params = {
            "$filter": filter_query,
            "$top": max_results,
            "$orderby": "ContentDate/Start desc",
        }

        response = requests.get(self.catalogue_url, params=params, timeout=60)
        response.raise_for_status()

        data = response.json()
        products = data.get("value", [])

        print(f"\nFound {len(products)} products")

        for i, product in enumerate(products, 1):
            print(f"\n[{i}] {product['Name']}")
            print(f"ID: {product['Id']}")
            print(f"Date: {product['ContentDate']['Start']}")
            print(f"Size: {product['ContentLength'] / 1024 / 1024:.1f} MB")

        return products

    def download_product(self, product_id, product_name, output_dir="./data/raw/sentinel5p"):
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, product_name)

        if os.path.exists(output_path):
            print(f"File already exists: {output_path}")
            return output_path

        token = self.get_token()
        url = f"{self.download_url}({product_id})/$value"

        headers = {"Authorization": f"Bearer {token}"}

        response = requests.get(url, headers=headers, stream=True, timeout=300)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)

                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        print(f"\rProgress: {percent:.1f}%", end="")

        print(f"\nDownloaded to {output_path}")
        return output_path


def main():
    print("Sentinel-5P Downloader")

    downloader = CopernicusDownloader()

    search_date = datetime(2025, 7, 14)

    products = downloader.search_products(
        start_date=search_date,
        end_date=search_date + timedelta(days=1),
        max_results=20
    )

    if not products:
        print("No products found.")
        return False

    downloaded_files = []

    for i, product in enumerate(products, 1):
        print(f"\n[{i}/{len(products)}] Processing: {product['Name']}")

        try:
            output_path = downloader.download_product(
                product_id=product['Id'],
                product_name=product['Name']
            )
            downloaded_files.append(output_path)

        except Exception as e:
            print(f"ERROR: Failed to download product {i}: {e}")
            continue

    if downloaded_files:
        print("\nDownloaded files:")
        for file in downloaded_files:
            print(f"  - {os.path.basename(file)}")

    return True


if __name__ == "__main__":
    try:
        success = main()
        exit(0 if success else 1)
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        import traceback

        traceback.print_exc()
        exit(1)
