import requests
import json
import pandas as pd
import sys

class EdmundsDataScraper:
    def __init__(self, year1, year2, makes, page_number):
        self.url = "https://www.edmunds.com/gateway/api/purchasefunnel/v1/srp/inventory"
        self.headers =  {
    "x-client-action-name": "new_used_car_inventory_srp.searchResults",
    "x-deadline": "1709822540259",
    "x-artifact-version": "2.0.5681",
    "sec-ch-ua-platform": "Windows",
    "sec-ch-ua-mobile": "?0",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "x-edw-page-name": "new_used_car_inventory_srp",
    "Referer": "https://www.edmunds.com/inventory/srp.html",
    "x-referer": "https://www.edmunds.com/inventory/srp.html",
    "x-artifact-id": "venom"
}
        self.year1 = year1
        self.year2 = year2
        self.makes = makes
        self.page_number = page_number
        self.all_data = []
        self.state = "CA"
        self.zip_code = "90001"

    def scrape_data(self):
        for make in self.makes:
            for page_num in range(1, self.page_number):
                querystring = {
                    "dma": "543", "inventoryType": "used,cpo",
                    "lat": "42.435682", "lon": "-72.648379",
                    "pageNum": page_num, "pageSize": "21",
                    "radius": "6000", "zip": self.zip_code,
                    "stateCode": self.state, "fetchSuggestedFacets": "true",
                    "make": make, "year": f"{self.year1}-{self.year2}",
                }

                response = requests.get(self.url, headers=self.headers, params=querystring)
                print(f"Brand: {make}, Page {page_num} status code: {response.status_code}")

                if response.status_code != 200:
                    print(f"Error: Request failed with status code {response.status_code}")
                    break

                data = json.loads(response.text)
                self.all_data.append(data)

        return self.all_data


    def clean_data(self, all_data):
        columns = [
            "vid", "vin", "stockNumber", "type", "sellersComments",
            "inTransit", "listingUrl",
            "dealerInfo.address.city", "dealerInfo.address.stateCode",
            "dealerInfo.address.stateName", "dealerInfo.address.zip", "dealerInfo.address.street", "dealerInfo.productFeatures.verified",
            "prices.displayPrice", "vehicleInfo.mileage", "prices.loan.payment", "prices.baseMsrp",
            "prices.totalMsrp", "vehicleInfo.vehicleColors.exterior.genericName", "vehicleInfo.vehicleColors.interior.genericName",
            "vehicleInfo.partsInfo.driveTrain", "vehicleInfo.partsInfo.cylinders", "vehicleInfo.partsInfo.engineSize",
            "vehicleInfo.partsInfo.engineType", "vehicleInfo.partsInfo.fuelType", "vehicleInfo.partsInfo.transmission",
            "vehicleInfo.styleInfo.make", "vehicleInfo.styleInfo.model", "vehicleInfo.styleInfo.trim", "vehicleInfo.styleInfo.style",
            "vehicleInfo.styleInfo.year", "vehicleInfo.styleInfo.bodyType", "vehicleInfo.styleInfo.vehicleStyle",
            "vehicleInfo.styleInfo.fuel.epaCombinedMPG", "vehicleInfo.styleInfo.fuel.epaCityMPG", "vehicleInfo.styleInfo.fuel.epaHighwayMPG",
            "vehicleInfo.styleInfo.numberOfSeats", "historyInfo.personalUseOnly", "historyInfo.ownerText", "historyInfo.usageType",
            "historyInfo.historyProvider", "historyInfo.salvageHistory", "historyInfo.frameDamage", "historyInfo.lemonHistory",
            "historyInfo.theftHistory", "historyInfo.accidentText", "computedDisplayInfo.priceValidation.dealType"
        ]

        data_list = []

        for data in all_data:
            inventories = data.get("inventories", {}).get("results", [])
            for inventory in inventories:
                data_dict = {}
                for column in columns:
                    value = inventory
                    for key in column.split("."):
                        if isinstance(value, dict):
                            value = value.get(key, {})
                        else:
                            break
                    data_dict[column] = value
                data_list.append(data_dict)

        return pd.DataFrame(data_list)

# # Example usage
# scraper = EdmundsDataScraper(year1=2018, year2=2023, makes=["Toyota", "Honda", "Ford"], page_number=5)
# all_data = scraper.scrape_data()
# cleaned_data = scraper.clean_data(all_data)