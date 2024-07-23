import requests
import json
import pandas as pd
import numpy as np
import warnings 
warnings.filterwarnings("ignore")
import sqlite3
from datetime import datetime

class EdmundsDataScraper:
    def __init__(self, year1, year2, makes, page_number):
        self.url = "https://www.edmunds.com/gateway/api/purchasefunnel/v1/srp/inventory"
        self.headers = {
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
            for page_num in range(1, self.page_number+1):
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

    def clean_data(self, raw_data):
        """
        Ham veriyi temizler ve işler.
        
        :param raw_data: Temizlenecek ham veri (liste formatında)
        :return: Temizlenmiş pandas DataFrame
        """
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

        for data in raw_data:
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

        df = pd.DataFrame(data_list)

        # İkinci temizleme fonksiyonundan alınan işlemler
        columns_to_drop = ["vid", "vin", "listingUrl", "dealerInfo.address.street",
                           "dealerInfo.productFeatures.verified", "dealerInfo.address.stateName",
                           "stockNumber", "inTransit", "vehicleInfo.styleInfo.fuel.epaCombinedMPG",
                           "vehicleInfo.styleInfo.fuel.epaCityMPG", "vehicleInfo.styleInfo.fuel.epaHighwayMPG", "historyInfo.personalUseOnly"]

        replace_dict = {
            "prices.displayPrice": np.nan,
            "vehicleInfo.vehicleColors.exterior.genericName": np.nan,
            "vehicleInfo.vehicleColors.interior.genericName": np.nan,
            "historyInfo.historyProvider": "USER",
            "vehicleInfo.mileage": np.nan,
            "historyInfo.usageType": "Unknown_Usage_Type",
            "prices.baseMsrp": np.nan,
            "prices.totalMsrp": np.nan,
            "prices.loan.payment": np.nan,
            "historyInfo.ownerText": "Unknown",
            "sellersComments": np.nan
        }

        for col, value in replace_dict.items():
            if col in df.columns:
                df[col] = df[col].replace("{}", value)

        if all(col in df.columns for col in ["vehicleInfo.partsInfo.engineSize", "vehicleInfo.partsInfo.fuelType", "vehicleInfo.styleInfo.style"]):
            df.loc[df["vehicleInfo.partsInfo.engineSize"].str.contains(r"\{\}") & 
                (df["vehicleInfo.partsInfo.fuelType"] == "{}") & 
                df["vehicleInfo.styleInfo.style"].str.contains(r'\d+cyl'), 
                "vehicleInfo.partsInfo.engineType"] = "gas"

        float_columns = ["prices.displayPrice", "vehicleInfo.mileage", "prices.totalMsrp", "prices.baseMsrp", "prices.loan.payment"]
        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
        
        if "prices.displayPrice" in df.columns and "vehicleInfo.mileage" in df.columns:
            df = df[~df["prices.displayPrice"].isna()]
            df = df[~df["vehicleInfo.mileage"].isna()]

        def process_column(df, column, group_cols):
            if column not in df.columns:
                return df
            
            df[column] = df[column].replace("{}", np.nan)
            
            is_numeric = pd.api.types.is_numeric_dtype(df[column])
            
            if is_numeric:
                df[column] = df.groupby(group_cols)[column].transform(
                    lambda x: x.fillna(x.median())
                )
                df[column] = df[column].fillna(df[column].median())
            else:
                df[column] = df.groupby(group_cols)[column].transform(
                    lambda x: x.fillna(x.mode().iloc[0] if not x.mode().empty else np.nan)
                )
                overall_mode = df[column].mode().iloc[0] if not df[column].mode().empty else "Unknown"
                df[column] = df[column].fillna(overall_mode)
            
            return df

        group_cols = ['vehicleInfo.styleInfo.make', 'vehicleInfo.styleInfo.model', 'vehicleInfo.styleInfo.year']

        columns_to_process = [
            'vehicleInfo.vehicleColors.exterior.genericName', 'vehicleInfo.vehicleColors.interior.genericName',
            'vehicleInfo.partsInfo.driveTrain', 'vehicleInfo.partsInfo.cylinders', 'vehicleInfo.partsInfo.engineSize',
            'vehicleInfo.partsInfo.engineType', 'vehicleInfo.partsInfo.fuelType', 'vehicleInfo.partsInfo.transmission',
            'vehicleInfo.styleInfo.trim', 'vehicleInfo.styleInfo.style', 'vehicleInfo.styleInfo.year',
            'vehicleInfo.styleInfo.bodyType', 'vehicleInfo.styleInfo.vehicleStyle', 'vehicleInfo.styleInfo.numberOfSeats',
            'historyInfo.ownerText', 'historyInfo.usageType', 'historyInfo.historyProvider', 'historyInfo.salvageHistory',
            'historyInfo.frameDamage', 'historyInfo.lemonHistory', 'historyInfo.theftHistory', 'historyInfo.accidentText',
            'computedDisplayInfo.priceValidation.dealType', 'prices.displayPrice', 'prices.loan.payment',
            'prices.baseMsrp', 'prices.totalMsrp', 'vehicleInfo.mileage'
        ]

        for column in columns_to_process:
            df = process_column(df, column, group_cols)
            if column in df.columns:
                print(f"Processed {column}. NaN count: {df[column].isna().sum()}")

        print("Processing complete.")
        return df

    def scrape_and_clean(self):
        raw_data = self.scrape_data()
        return self.clean_data(raw_data)



    def send_to_database(self, df, db_path="edmunds_database.db", if_exists='replace'):
    
        def preprocess_for_sqlite(df):
            for column in df.columns:
                if df[column].apply(lambda x: isinstance(x, dict)).any():
                    df[column] = df[column].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
            return df

        try:
            
            df = preprocess_for_sqlite(df)

            conn = sqlite3.connect(db_path)
            
            table_name = f"edmunds_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Write the preprocessed DataFrame to SQLite
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            
            conn.commit()
            print(f"Data successfully written to table '{table_name}' in {db_path}")
            return table_name  
        except sqlite3.Error as e:
            print(f"SQLite error occurred: {str(e)}")
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")
        finally:
            if conn:
                conn.close()