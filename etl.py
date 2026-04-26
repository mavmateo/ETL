import csv
import requests
import sqlite3
import json
from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class DataPipeLine:

    def __init__(self, source_url:str , db_path:str = "pipeline_output.db"):
        self.source_url = source_url
        self.db_path = db_path
        self.raw_data =  []
        self.transformed_data = []


    def extract(self , source_url : str) -> List[Dict] :
        print(f"[EXTRACT]Extracting data from {source_url}...")
        response = requests.get(source_url, timeout = 10)
        response.raise_for_status()
        self.raw_data = response.json()
        print(f"Data extraction completed with {len(self.raw_data)} records.")


        return self.raw_data
           

    def transform(self, data: list[Dict]) -> List[Dict]:
         print("[TRANSFORM]Transforming extracted  data...")
         self.transformed_data = []

         for record in data:
             transformed = {
                    "id": record.get("id").strip(),
                    "name": record.get("name", "").strip().title(),
                    "email": record.get("email","").strip().lower(),
                    "userame": record.get("username","").strip().lower(),
                    "city": record.get("address", "").get("city", "Unknown"),
                    "company": record.get("company", "").get("name", "Unknown"),
             }

         if not transformed["id"] or not transformed["email"]:
            print(f"Skipping record with missing id or email: {record}")
            

         return self.transformed_data
    

    def load(self, data: list[Dict]) -> None:
        print(f"[LOAD]Loading uploading data into SQLite database at {self.db_path}...")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS users (
                           id       TEXT PRIMARY KEY,
                           name     TEXT,
                           email    TEXT UNIQUE,
                           username TEXT,
                           city     TEXT,
                           company  TEXT
                       )
                       """)
        inserted,skipped = 0,0
        for record in data:
            try:
                cursor.execute("""
                               INSERT or REPLACE INTO users 
                               (id, name, email, username, city, company)
                            
                               VALUES
                              (:id, :name, :email, :username, :city, :company)
                               """, record)
                inserted += 1
            except sqlite3.IntegrityError as e:
                print(f"Skipping duplicate record: {record} - Error: {e}")
                skipped += 1

        conn.commit()
        conn.close()
        print(f"[LOAD]Data loading completed. {inserted} records inserted, {skipped} records skipped due to duplicates.")     


        def run ( self) -> None:
            print("=" * 50)
            print("[RUN]Starting ETL pipeline...")  
            print("=" * 50)
            raw = self.extract()
            transformed = self.transform(raw)
            self.load(transformed)
            print("=" * 50)
            print("[RUN]ETL pipeline completed successfully.")
            print("=" * 50)


        if __name__ == "__main__":
         source_url = "https://jsonplaceholder.typicode.com/users"
         pipeline = DataPipeLine(source_url , "pipeline_output.db")
         pipeline.run()           