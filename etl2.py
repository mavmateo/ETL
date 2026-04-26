import os
import sys
import argparse
from pathlib import Path
import json
import csv

import requests
import sqlite3
from typing import List , Dict
from dataclasses import dataclass


@dataclass
class DataPipeLine:
    #change source url to source
    #make provision to check of directory exists with os.path.isdir
    def __init__(self, source :str, db_path :str = "pipeline_output.db"):
        self.source = source
        self.db_path = db_path
        self.raw_data = []
        self.transformed_data = []
        self.is_local = os.path.isdir(source)
       

    # create function to check which extract type to call
    def extract(self) -> list[dict]:
        if self.is_local:
            return self._extract_from_directory(self.source)
        return self._extract_from_url(self.source)       
     



   
    
    # add len of data extracted to show number of records
    # checking state of extraction is raise_for_status
    def _extract_from_url(self, source_url : str) -> list[dict]:
        print("[EXTRACT], Extracting records from external url......")
        self.raw_data = []
        response = requests.get(source_url, timeout=10)
        response.raise_for_status()
        self.raw_data = response.json()
        print(f"[EXTRACT], Successfully extracted {len(self.raw_data)} records ...")
        return self.raw_data
      
    
    # put raw_data into an empty array
    #go through all the filenames in the directory
    #join filename to directory link one by one for all
    #process json files
    #process csv files
    #skip files of unknown types
    #print total records extracted
    def _extract_from_directory(self, directory: str) -> list[dict]:
         self.raw_data = []
         for filename in os.listdir(directory):
             filepath = os.path.join(directory, filename)

             if filepath.endswith(".json"):
                 with open(filepath , "r", encoding="utf-8") as f:
                    data = json.load(f)
                    records = data if isinstance(data, list) else [data]
                    self.raw_data.extend(records)
                    print(f"[EXTRACT],  {filename} -> {len(records)} record(s)")

             elif filepath.endswith(".csv"):
                with open(filepath, "r" , encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    records = list(reader) 
                    self.raw_data.extend(records)
                    print(f"[EXTRACT], {filename} -> {len(records)} record(s)")

             else:
                print(f"[EXTRACT], skipping unsupported file: {filename} ")     
                return self.raw_data 








       
    

    def transform(self, data :list[dict]) -> list[dict]:
        print("[TRANSFORM], Transforming raw data ........")
        self.transformed_data = []

        for record in data:
         transformed = {
            
                "id"         : record.get("id", "").strip(),
                "name"       : record.get("name", "").strip().title(),
                "email"      : record.get("email", "").strip().lower(),
                "username"   : record.get("username","").strip().lower(),
                "city"       : record.get("address", "").get("city", "Unkwown"),
                "company"    : record.get("company").get("name", "Unkown")
        }

        if not transformed["id"] or not transformed["email"]:
            print(f"[TRANSFORM], Skipping records with missing id or email {records}")



            

    def load(self, data :list[dict]) -> None:
        print(f"[LOAD], loading data into db {self.db_path} ")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id       INTEGER PRIMARY KEY,
                    name     TEXT,
                    email    TEXT UNIQUE,
                    username TEXT,
                    city     TEXT,
                    company  TEXT, 
                )
        """) 
        inserted,skipped = 0,0
        for record in data :
            try:
                cursor.execute("""
                INSERT OR REPLACE INTO users 
                     (id, name, email, username, city, company)

                     VALUES

                     (:id, :name, :email, :username, :city, :company)
                
                """, record)
                inserted+=1
            except sqlite3.IntegrityError as e:
                print(f"[LOAD], Skipping duplicate record {record['id']}: {e}")   
                skipped+=1

        conn.commit()
        conn.close()
        print(f"[LOAD], loaded -> {inserted}, skipped -> {skipped} ")

          
     
                          


    def run(self) -> None:
        print("=" * 50)
        print("[RUN] , Executing the ETL Pipeline ......")
        print("=" * 50)
        raw = self.extract()
        transformed = self.transform(raw)
        self.load(transformed)
        print("=" * 50)
        print("[RUN] , Pipeline Complete")
        print("=" * 50)

    
    

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=" client data ETL pipeline")
    parser.add_argument("source", help="source url for data extraction")
    parser.add_argument("--db", default="pipeline_output.db", help="path to the sqlite output database")

    return parser.parse_args()
          


if __name__ == "__main__":

    args = parse_args()

    if not args.source.startswith("http"):
         if not os.path.isdir(args.source):
              print(f" [ERROR]- link provided {args.source} is not a valid http url or directory path")
              raise SystemExit(1)
    
    pipeline = DataPipeLine(source = args.source, db_path = args.db)
    pipeline.run()
         
