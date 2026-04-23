import os
import sys
import argparse
from pathlib import Path

import requests
import sqlite3
from typing import List , Dict
from dataclasses import dataclass


@dataclass
class DataPipeLine:
    #change source url to source
    #make provision to check of directory exists with os.path.isdir
    def __init__(self, source :str, db_path :str = "pipeline_output.db"):
        self.source_url = source_url
        self.db_path = db_path
        self.raw_data = []
        self.transformed_data = []
        self.is_local = os.path.isdir(source)

    # create function to check which extract type to call
    def extract(self) -> list[Dict]:
        if self.is_local(self):
            return self._extract_from_directory_(self.source)
        return self._extract_from_url_(self.source)



   
    
    # add len of data extracted to show number of records
    # checking state of extraction is raise_for_status
    def _extract_from_url_(self, source_url : str) -> list[Dict]:
        print("[EXTRACT], Extracting data from source url ........")
        response = requests.get(source_url, timeout=10)
        response.raise_for_status()
        self.raw_data = response.json()
        print(f"[EXTRACT], Successfully extracted {len(self.raw_data)} records from source url")
    

    def _extract_from_directory_(self, directory: str) -> list[Dict]:


    

    #
    def transform(self, data :list[Dict]) -> list[Dict]:
        print("[TRANSFORM], Transforming the extracted data ........ ")
        self.transformed_data = []

        for record in data:
            transformed = {
                                 
                           "id"       :record.get("id").strip(),
                           "name"     :record.get("name", "").strip().title(),
                           "email"    :record.get("email", "").strip().lower(),
                           "username" :record.get("username", "").strip().lower(),
                           "city"     :record.get("city", "").get("city", "Unknown"),
                           "company"  :record.get("company", "").get("company", "Unknown"),

                          }
            if not transformed["id"] or not transformed["email"]:
                print(f"Skipping {record} missing id and email entries ")


        return self.transformed_data        
    


    def load(self, data :list[Dict]) -> None:
        print(f"[LOAD], Loading transformed data into {self.db_path}")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users(
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
                               """, record),
                inserted+=1
            except sqlite3.IntergrityError as e:  
                print(f"Skipping duplicate error -  : {e}")
                skipped+=1  

                conn.commit()
                conn.close()
                print(f"[LOAD], Successfully loaded {inserted} records and skipped {skipped} duplicate records")


    def run(self) -> None:
        print("=" * 50)
        print("[RUN], Executing the pipeline ..........")
        print("=" * 50)
        raw = self.extract()
        transformed = self.transform(raw) 
        self.load(transformed) 
        print("=" * 50)
        print("[RUN], Pipeline executed successfully ..........")
        print("=" * 50)
    

def parse_args() -> argparse.Namespace:
            parser = argparse.ArgumentParser(description="Client data cleaning cli script")
            parser.add_argument("source", help="path to the raw input file" )
            parser.add_argument("--db", default="pipleline_output.db", help="path to SQLite output db" )

            return parser.parse_args()


if __name__ == "__main__":

    args = parse_args()

    if not args.source.startswith("http"):
         if not os.path.isdir(args.source):
              print(f" [ERROR]- link provided {args.source} is not a valid http url or directory path")
              raise SystemExit(1)
    
    pipeline = DataPipeLine(source = args.source, db_path = args.db)
    pipeline.run()
         
