import os
import time
import json
import glob
import traceback
import datetime
import requests
import pandas as pd
import numpy as np
import multiprocessing as mp
from bs4 import BeautifulSoup as bs
from .config import (
    DATA_STORAGE,
    COMPANY_NAMES,
    HEADERS,
    DATA_SQL_NAMES,
    SCRAPED_DATES_SQL_NAME,
    DATA_PICKLES
)

def get_request(url, **kwargs):
    return requests.get(
        url,
        headers=HEADERS,
        **kwargs
    ).content

def save_xml_file(file_path, xml_data):
    """Save raw XML data to file."""
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Write the XML data
        with open(file_path, 'wb') as f:
            f.write(xml_data)
        return True
        
    except Exception as e:
        print(f"Error saving: {file_path}")
        print(e)
        return False

class CMBSScraper:
    """
    A scraper for CMBS (Commercial Mortgage-Backed Securities) ABS-EE filings from SEC EDGAR.
    
    This scraper:
    1. Searches for CMBS companies/trusts
    2. Retrieves their ABS-EE filings
    3. Downloads and stores the original XML data with metadata in filenames
    """
    
    def __init__(self, time_until=None):
        self.count = 100  # Number of results per page
        self.request_count = 0
        self.time_until = time_until
        self.accepted_filings = ["10-D", "ABS-EE", "EX-102"]
        
        # Table column definitions
        self.search_table_cols = ["CIK", "Company", "State/Country"]
        self.search_table_ids = ["Company"]
        self.company_table_cols = ["Filings", "Format", "Description", "Filing Date", "File/Film Number"]
        self.company_table_ids = ["Filings", "Filing Date"]
        self.filing_table_cols = ["Seq", "Description", "Document", "Type", "Size"]
        
        # Create data storage directory if it doesn't exist
        os.makedirs(DATA_STORAGE, exist_ok=True)

    def get_scraped_dates(self, company_name):
        """Get previously scraped dates for a company."""
        scraped_dates_file = os.path.join(DATA_STORAGE, company_name, DATA_PICKLES["scraped_dates"])
        if os.path.exists(scraped_dates_file):
            return pd.read_pickle(scraped_dates_file)
        return pd.DataFrame(columns=["Filing Date", "Trust"])

    def save_scraped_dates(self, scraped_dates):
        """Save scraped dates for a company."""
        scraped_dates_file = os.path.join(DATA_STORAGE, self.company_name, DATA_PICKLES["scraped_dates"])
        os.makedirs(os.path.dirname(scraped_dates_file), exist_ok=True)
        scraped_dates.to_pickle(scraped_dates_file)

    def update_scraped_dates(self, new_dates):
        """Update the record of scraped dates."""
        scraped_dates = self.get_scraped_dates(self.company_name)
        updated_dates = pd.concat([scraped_dates, new_dates], ignore_index=True)
        updated_dates = updated_dates.drop_duplicates(subset=["Filing Date", "Trust"])
        updated_dates = updated_dates.sort_values(["Trust", "Filing Date"])
        self.save_scraped_dates(updated_dates)
        return updated_dates

    def check_stop(self):
        """Check if we've reached the time limit."""
        if self.time_until is not None and time.time() >= self.time_until:
            raise StopIteration

    def get_request(self, url, **kwargs):
        """Make an HTTP request with rate limiting."""
        try:
            # Set a higher recursion limit
            import sys
            sys.setrecursionlimit(10000)  # Increase if needed, default is 1000
            
            r = requests.get(url, headers=HEADERS, **kwargs)
            self.request_count += 1
            if self.request_count >= 8:  # SEC rate limit
                time.sleep(1)
                self.request_count = 0
            return r.content
        except RecursionError:
            # Alternative approach using lxml directly instead of BeautifulSoup
            import lxml.html
            r = requests.get(url, headers=HEADERS, **kwargs)
            return lxml.html.fromstring(r.content)

    def get_soup(self, url):
        """Get BeautifulSoup object from URL."""
        content = self.get_request(url)
        return bs(content, 'lxml')

    def parse_table(self, soup, columns, table_class="tableFile2", page_limit=10):
        """Parse SEC EDGAR table data."""
        final_table = []
        final_table_text = pd.DataFrame(columns=columns)
        
        for _ in range(page_limit):
            table_soup = soup.find("table", {"class": table_class})
            if table_soup is None:
                break
                
            header = [th.text.strip() for th in table_soup.find_all("th")]
            if not all([col in header for col in columns]):
                break
                
            table = [tr.find_all("td") for tr in table_soup.find_all("tr")[1:]]
            table = [[row[header.index(col)] for col in columns] for row in table]
            
            table_text = pd.DataFrame(
                [
                    [
                        td.text.strip()
                        if td.find('br') is None
                        else td.find('br').previous_sibling
                        for td in tr
                    ]
                    for tr in table
                ],
                columns=header
            )[columns]

            final_table += table
            final_table_text = pd.concat([final_table_text, table_text], ignore_index=True)

            next_page = soup.find("input", {"value": f"Next {self.count}"})
            if next_page is None:
                break
                
            soup = self.get_soup(
                "https://www.sec.gov"
                +
                next_page.get("onclick").replace("parent.location=", "").strip("'")
            )

            self.check_stop()

        return final_table, final_table_text

    def filter_tables(self, table, table_text):
        """Filter and align table data with text data."""
        table = [table[i] for i in table_text.index]
        table_text = table_text.reset_index(drop=True)
        return table, table_text

    def get_search_table(self, company_name, date_from=None, trusts=None):
        """Get the initial search results table for a company."""
        self.check_stop()
        url = f"https://www.sec.gov/cgi-bin/browse-edgar?company={company_name.replace(' ', '+')}&match=starts-with&filenum=&State=&Country=&SIC=&myowner=exclude&action=getcompany&count={self.count}"
        
        self.search_table, self.search_table_text = self.parse_table(
            self.get_soup(url), 
            self.search_table_cols
        )
        
        # Filter by company name
        self.search_table_text = self.search_table_text[
            self.search_table_text["Company"]
            .str.lower()
            .str.startswith(company_name.lower())
        ].iloc[::-1]
        
        # Filter by trust names if provided
        if trusts is not None:
            self.search_table_text = self.search_table_text[
                self.search_table_text["Company"].isin(trusts)
            ]
        
        # Filter by date if provided
        if date_from is not None:
            date_from = pd.to_datetime(date_from)
            self.search_table_text = self.search_table_text[
                self.search_table_text["Company"].str.extract(r"(\d{4})")[0].astype(float) >= date_from.year
            ]

        self.search_table, self.search_table_text = self.filter_tables(
            self.search_table, 
            self.search_table_text
        )

    def get_company_table(self):
        """Get filing information for each company."""
        self.check_stop()
        self.company_table = []
        self.company_table_text = pd.DataFrame(
            columns=self.company_table_cols + self.search_table_ids
        )
        
        # Get previously scraped dates
        scraped_dates = self.get_scraped_dates(self.company_name)
        
        for i, row in enumerate(self.search_table):
            # Get company filings page
            company_url = (
                "https://www.sec.gov"
                +
                row[
                    self.search_table_cols.index("CIK")
                ].find("a")
                 .get("href")
                 .replace("count=40", f"count={self.count}")
            )
            
            company_table, company_table_text = self.parse_table(
                self.get_soup(company_url), 
                self.company_table_cols
            )
            
            # Process dates and filter filings
            company_table_text["Filing Date"] = pd.to_datetime(company_table_text["Filing Date"])
            company_table_text = company_table_text[
                company_table_text["Filings"].isin(self.accepted_filings)
            ]
            
            # Filter out already scraped dates for this trust
            trust_name = self.search_table_text.iloc[i]["Company"]
            trust_scraped = scraped_dates[scraped_dates["Trust"] == trust_name]
            if not trust_scraped.empty:
                company_table_text = company_table_text[
                    ~company_table_text["Filing Date"].isin(trust_scraped["Filing Date"])
                ]
            
            company_table_text = company_table_text.sort_values("Filing Date", ascending=False)
            company_table, company_table_text = self.filter_tables(
                company_table, 
                company_table_text
            )

            # Add company information
            for col in self.search_table_ids:
                company_table_text[col] = self.search_table_text.iloc[i][col]
            
            self.company_table += company_table
            self.company_table_text = pd.concat(
                [self.company_table_text, company_table_text], 
                ignore_index=True
            )

    def get_filing_table(self):
        """Get individual filing information."""
        self.check_stop()
        self.filing_table = []
        self.filing_table_text = pd.DataFrame(
            columns=self.filing_table_cols + self.search_table_ids + self.company_table_ids
        )
        
        for i, row in enumerate(self.company_table):
            # Get filing detail page
            filing_url = (
                "https://www.sec.gov"
                +
                row[
                    self.company_table_cols.index("Format")
                ].find("a")
                 .get("href")
            )
            
            filing_soup = self.get_soup(filing_url)
            
            # Handle rate limiting
            if filing_soup.find('div', string='Period of Report') is None:
                time.sleep(100)
                filing_soup = self.get_soup(filing_url)
            
            filing_table, filing_table_text = self.parse_table(
                filing_soup, 
                self.filing_table_cols, 
                "tableFile"
            )
            
            # Filter for accepted filing types
            filing_table_text = filing_table_text[
                filing_table_text["Type"].isin(self.accepted_filings)
            ]
            filing_table, filing_table_text = self.filter_tables(
                filing_table, 
                filing_table_text
            )

            # Add company and filing information
            for col in self.search_table_ids + self.company_table_ids:
                filing_table_text[col] = self.company_table_text.iloc[i][col]

            self.filing_table += filing_table
            self.filing_table_text = pd.concat(
                [self.filing_table_text, filing_table_text], 
                ignore_index=True
            )

    def prep_exhibit_urls(self):
        self.check_stop()
        exhibit_files = pd.DataFrame(columns=["URL", "File", "Trust", "Filing Date"])
        for i, row in enumerate(self.filing_table):
            form_url = (
                "https://www.sec.gov"
                +
                row[
                    self.filing_table_cols.index("Document")
                ].find("a")
                 .get("href")
            )
            if form_url.endswith(".xml"):
                hrefs = [form_url]
                form_soup = None
            else:
                hrefs = None
                form_soup = self.get_soup(form_url)

            data_folder = DATA_STORAGE
            company_folder = os.path.join(data_folder, self.company_name)
            search_folder = os.path.join(company_folder, self.filing_table_text.iloc[i]["Company"])
            os.makedirs(search_folder, exist_ok=True)

            exhibit = "102"
            check_a = (
                lambda a: 
                (
                    "exhibit" in a.text.lower()
                    and exhibit in a.text.lower()
                    or
                    a.get("href")
                    and
                    f"ex{exhibit}" in a.get("href").lower()
                )
                and a.get("href").endswith(".xml")
            )
            if hrefs is None:
                hrefs = [a.get("href") for a in form_soup.find_all("a") if check_a(a)]

            for href in hrefs:
                if "/" not in href:
                    href = form_url.rsplit("/", 1)[0] + "/" + href.rsplit("/", 1)[-1]
                f = search_folder+ "\\" + href.split("/")[-2] + "_" + href.split("/")[-1]
                exhibit_files.loc[len(exhibit_files)] = [
                    href, f, self.filing_table_text.iloc[i]["Company"], self.filing_table_text.iloc[i]["Filing Date"]
                ]
                
        return self.filter_exhibit_urls(exhibit_files)

    def filter_exhibit_urls(self, exhibit_files):
        """Filter out already downloaded exhibits."""
        self.check_stop()
        filter_ = exhibit_files["File"].apply(os.path.exists)
        return exhibit_files[~filter_].drop_duplicates()

    def download_exhibits(self, prep_urls=True, batch_size=8):
        """Download and store XML files."""
        self.check_stop()
        
        if prep_urls:
            print("Preparing exhibit urls...")
            self.exhibit_files = self.prep_exhibit_urls()
        else:
            self.exhibit_files = self.filter_exhibit_urls(self.exhibit_files)
            
        if len(self.exhibit_files) == 0:
            print("No new exhibits to download")
            return
            
        for i in range(0, len(self.exhibit_files), batch_size):
            time.sleep(1)
            
            # Download files in parallel
            with mp.Pool(batch_size) as pool:
                files = pool.map(
                    get_request,
                    self.exhibit_files.iloc[i:i+batch_size]["URL"]
                )
            
            # Save files in parallel
            with mp.Pool(batch_size) as pool:
                result = pool.starmap(
                    save_xml_file,
                    [
                        (f, data) 
                        for f, data in zip(
                            self.exhibit_files.iloc[i:i+batch_size]["File"],
                            files
                        )
                    ]
                )

            # Update scraped dates for successfully downloaded files
            successful_downloads = self.exhibit_files.iloc[i:i+batch_size][
                [bool(r) for r in result]
            ]
            if not successful_downloads.empty:
                self.update_scraped_dates(
                    successful_downloads[["Filing Date", "Trust"]]
                )

            for j in range(len(result)):
                if result[j]:
                    print("Saved", self.exhibit_files.iloc[i+j]["File"])

        self.check_stop()
        
        # Clean up empty folders
        if os.path.exists(os.path.join(DATA_STORAGE, self.company_name)):
            for folder in os.listdir(os.path.join(DATA_STORAGE, self.company_name)):
                try:
                    folder_path = os.path.join(DATA_STORAGE, self.company_name, folder)
                    if len(os.listdir(folder_path)) == 0:
                        os.rmdir(folder_path)
                except:
                    pass

    def scrape_company(
        self,
        company_name,
        date_from=None,
        scrape=True,
        download=True,
        trusts=None,
        download_batch_size=8,
    ):
        """Main method to scrape data for a company."""
        self.company_name = company_name

        if scrape:
            self.get_search_table(company_name, date_from, trusts)
            self.get_company_table()
            self.get_filing_table()
            if download:
                self.download_exhibits(batch_size=download_batch_size)

def main():
    """Main function to run the CMBS scraper."""
    scraper = CMBSScraper()
    
    for company_id, company_name in COMPANY_NAMES.items():
        print(f"Scraping data for {company_id}")
        try:
            scraper.scrape_company(
                company_name,
                scrape=True,
                download=True
            )
        except Exception as e:
            print(f"Error scraping {company_id}: {str(e)}")
            continue

if __name__ == "__main__":
    main() 