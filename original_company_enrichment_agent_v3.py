import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random
import re
from typing import Dict, Optional
import logging
from urllib.parse import urlparse, urljoin
import json
from datetime import datetime

class CompanyEnrichmentAgent:
    def __init__(self, delay_range=(2, 4)):
        self.delay_range = delay_range
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
    def random_delay(self):
        time.sleep(random.uniform(*self.delay_range))
        
    def get_companies_house_data(self, company_number: str, company_name: str) -> Dict:
        """Get comprehensive data from Companies House including filing history"""
        info = {
            'company_url': '',
            'description': '',
            'employees_2024': '',
            'employees_2023': '',
            'employees_2022': '',
            'manufacturing_location': ''
        }
        
        try:
            # Get main company page
            main_url = f"https://find-and-update.company-information.service.gov.uk/company/{company_number}"
            self.logger.info(f"Fetching Companies House data for {company_name} ({company_number})")
            self.random_delay()
            
            response = self.session.get(main_url, timeout=15)
            if response.status_code != 200:
                self.logger.warning(f"Failed to get main page for {company_number}")
                return info
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract registered office address
            address_section = soup.find('div', {'id': 'company-addresses'})
            if address_section:
                address_text = address_section.get_text(strip=True)
                # Clean up the address
                address_text = re.sub(r'Registered office address', '', address_text)
                address_text = re.sub(r'\s+', ' ', address_text).strip()
                if address_text:
                    info['manufacturing_location'] = address_text
                    
            # Get filing history for accounts
            filing_url = f"https://find-and-update.company-information.service.gov.uk/company/{company_number}/filing-history"
            self.random_delay()
            
            filing_response = self.session.get(filing_url, timeout=15)
            if filing_response.status_code == 200:
                employee_data = self.extract_employee_data_from_filings(filing_response.content, company_number)
                info.update(employee_data)
                
        except Exception as e:
            self.logger.error(f"Error getting Companies House data for {company_number}: {e}")
            
        return info
        
    def extract_employee_data_from_filings(self, filing_page_content: bytes, company_number: str) -> Dict:
        """Extract employee data from filing history page"""
        employee_info = {
            'employees_2024': '',
            'employees_2023': '',
            'employees_2022': ''
        }
        
        try:
            soup = BeautifulSoup(filing_page_content, 'html.parser')
            
            # Look for annual accounts links
            filing_rows = soup.find_all('tr')
            account_links = []
            
            for row in filing_rows:
                description_cell = row.find('td')
                if description_cell and 'annual accounts' in description_cell.get_text().lower():
                    date_cell = row.find_all('td')[1] if len(row.find_all('td')) > 1 else None
                    link_cell = row.find('a', href=True)
                    
                    if date_cell and link_cell:
                        date_text = date_cell.get_text().strip()
                        link_href = link_cell['href']
                        
                        # Extract year from date or description
                        year_match = re.search(r'20(22|23|24)', date_text + ' ' + description_cell.get_text())
                        if year_match:
                            year = '20' + year_match.group(1)
                            account_links.append((year, link_href, date_text))
                            
            # Process the most recent accounts for each year
            for year, link_href, date_text in account_links[:6]:  # Limit to 6 most recent
                if year in ['2024', '2023', '2022'] and not employee_info[f'employees_{year}']:
                    self.logger.info(f"Checking {year} accounts for {company_number}")
                    employee_count = self.get_employee_count_from_accounts(link_href)
                    if employee_count:
                        employee_info[f'employees_{year}'] = employee_count
                        
        except Exception as e:
            self.logger.error(f"Error extracting employee data from filings: {e}")
            
        return employee_info
        
    def get_employee_count_from_accounts(self, accounts_link: str) -> str:
        """Extract employee count from accounts document"""
        try:
            if not accounts_link.startswith('http'):
                accounts_url = f"https://find-and-update.company-information.service.gov.uk{accounts_link}"
            else:
                accounts_url = accounts_link
                
            self.random_delay()
            response = self.session.get(accounts_url, timeout=15)
            
            if response.status_code != 200:
                return ''
                
            # If it's a PDF, we need to handle it differently
            if 'pdf' in response.headers.get('content-type', '').lower():
                # For PDFs, we'd need a PDF parser, but let's try the HTML version first
                html_url = accounts_url.replace('.pdf', '').replace('/document/', '/document/').split('?')[0]
                self.random_delay()
                html_response = self.session.get(html_url, timeout=15)
                if html_response.status_code == 200:
                    content = html_response.text
                else:
                    content = response.text
            else:
                content = response.text
                
            # Look for employee-related patterns in the content
            employee_patterns = [
                r'average\s+number\s+of\s+employees[:\s]+(\d+)',
                r'number\s+of\s+employees[:\s]+(\d+)',
                r'employees?\s*[:\-]\s*(\d+)',
                r'staff\s+numbers?[:\s]+(\d+)',
                r'total\s+employees[:\s]+(\d+)',
                r'workforce[:\s]+(\d+)',
                r'(\d+)\s+employees?',
                r'employ\s+(\d+)\s+people',
            ]
            
            for pattern in employee_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                if matches:
                    # Take the first reasonable number (between 1 and 10000 for most SMEs)
                    for match in matches:
                        num = int(match)
                        if 1 <= num <= 10000:
                            return str(num)
                            
        except Exception as e:
            self.logger.error(f"Error getting employee count from accounts: {e}")
            
        return ''
        
    def search_company_website(self, company_name: str, sic_codes: list) -> Dict:
        """Search for company website using multiple methods"""
        info = {
            'company_url': '',
            'description': ''
        }
        
        try:
            # Clean company name for search
            search_name = re.sub(r'\b(LIMITED|LTD|CO\.?,?\s*LTD\.?)\b', '', company_name, flags=re.IGNORECASE).strip()
            search_name = re.sub(r'[^\w\s]', ' ', search_name).strip()
            
            # Try direct domain guessing first (faster)
            website = self.guess_company_domain(search_name)
            if website:
                info['company_url'] = website
                
            # If no website found and it's a UK company, try search
            if not info['company_url'] and not search_name.upper().startswith(('ZHEJIANG', 'ZHENGZHOU', 'ZHENPING')):
                website = self.search_for_website(search_name)
                if website:
                    info['company_url'] = website
                    
            # Generate description from SIC codes
            if sic_codes:
                info['description'] = self.generate_description_from_sic(sic_codes)
                
        except Exception as e:
            self.logger.error(f"Error searching for {company_name}: {e}")
            
        return info
        
    def guess_company_domain(self, company_name: str) -> str:
        """Guess company domain from name"""
        try:
            # Clean and create potential domain names
            clean_name = re.sub(r'[^\w\s]', '', company_name.lower())
            words = clean_name.split()
            
            if not words:
                return ''
                
            # Try various domain combinations
            potential_domains = []
            
            if len(words) == 1:
                potential_domains = [
                    f"{words[0]}.co.uk",
                    f"{words[0]}.com"
                ]
            else:
                # Multi-word company names
                potential_domains = [
                    f"{words[0]}{words[1] if len(words) > 1 else ''}.co.uk",
                    f"{words[0]}-{words[1] if len(words) > 1 else words[0]}.co.uk",
                    f"{''.join(words[:2])}.co.uk",
                    f"{words[0]}.co.uk"
                ]
                
            # Test domains (limit to 3 to be respectful)
            for domain in potential_domains[:3]:
                try:
                    test_url = f"https://www.{domain}"
                    self.random_delay()
                    response = self.session.head(test_url, timeout=8, allow_redirects=True)
                    if response.status_code < 400:
                        # Verify it's actually a company website
                        if self.verify_company_website(test_url, company_name):
                            return test_url
                except:
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error guessing domain for {company_name}: {e}")
            
        return ''
        
    def verify_company_website(self, url: str, company_name: str) -> bool:
        """Verify if URL is actually the company's website"""
        try:
            response = self.session.get(url, timeout=8)
            if response.status_code >= 400:
                return False
                
            content = response.text.lower()
            company_words = company_name.lower().replace('limited', '').replace('ltd', '').split()
            company_words = [word for word in company_words if len(word) > 3]
            
            # Check if company name appears on the page
            word_matches = sum(1 for word in company_words if word in content)
            return word_matches >= min(2, len(company_words))
            
        except:
            return False
            
    def search_for_website(self, company_name: str) -> str:
        """Search for company website using Bing search"""
        try:
            query = f"{company_name} UK company official website"
            search_url = f"https://www.bing.com/search?q={query.replace(' ', '+')}"
            
            self.random_delay()
            response = self.session.get(search_url, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Look for search result links
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if href.startswith('http') and self.is_likely_company_website(href, company_name):
                        return href
                        
        except Exception as e:
            self.logger.error(f"Error searching for website: {e}")
            
        return ''
        
    def is_likely_company_website(self, url: str, company_name: str) -> bool:
        """Check if URL is likely the company's website"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Skip obvious non-company sites
            skip_domains = ['wikipedia.org', 'linkedin.com', 'facebook.com', 'twitter.com', 
                           'google.com', 'youtube.com', 'companieshouse.gov.uk', 'bing.com',
                           'gov.uk', 'ac.uk']
            
            if any(skip in domain for skip in skip_domains):
                return False
                
            # Must be UK domain or common business domain
            if not (domain.endswith('.co.uk') or domain.endswith('.com') or domain.endswith('.org')):
                return False
                
            return True
            
        except:
            return False
            
    def generate_description_from_sic(self, sic_codes: list) -> str:
        """Generate a description based on SIC codes"""
        if not sic_codes:
            return ''
            
        # Take the first meaningful SIC code description
        for sic in sic_codes:
            if sic and ' - ' in sic:
                description = sic.split(' - ', 1)[1]
                return f"Company engaged in {description.lower()}"
                
        return ''
        
    def enrich_company(self, row: pd.Series) -> Dict:
        """Enrich a single company's data"""
        company_name = str(row['CompanyName']).strip()
        company_number = str(row.get('CompanyNumber', '')).strip()
        
        self.logger.info(f"Processing: {company_name}")
        
        # Get SIC codes
        sic_codes = [
            str(row.get('SICCode.SicText_1', '')),
            str(row.get('SICCode.SicText_2', '')),
            str(row.get('SICCode.SicText_3', '')),
            str(row.get('SICCode.SicText_4', ''))
        ]
        sic_codes = [sic for sic in sic_codes if sic and sic != 'nan']
        
        # Initialize with empty values
        info = {
            'company_url': '',
            'description': '',
            'employees_2024': '',
            'employees_2023': '',
            'employees_2022': '',
            'manufacturing_location': ''
        }
        
        # Get Companies House data first (most reliable)
        if company_number and company_number != 'nan':
            ch_info = self.get_companies_house_data(company_number, company_name)
            for key, value in ch_info.items():
                if value:
                    info[key] = value
                    
        # Search for website and description
        search_info = self.search_company_website(company_name, sic_codes)
        for key, value in search_info.items():
            if value and not info[key]:
                info[key] = value
                
        return info
        
    def process_csv(self, input_file: str, output_file: str = None):
        """Process the CSV file and enrich company data"""
        try:
            df = pd.read_csv(input_file)
            self.logger.info(f"Loaded {len(df)} companies from {input_file}")
            
            # Add new enrichment columns
            new_columns = ['company_url', 'description', 'employees_2024', 'employees_2023', 'employees_2022', 'manufacturing_location']
            
            for col in new_columns:
                if col not in df.columns:
                    df[col] = ''
            
            # Process each company
            processed_count = 0
            for index, row in df.iterrows():
                try:
                    # Always process (don't skip based on existing data)
                    enriched_info = self.enrich_company(row)
                    
                    # Update dataframe with non-empty values
                    for key, value in enriched_info.items():
                        if value and str(value).strip():
                            df.at[index, key] = str(value).strip()
                            
                    processed_count += 1
                    
                    # Save progress every 5 companies
                    if processed_count % 5 == 0:
                        if output_file:
                            df.to_csv(output_file, index=False)
                        self.logger.info(f"Processed {processed_count}/{len(df)} companies")
                        
                except Exception as e:
                    self.logger.error(f"Error processing {row.get('CompanyName', 'Unknown')}: {e}")
                    continue
                    
            # Final save
            if not output_file:
                output_file = input_file.replace('.csv', '_fully_enriched.csv')
                
            df.to_csv(output_file, index=False)
            self.logger.info(f"Processing complete! Results saved to {output_file}")
            
            # Show summary
            summary = {}
            for col in new_columns:
                filled = (df[col] != '').sum()
                summary[col] = f"{filled}/{len(df)}"
                
            self.logger.info(f"Enrichment summary: {summary}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing file: {e}")
            return None

# Usage
def main():
    agent = CompanyEnrichmentAgent(delay_range=(3, 5))  # Be more respectful with delays
    
    input_file = "industrials_enriched.csv"
    output_file = "industrials_fully_enriched.csv"
    
    enriched_df = agent.process_csv(input_file, output_file)
    
    if enriched_df is not None:
        print("\nSample of enriched data:")
        sample_cols = ['CompanyName', 'company_url', 'description', 'employees_2024', 'employees_2023', 'manufacturing_location']
        available_cols = [col for col in sample_cols if col in enriched_df.columns]
        
        sample_data = enriched_df[available_cols].head(5)
        for col in available_cols:
            if col != 'CompanyName':
                filled_count = (sample_data[col] != '').sum()
                print(f"{col}: {filled_count}/5 filled")
                
        print(sample_data.to_string(max_colwidth=50))

if __name__ == "__main__":
    main()