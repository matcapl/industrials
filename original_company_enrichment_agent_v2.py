import pandas as pd
import requests
import time
import random
import re
from typing import Dict, Optional
import logging
from urllib.parse import urlparse
import json

class CompanyEnrichmentAgent:
    def __init__(self, delay_range=(2, 4)):
        self.delay_range = delay_range
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Companies House API base URL
        self.ch_api_base = "https://api.company-information.service.gov.uk"
        
    def random_delay(self):
        time.sleep(random.uniform(*self.delay_range))
        
    def get_companies_house_data(self, company_number: str) -> Dict:
        """Get data from Companies House API"""
        info = {
            'company_url': '',
            'description': '',
            'employees': '',
            'manufacturing_location': ''
        }
        
        try:
            # Get basic company info
            url = f"https://find-and-update.company-information.service.gov.uk/company/{company_number}"
            self.random_delay()
            
            response = self.session.get(url, timeout=10)
            if response.status_code == 200:
                content = response.text
                
                # Extract registered office address
                address_match = re.search(r'Registered office address</h2>.*?<p[^>]*>(.*?)</p>', content, re.DOTALL)
                if address_match:
                    address = re.sub(r'<[^>]+>', ' ', address_match.group(1))
                    address = ' '.join(address.split())
                    info['manufacturing_location'] = address
                    
        except Exception as e:
            self.logger.error(f"Error getting Companies House data for {company_number}: {e}")
            
        return info
        
    def search_company_website(self, company_name: str, sic_codes: list) -> Dict:
        """Search for company website and information using DuckDuckGo"""
        info = {
            'company_url': '',
            'description': '',
            'employees': '',
            'manufacturing_location': ''
        }
        
        try:
            # Clean company name for search
            search_name = company_name.replace('LIMITED', '').replace('LTD', '').replace(',', '').strip()
            
            # Try DuckDuckGo instant answer API
            search_url = f"https://api.duckduckgo.com/?q={search_name}+uk+company&format=json&no_html=1&skip_disambig=1"
            
            self.random_delay()
            response = self.session.get(search_url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # Check for official website in results
                if 'Answer' in data and data['Answer']:
                    answer = data['Answer']
                    # Look for URLs in the answer
                    urls = re.findall(r'https?://[^\s<>"]+', answer)
                    for url in urls:
                        if self.is_likely_company_website(url, search_name):
                            info['company_url'] = url
                            break
                            
                # Try to get description from abstract
                if 'Abstract' in data and data['Abstract']:
                    info['description'] = data['Abstract'][:500]
                    
        except Exception as e:
            self.logger.error(f"Error searching for {company_name}: {e}")
            
        # If no website found, try alternative search
        if not info['company_url']:
            info['company_url'] = self.alternative_website_search(company_name)
            
        # Generate description from SIC codes if none found
        if not info['description'] and sic_codes:
            info['description'] = self.generate_description_from_sic(sic_codes)
            
        return info
        
    def alternative_website_search(self, company_name: str) -> str:
        """Alternative method to find company website"""
        try:
            # Try a more direct approach - construct likely domain names
            clean_name = company_name.lower()
            clean_name = re.sub(r'[^a-zA-Z0-9\s]', '', clean_name)
            clean_name = clean_name.replace('limited', '').replace('ltd', '').strip()
            
            # Try common domain patterns
            words = clean_name.split()
            if len(words) >= 1:
                potential_domains = [
                    f"{words[0]}.co.uk",
                    f"{words[0]}.com",
                    f"{''.join(words[:2])}.co.uk" if len(words) > 1 else f"{words[0]}.co.uk",
                    f"{'-'.join(words[:2])}.co.uk" if len(words) > 1 else f"{words[0]}.co.uk"
                ]
                
                for domain in potential_domains[:2]:  # Only try first 2 to be respectful
                    try:
                        test_url = f"https://www.{domain}"
                        self.random_delay()
                        response = self.session.head(test_url, timeout=5)
                        if response.status_code < 400:
                            return test_url
                    except:
                        continue
                        
        except Exception as e:
            self.logger.error(f"Error in alternative search for {company_name}: {e}")
            
        return ''
        
    def is_likely_company_website(self, url: str, company_name: str) -> bool:
        """Check if URL is likely the company's website"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Skip obvious non-company sites
            skip_domains = ['wikipedia.org', 'linkedin.com', 'facebook.com', 'twitter.com', 
                           'google.com', 'youtube.com', 'companieshouse.gov.uk']
            
            if any(skip in domain for skip in skip_domains):
                return False
                
            # Check if company name words appear in domain
            company_words = company_name.lower().replace('limited', '').replace('ltd', '').strip().split()
            company_words = [word for word in company_words if len(word) > 2]
            
            for word in company_words:
                if word in domain:
                    return True
                    
            return '.co.uk' in domain or '.com' in domain
            
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
        
    def estimate_employees_from_sic(self, sic_codes: list, company_age_years: int) -> str:
        """Estimate employee count based on SIC codes and company age"""
        if not sic_codes:
            return ''
            
        # Very rough estimates based on typical UK SME patterns
        manufacturing_sics = ['22', '23', '24', '25', '26', '27', '28', '29']
        is_manufacturing = any(sic.startswith(code) for code in manufacturing_sics for sic in sic_codes if sic)
        
        if is_manufacturing:
            if company_age_years < 2:
                return '1-10'
            elif company_age_years < 5:
                return '5-25'
            elif company_age_years < 10:
                return '10-50'
            else:
                return '20-100'
        else:
            # Service companies tend to be smaller
            if company_age_years < 2:
                return '1-5'
            elif company_age_years < 5:
                return '2-15'
            else:
                return '5-30'
                
    def calculate_company_age(self, incorporation_date: str) -> int:
        """Calculate company age in years"""
        try:
            if '/' in incorporation_date:
                parts = incorporation_date.split('/')
                year = int(parts[2]) if len(parts) == 3 else 2020
            elif '-' in incorporation_date:
                year = int(incorporation_date.split('-')[0])
            else:
                year = 2020
                
            return 2024 - year
        except:
            return 5  # Default assumption
            
    def enrich_company(self, row: pd.Series) -> Dict:
        """Enrich a single company's data"""
        company_name = row['CompanyName']
        company_number = row.get('CompanyNumber', '')
        incorporation_date = row.get('incorporation_date', '')
        
        self.logger.info(f"Processing: {company_name}")
        
        # Get SIC codes
        sic_codes = [
            row.get('SICCode.SicText_1', ''),
            row.get('SICCode.SicText_2', ''),
            row.get('SICCode.SicText_3', ''),
            row.get('SICCode.SicText_4', '')
        ]
        sic_codes = [sic for sic in sic_codes if sic]
        
        # Start with empty info
        info = {
            'company_url': '',
            'description': '',
            'employees': '',
            'manufacturing_location': ''
        }
        
        # Try Companies House first
        if company_number:
            ch_info = self.get_companies_house_data(company_number)
            info.update({k: v for k, v in ch_info.items() if v})
            
        # Search for website and additional info
        search_info = self.search_company_website(company_name, sic_codes)
        for key, value in search_info.items():
            if value and not info[key]:
                info[key] = value
                
        # Estimate employees if not found
        if not info['employees']:
            company_age = self.calculate_company_age(incorporation_date)
            info['employees'] = self.estimate_employees_from_sic(sic_codes, company_age)
            
        return info
        
    def process_csv(self, input_file: str, output_file: str = None):
        """Process the CSV file and enrich company data"""
        try:
            df = pd.read_csv(input_file)
            self.logger.info(f"Loaded {len(df)} companies from {input_file}")
            
            # Add enrichment columns
            enrichment_columns = ['company_url', 'description', 'employees', 'manufacturing_location']
            for col in enrichment_columns:
                if col not in df.columns:
                    df[col] = ''
            
            # Process each company
            for index, row in df.iterrows():
                # Skip if already processed
                if all(row.get(col, '') for col in enrichment_columns):
                    continue
                    
                try:
                    enriched_info = self.enrich_company(row)
                    
                    # Update dataframe
                    for key, value in enriched_info.items():
                        if value:
                            df.at[index, key] = value
                            
                    # Save progress every 5 companies
                    if (index + 1) % 5 == 0:
                        if output_file:
                            df.to_csv(output_file, index=False)
                        self.logger.info(f"Processed {index + 1}/{len(df)} companies")
                        
                except Exception as e:
                    self.logger.error(f"Error processing {row['CompanyName']}: {e}")
                    continue
                    
            # Final save
            if not output_file:
                output_file = input_file.replace('.csv', '_enriched.csv')
                
            df.to_csv(output_file, index=False)
            self.logger.info(f"Processing complete! Results saved to {output_file}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing file: {e}")
            return None

# Usage example
def main():
    agent = CompanyEnrichmentAgent()
    
    # Process your file
    input_file = "industrials_enriched.csv"  # Your uploaded file
    output_file = "industrials_fully_enriched.csv"
    
    enriched_df = agent.process_csv(input_file, output_file)
    
    if enriched_df is not None:
        print("Sample of enriched data:")
        sample_cols = ['CompanyName', 'company_url', 'description', 'employees', 'manufacturing_location']
        print(enriched_df[sample_cols].head(10).to_string())

if __name__ == "__main__":
    main()