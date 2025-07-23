import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random
import re
from urllib.parse import urljoin, urlparse
import json
import logging
from typing import Dict, Optional, List
import os

class CompanyEnrichmentAgent:
    def __init__(self, delay_range=(1, 3)):
        """
        Initialize the enrichment agent
        
        Args:
            delay_range: Tuple of (min, max) seconds to wait between requests
        """
        self.delay_range = delay_range
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Cache for results to avoid re-processing
        self.cache = {}
        
    def random_delay(self):
        """Add random delay between requests to be respectful"""
        time.sleep(random.uniform(*self.delay_range))
        
    def search_company_info(self, company_name: str, company_number: str = None) -> Dict:
        """
        Search for company information using multiple strategies
        
        Args:
            company_name: Name of the company
            company_number: Companies House number (optional)
            
        Returns:
            Dictionary with found information
        """
        cache_key = f"{company_name}_{company_number}"
        if cache_key in self.cache:
            return self.cache[cache_key]
            
        info = {
            'company_url': '',
            'description': '',
            'employees': '',
            'manufacturing_location': ''
        }
        
        try:
            # Strategy 1: Search for official website
            website = self.find_company_website(company_name)
            if website:
                info['company_url'] = website
                # Extract info from company website
                website_info = self.extract_website_info(website)
                info.update(website_info)
            
            # Strategy 2: Search Companies House if we have company number
            if company_number:
                ch_info = self.search_companies_house(company_name, company_number)
                # Merge non-empty values
                for key, value in ch_info.items():
                    if value and not info[key]:
                        info[key] = value
            
            # Strategy 3: General web search for missing info
            if not info['description'] or not info['employees']:
                search_info = self.general_web_search(company_name)
                for key, value in search_info.items():
                    if value and not info[key]:
                        info[key] = value
                        
        except Exception as e:
            self.logger.error(f"Error processing {company_name}: {e}")
            
        # Cache the result
        self.cache[cache_key] = info
        return info
        
    def find_company_website(self, company_name: str) -> Optional[str]:
        """Find the official company website"""
        try:
            # Search query for company website
            search_query = f"{company_name} official website"
            search_url = f"https://www.google.com/search?q={search_query.replace(' ', '+')}"
            
            self.random_delay()
            response = self.session.get(search_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for search results
            links = soup.find_all('a', href=True)
            for link in links:
                href = link['href']
                if '/url?q=' in href:
                    url = href.split('/url?q=')[1].split('&')[0]
                    if self.is_valid_company_url(url, company_name):
                        return url
                        
        except Exception as e:
            self.logger.error(f"Error finding website for {company_name}: {e}")
            
        return None
        
    def is_valid_company_url(self, url: str, company_name: str) -> bool:
        """Check if URL is likely the company's official website"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Skip common non-company domains
            skip_domains = ['google.com', 'facebook.com', 'linkedin.com', 'twitter.com', 
                          'youtube.com', 'wikipedia.org', 'companieshouse.gov.uk']
            
            if any(skip in domain for skip in skip_domains):
                return False
                
            # Check if company name appears in domain
            company_words = company_name.lower().replace('limited', '').replace('ltd', '').strip().split()
            for word in company_words:
                if len(word) > 3 and word in domain:
                    return True
                    
            return False
            
        except:
            return False
            
    def extract_website_info(self, url: str) -> Dict:
        """Extract information from company website"""
        info = {
            'description': '',
            'employees': '',
            'manufacturing_location': ''
        }
        
        try:
            self.random_delay()
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract description from meta tags or about sections
            description = self.extract_description(soup)
            if description:
                info['description'] = description
                
            # Look for employee information
            employees = self.extract_employee_count(soup)
            if employees:
                info['employees'] = employees
                
            # Look for location information
            location = self.extract_location(soup)
            if location:
                info['manufacturing_location'] = location
                
        except Exception as e:
            self.logger.error(f"Error extracting info from {url}: {e}")
            
        return info
        
    def extract_description(self, soup: BeautifulSoup) -> str:
        """Extract company description from webpage"""
        # Try meta description first
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            return meta_desc['content'].strip()
            
        # Look for about sections
        about_selectors = [
            'section[class*="about"]',
            'div[class*="about"]',
            '.company-description',
            '.business-description'
        ]
        
        for selector in about_selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text().strip()
                if len(text) > 50:
                    return text[:500] + '...' if len(text) > 500 else text
                    
        return ''
        
    def extract_employee_count(self, soup: BeautifulSoup) -> str:
        """Extract employee count from webpage"""
        text = soup.get_text().lower()
        
        # Look for patterns like "50 employees", "team of 100", etc.
        patterns = [
            r'(\d+)\s*employees?',
            r'team\s*of\s*(\d+)',
            r'(\d+)\s*staff',
            r'employs?\s*(\d+)',
            r'workforce\s*of\s*(\d+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
                
        return ''
        
    def extract_location(self, soup: BeautifulSoup) -> str:
        """Extract location information from webpage"""
        # Look for address information
        address_selectors = [
            '[class*="address"]',
            '[class*="location"]',
            '[class*="contact"]'
        ]
        
        for selector in address_selectors:
            elements = soup.select(selector)
            for element in elements:
                text = element.get_text().strip()
                if self.is_uk_address(text):
                    return text
                    
        # Look for postcode patterns in general text
        text = soup.get_text()
        postcode_match = re.search(r'[A-Z]{1,2}[0-9][A-Z0-9]?\s*[0-9][ABD-HJLNP-UW-Z]{2}', text)
        if postcode_match:
            return postcode_match.group().strip()
            
        return ''
        
    def is_uk_address(self, text: str) -> bool:
        """Check if text contains a UK address"""
        uk_indicators = ['uk', 'united kingdom', 'england', 'scotland', 'wales', 'northern ireland']
        postcode_pattern = r'[A-Z]{1,2}[0-9][A-Z0-9]?\s*[0-9][ABD-HJLNP-UW-Z]{2}'
        
        text_lower = text.lower()
        return any(indicator in text_lower for indicator in uk_indicators) or bool(re.search(postcode_pattern, text))
        
    def search_companies_house(self, company_name: str, company_number: str) -> Dict:
        """Search Companies House for additional information"""
        info = {
            'description': '',
            'employees': '',
            'manufacturing_location': ''
        }
        
        # This would typically use the Companies House API
        # For now, we'll use web scraping as a fallback
        try:
            url = f"https://find-and-update.company-information.service.gov.uk/company/{company_number}"
            self.random_delay()
            response = self.session.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract registered address
            address_elem = soup.find('div', {'id': 'company-addresses'})
            if address_elem:
                address_text = address_elem.get_text().strip()
                info['manufacturing_location'] = address_text
                
        except Exception as e:
            self.logger.error(f"Error searching Companies House for {company_name}: {e}")
            
        return info
        
    def general_web_search(self, company_name: str) -> Dict:
        """Perform general web search for missing information"""
        info = {
            'description': '',
            'employees': '',
            'manufacturing_location': ''
        }
        
        # This would typically use a proper search API
        # Implementation would depend on available search services
        
        return info
        
    def process_dataframe(self, df: pd.DataFrame, output_file: str = None) -> pd.DataFrame:
        """
        Process the entire dataframe and enrich with additional data
        
        Args:
            df: Input dataframe with company data
            output_file: Optional output CSV file path
            
        Returns:
            Enriched dataframe
        """
        # Add new columns if they don't exist
        new_columns = ['company_url', 'description', 'employees', 'manufacturing_location']
        for col in new_columns:
            if col not in df.columns:
                df[col] = ''
                
        total_companies = len(df)
        
        for index, row in df.iterrows():
            self.logger.info(f"Processing company {index + 1}/{total_companies}: {row['CompanyName']}")
            
            # Skip if already processed (all new fields have values)
            if all(row.get(col, '') for col in new_columns):
                self.logger.info(f"Skipping {row['CompanyName']} - already processed")
                continue
                
            # Get company information
            company_info = self.search_company_info(
                row['CompanyName'], 
                row.get('CompanyNumber', '')
            )
            
            # Update the dataframe
            for col, value in company_info.items():
                if value and not row.get(col, ''):
                    df.at[index, col] = value
                    
            # Save progress periodically
            if (index + 1) % 10 == 0 and output_file:
                df.to_csv(output_file, index=False)
                self.logger.info(f"Progress saved to {output_file}")
                
        # Final save
        if output_file:
            df.to_csv(output_file, index=False)
            self.logger.info(f"Final results saved to {output_file}")
            
        return df

def main():
    """Main function to run the enrichment agent"""
    # Initialize the agent
    agent = CompanyEnrichmentAgent(delay_range=(2, 4))  # Be respectful with delays
    
    # Load the data
    input_file = "./industrials.xlsx"  # Change this to your file path
    output_file = "./industrials_enriched.csv"
    
    try:
        # Try to load Excel file
        if input_file.endswith('.xlsx'):
            df = pd.read_excel(input_file)
        else:
            df = pd.read_csv(input_file)
            
        print(f"Loaded {len(df)} companies from {input_file}")
        
        # Process the dataframe
        enriched_df = agent.process_dataframe(df, output_file)
        
        print(f"Processing complete! Results saved to {output_file}")
        print(f"Enriched {len(enriched_df)} companies")
        
        # Display sample results
        print("\nSample of enriched data:")
        print(enriched_df[['CompanyName', 'company_url', 'description', 'employees', 'manufacturing_location']].head())
        
    except FileNotFoundError:
        print(f"Error: Could not find input file {input_file}")
        print("Please ensure the file exists and update the input_file variable")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()