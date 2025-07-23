import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random
import re
from typing import Dict, Optional, List
import logging
from urllib.parse import urlparse, urljoin
import json
from datetime import datetime
import PyPDF2
import io

class QualityValidator:
    """AI-powered quality validation for enrichment results"""
    
    @staticmethod
    def validate_company_website(url: str, company_name: str, sic_codes: List[str]) -> Dict:
        """Validate if a URL is actually the company's official website"""
        if not url or not url.startswith('http'):
            return {"valid": False, "reason": "Invalid URL format"}
            
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Immediate disqualification criteria
            blacklisted_domains = [
                'microsoft.com', 'google.com', 'facebook.com', 'linkedin.com', 'twitter.com',
                'youtube.com', 'wikipedia.org', 'gov.uk', 'bbc.com', 'booking.com',
                'cnbc.com', 'scribd.com', 'forums.', 'forum.', 'support.', 'answers.',
                'fandom.com', 'npmjs.com', 'kortoverkobenhavn.com', 'svenskafans.com',
                'writedu.com', 'bmj.com', 'epictravelplans.com', 'fuji-x-forum.com',
                'leagueoflegends.', 'wordreference.com', 'x-plane.org'
            ]
            
            if any(blocked in domain for blocked in blacklisted_domains):
                return {"valid": False, "reason": f"Blacklisted domain: {domain}"}
                
            # Check domain relevance to company name
            company_words = re.sub(r'\b(LIMITED|LTD|CO\.?,?\s*LTD\.?)\b', '', company_name, flags=re.IGNORECASE)
            company_words = re.sub(r'[^\w\s]', ' ', company_words).strip().lower().split()
            company_words = [word for word in company_words if len(word) > 2]
            
            domain_matches = sum(1 for word in company_words if word in domain)
            
            # For UK companies, expect .co.uk or .com domains
            if not (domain.endswith('.co.uk') or domain.endswith('.com') or domain.endswith('.org')):
                return {"valid": False, "reason": "Non-business domain extension"}
                
            # Must have some company name match for validation
            if domain_matches == 0 and len(company_words) > 0:
                return {"valid": False, "reason": "No company name match in domain"}
                
            return {"valid": True, "confidence": min(100, domain_matches * 30 + 40)}
            
        except Exception as e:
            return {"valid": False, "reason": f"Validation error: {e}"}
    
    @staticmethod
    def validate_company_description(description: str, company_name: str, sic_codes: List[str]) -> Dict:
        """Validate if description makes sense and isn't redundant"""
        if not description:
            return {"valid": False, "reason": "Empty description"}
            
        # Check if it's just a rehash of SIC codes
        if description.startswith("Company engaged in"):
            sic_text = " ".join(sic_codes).lower()
            desc_core = description.replace("Company engaged in", "").strip().lower()
            
            # If description is too similar to SIC codes, it's redundant
            similarity_words = len(set(desc_core.split()) & set(sic_text.split()))
            if similarity_words > 3:
                return {"valid": False, "reason": "Too similar to SIC codes"}
                
        return {"valid": True, "confidence": 70}

class CompanyEnrichmentAgent:
    def __init__(self, delay_range=(3, 6)):
        self.delay_range = delay_range
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        self.validator = QualityValidator()
        
    def random_delay(self):
        time.sleep(random.uniform(*self.delay_range))
        
    def find_official_website(self, company_name: str, company_number: str) -> str:
        """Find the official company website using multiple high-quality methods"""
        
        # Method 1: Direct domain construction and testing
        website = self._construct_and_test_domains(company_name)
        if website:
            validation = self.validator.validate_company_website(website, company_name, [])
            if validation["valid"]:
                self.logger.info(f"Found website via domain construction: {website}")
                return website
                
        # Method 2: Companies House filing search for website mentions
        if company_number:
            website = self._extract_website_from_companies_house(company_number)
            if website:
                validation = self.validator.validate_company_website(website, company_name, [])
                if validation["valid"]:
                    self.logger.info(f"Found website via Companies House: {website}")
                    return website
                    
        # Method 3: Targeted web search with quality filtering
        website = self._search_with_quality_filter(company_name)
        if website:
            self.logger.info(f"Found website via filtered search: {website}")
            return website
            
        return ""
        
    def _construct_and_test_domains(self, company_name: str) -> str:
        """Construct and test likely domain names"""
        # Clean company name
        clean_name = re.sub(r'\b(LIMITED|LTD|CO\.?,?\s*LTD\.?)\b', '', company_name, flags=re.IGNORECASE)
        clean_name = re.sub(r'[^\w\s]', '', clean_name).strip().lower()
        words = [w for w in clean_name.split() if len(w) > 2]
        
        if not words:
            return ""
            
        # Generate domain candidates
        candidates = []
        if len(words) >= 2:
            candidates.extend([
                f"{words[0]}{words[1]}.co.uk",
                f"{words[0]}-{words[1]}.co.uk",
                f"{words[0]}{words[1]}.com",
                f"{''.join(words[:3])}.co.uk" if len(words) > 2 else f"{words[0]}{words[1]}.co.uk"
            ])
        candidates.extend([
            f"{words[0]}.co.uk",
            f"{words[0]}.com"
        ])
        
        # Test each candidate
        for domain in candidates[:4]:  # Limit to prevent abuse
            try:
                test_url = f"https://www.{domain}"
                self.random_delay()
                
                response = self.session.head(test_url, timeout=8, allow_redirects=True)
                if 200 <= response.status_code < 400:
                    # Verify it's actually a business website
                    if self._verify_business_website(test_url, company_name):
                        return test_url
                        
            except Exception:
                continue
                
        return ""
        
    def _verify_business_website(self, url: str, company_name: str) -> bool:
        """Verify URL is actually a business website"""
        try:
            self.random_delay()
            response = self.session.get(url, timeout=10)
            if response.status_code >= 400:
                return False
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Check for business indicators
            text_content = soup.get_text().lower()
            
            # Look for business-related keywords
            business_keywords = ['company', 'business', 'services', 'products', 'about us', 'contact', 'manufacturing']
            business_score = sum(1 for keyword in business_keywords if keyword in text_content)
            
            # Check for company name presence
            company_words = company_name.lower().replace('limited', '').replace('ltd', '').split()
            name_score = sum(1 for word in company_words if len(word) > 3 and word in text_content)
            
            # Must have both business indicators and company name
            return business_score >= 2 and name_score >= 1
            
        except Exception:
            return False
            
    def _extract_website_from_companies_house(self, company_number: str) -> str:
        """Extract website from Companies House filings"""
        try:
            # Check recent filings for website mentions
            filing_url = f"https://find-and-update.company-information.service.gov.uk/company/{company_number}/filing-history"
            self.random_delay()
            
            response = self.session.get(filing_url, timeout=15)
            if response.status_code != 200:
                return ""
                
            # Look for website mentions in filing descriptions or documents
            content = response.text
            
            # Extract potential website URLs
            url_pattern = r'https?://(?:www\.)?([a-zA-Z0-9-]+\.(?:co\.uk|com|org))'
            matches = re.findall(url_pattern, content)
            
            for match in matches:
                potential_url = f"https://www.{match}"
                if self._verify_business_website(potential_url, company_number):
                    return potential_url
                    
        except Exception as e:
            self.logger.error(f"Error extracting website from Companies House: {e}")
            
        return ""
        
    def _search_with_quality_filter(self, company_name: str) -> str:
        """Search for company website with strict quality filtering"""
        try:
            # Use DuckDuckGo for search (more reliable than scraping Google)
            query = f"{company_name} UK company official website -linkedin -facebook -wikipedia"
            search_url = f"https://html.duckduckgo.com/html/?q={query.replace(' ', '+')}"
            
            self.random_delay()
            response = self.session.get(search_url, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract search result URLs
                for link in soup.find_all('a', href=True):
                    href = link.get('href', '')
                    if href.startswith('/l/?uddg=') and 'http' in href:
                        # Extract actual URL from DuckDuckGo redirect
                        url_match = re.search(r'uddg=([^&]+)', href)
                        if url_match:
                            import urllib.parse
                            actual_url = urllib.parse.unquote(url_match.group(1))
                            
                            validation = self.validator.validate_company_website(actual_url, company_name, [])
                            if validation["valid"] and validation.get("confidence", 0) > 50:
                                if self._verify_business_website(actual_url, company_name):
                                    return actual_url
                                    
        except Exception as e:
            self.logger.error(f"Error in quality filtered search: {e}")
            
        return ""
        
    def extract_company_description(self, url: str, company_name: str) -> str:
        """Extract what the company says they do from their website"""
        if not url:
            return ""
            
        try:
            self.random_delay()
            response = self.session.get(url, timeout=15)
            if response.status_code >= 400:
                return ""
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Strategy 1: Meta description
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                desc = meta_desc['content'].strip()
                if len(desc) > 20 and not desc.lower().startswith('welcome to'):
                    return desc[:500]
                    
            # Strategy 2: About sections
            about_selectors = [
                'section[class*="about"]',
                'div[class*="about"]',
                '.company-description',
                '.business-description',
                'div[id*="about"]',
                'section[id*="about"]'
            ]
            
            for selector in about_selectors:
                elements = soup.select(selector)
                for element in elements:
                    text = element.get_text(strip=True)
                    if 50 < len(text) < 1000:
                        # Clean up the text
                        text = re.sub(r'\s+', ' ', text)
                        return text[:500]
                        
            # Strategy 3: First substantial paragraph
            paragraphs = soup.find_all('p')
            for p in paragraphs:
                text = p.get_text(strip=True)
                if 50 < len(text) < 500:
                    return text
                    
        except Exception as e:
            self.logger.error(f"Error extracting description from {url}: {e}")
            
        return ""
        
    def get_employee_data_from_accounts(self, company_number: str) -> Dict:
        """Extract employee data from Companies House accounts PDFs"""
        employee_data = {
            'employees_2024': '',
            'employees_2023': '',
            'employees_2022': ''
        }
        
        try:
            # Get filing history
            filing_url = f"https://find-and-update.company-information.service.gov.uk/company/{company_number}/filing-history"
            self.random_delay()
            
            response = self.session.get(filing_url, timeout=15)
            if response.status_code != 200:
                return employee_data
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find annual accounts filings
            account_links = []
            filing_items = soup.find_all('div', class_='filing-history-item')
            
            for item in filing_items:
                description = item.find('h3')
                if not description:
                    continue
                    
                desc_text = description.get_text().lower()
                if 'annual accounts' in desc_text or 'accounts' in desc_text:
                    date_elem = item.find('time')
                    link_elem = item.find('a', string=re.compile(r'View PDF|Download'))
                    
                    if date_elem and link_elem:
                        date_str = date_elem.get('datetime', '')
                        pdf_link = link_elem.get('href', '')
                        
                        # Extract year
                        year_match = re.search(r'20(22|23|24)', date_str + ' ' + desc_text)
                        if year_match:
                            year = '20' + year_match.group(1)
                            account_links.append((year, pdf_link, desc_text))
                            
            # Process accounts for each year
            for year, pdf_link, description in account_links[:6]:  # Limit to recent filings
                if year in ['2024', '2023', '2022'] and not employee_data[f'employees_{year}']:
                    self.logger.info(f"Processing {year} accounts for {company_number}")
                    employee_count = self._extract_employees_from_pdf(pdf_link)
                    if employee_count:
                        employee_data[f'employees_{year}'] = employee_count
                        
        except Exception as e:
            self.logger.error(f"Error getting employee data for {company_number}: {e}")
            
        return employee_data
        
    def _extract_employees_from_pdf(self, pdf_link: str) -> str:
        """Extract employee count from PDF document"""
        try:
            if not pdf_link.startswith('http'):
                pdf_url = f"https://find-and-update.company-information.service.gov.uk{pdf_link}"
            else:
                pdf_url = pdf_link
                
            self.random_delay()
            response = self.session.get(pdf_url, timeout=20)
            
            if response.status_code != 200:
                return ""
                
            # Try to extract text from PDF
            try:
                pdf_file = io.BytesIO(response.content)
                pdf_reader = PyPDF2.PdfReader(pdf_file)
                
                text_content = ""
                for page in pdf_reader.pages[:10]:  # Check first 10 pages
                    text_content += page.extract_text()
                    
            except Exception:
                # If PDF parsing fails, try as text
                text_content = response.text
                
            # Search for employee patterns
            employee_patterns = [
                r'average\s+number\s+of\s+employees[:\s]+(\d+)',
                r'number\s+of\s+employees[:\s]+(\d+)',
                r'employees?\s*[:\-]\s*(\d+)',
                r'staff\s+numbers?[:\s]+(\d+)',
                r'total\s+employees[:\s]+(\d+)',
                r'workforce[:\s]+(\d+)',
                r'employ(?:ed|s)?\s+(\d+)\s+(?:people|staff|employees)',
                r'(\d+)\s+employees?\s+(?:were\s+)?employed',
                r'employment\s+of\s+(\d+)',
                r'(\d+)\s+(?:full|part).{0,20}time\s+employees?'
            ]
            
            for pattern in employee_patterns:
                matches = re.findall(pattern, text_content, re.IGNORECASE)
                if matches:
                    for match in matches:
                        try:
                            num = int(match)
                            # Reasonable range for SME employee counts
                            if 1 <= num <= 5000:
                                return str(num)
                        except ValueError:
                            continue
                            
        except Exception as e:
            self.logger.error(f"Error extracting from PDF: {e}")
            
        return ""
        
    def get_companies_house_address(self, company_number: str) -> str:
        """Get registered address from Companies House"""
        try:
            url = f"https://find-and-update.company-information.service.gov.uk/company/{company_number}"
            self.random_delay()
            
            response = self.session.get(url, timeout=15)
            if response.status_code != 200:
                return ""
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find registered office address
            address_section = soup.find('div', {'id': 'company-addresses'})
            if not address_section:
                return ""
                
            # Extract address text
            address_text = address_section.get_text()
            address_text = re.sub(r'Registered office address', '', address_text)
            address_text = re.sub(r'\s+', ' ', address_text).strip()
            
            return address_text
            
        except Exception as e:
            self.logger.error(f"Error getting address for {company_number}: {e}")
            return ""
            
    def enrich_company(self, row: pd.Series) -> Dict:
        """Enrich a single company with high-quality data"""
        company_name = str(row['CompanyName']).strip()
        company_number = str(row.get('CompanyNumber', '')).strip()
        
        if company_number == 'nan':
            company_number = ''
            
        self.logger.info(f"Processing: {company_name}")
        
        info = {
            'company_url': '',
            'description': '',
            'employees_2024': '',
            'employees_2023': '',
            'employees_2022': '',
            'manufacturing_location': ''
        }
        
        # Skip Chinese companies (they won't have meaningful UK websites)
        if any(company_name.upper().startswith(prefix) for prefix in ['ZHEJIANG', 'ZHENGZHOU', 'ZHENPING', 'ZHONGSHAN']):
            self.logger.info(f"Skipping Chinese company: {company_name}")
            if company_number:
                info['manufacturing_location'] = self.get_companies_house_address(company_number)
            return info
            
        # 1. Find official website
        website = self.find_official_website(company_name, company_number)
        if website:
            info['company_url'] = website
            
            # 2. Extract company description from website
            description = self.extract_company_description(website, company_name)
            if description:
                sic_codes = [str(row.get(f'SICCode.SicText_{i}', '')) for i in range(1, 5)]
                validation = self.validator.validate_company_description(description, company_name, sic_codes)
                if validation["valid"]:
                    info['description'] = description
                    
        # 3. Get employee data from Companies House accounts
        if company_number:
            employee_data = self.get_employee_data_from_accounts(company_number)
            info.update(employee_data)
            
            # 4. Get registered address
            address = self.get_companies_house_address(company_number)
            if address:
                info['manufacturing_location'] = address
                
        return info
        
    def process_csv(self, input_file: str, output_file: str = None):
        """Process CSV with quality controls"""
        try:
            df = pd.read_csv(input_file)
            self.logger.info(f"Loaded {len(df)} companies from {input_file}")
            
            # Add new columns
            new_columns = ['company_url', 'description', 'employees_2024', 'employees_2023', 'employees_2022', 'manufacturing_location']
            for col in new_columns:
                if col not in df.columns:
                    df[col] = ''
                    
            if not output_file:
                output_file = input_file.replace('.csv', '_quality_enriched.csv')
                
            processed = 0
            for index, row in df.iterrows():
                try:
                    enriched_info = self.enrich_company(row)
                    
                    # Update with quality-validated data only
                    for key, value in enriched_info.items():
                        if value and str(value).strip():
                            df.at[index, key] = str(value).strip()
                            
                    processed += 1
                    
                    # Save progress
                    if processed % 3 == 0:
                        df.to_csv(output_file, index=False)
                        self.logger.info(f"Processed {processed}/{len(df)} companies")
                        
                except Exception as e:
                    self.logger.error(f"Error processing {row.get('CompanyName', 'Unknown')}: {e}")
                    continue
                    
            df.to_csv(output_file, index=False)
            self.logger.info(f"Quality enrichment complete! Results saved to {output_file}")
            
            # Quality report
            for col in new_columns:
                filled = (df[col] != '').sum()
                self.logger.info(f"{col}: {filled}/{len(df)} ({filled/len(df)*100:.1f}%) filled")
                
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing file: {e}")
            return None

def main():
    # Install required package if not available
    try:
        import PyPDF2
    except ImportError:
        print("Installing PyPDF2 for PDF processing...")
        import subprocess
        subprocess.check_call(['pip', 'install', 'PyPDF2'])
        import PyPDF2
    
    agent = CompanyEnrichmentAgent(delay_range=(4, 7))  # Respectful delays
    
    input_file = "industrials_enriched.csv"
    output_file = "industrials_quality_enriched.csv"
    
    enriched_df = agent.process_csv(input_file, output_file)
    
    if enriched_df is not None:
        print("\nQuality Enrichment Summary:")
        new_cols = ['company_url', 'description', 'employees_2024', 'employees_2023', 'employees_2022', 'manufacturing_location']
        
        for col in new_cols:
            if col in enriched_df.columns:
                filled = (enriched_df[col] != '').sum()
                print(f"{col}: {filled}/{len(enriched_df)} ({filled/len(enriched_df)*100:.1f}%)")
                
        print("\nSample results:")
        sample = enriched_df[['CompanyName'] + new_cols].head(3)
        for _, row in sample.iterrows():
            print(f"\n{row['CompanyName']}:")
            for col in new_cols:
                if row[col]:
                    print(f"  {col}: {row[col][:100]}...")

if __name__ == "__main__":
    main()