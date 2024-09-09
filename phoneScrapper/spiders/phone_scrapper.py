import os
import re
import sys
import time
import scrapy
import phonenumbers
import pandas as pd
from scrapy import signals
from pydispatch import dispatcher
from phoneScrapper.items import PhoneScrapperItem
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TimeoutError


# Determine the base path based on whether the script is bundled or not
if getattr(sys, 'frozen', False):
    base_path = sys._MEIPASS
    base_path = base_path 
else:
    # Adjust this if your spider is executed differently
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = os.path.abspath(os.path.join(base_path, '..', '..'))

# csv_file_path = os.path.join(base_path, 'dataset', 'Country_zip.csv')
csv_file_path = os.path.join(base_path, 'dataset', 'Country_zip.csv') # r'phoneScrapper\dataset\Country_zip.csv'

print(base_path)
print(f"CSV File Path: {csv_file_path}")

class PhoneScrapperSpider(scrapy.Spider):
    name = "phone_scrapper"

    def __init__(self, domains=None, pause_event=None, excel_file_path=csv_file_path, *args, **kwargs):
        super(PhoneScrapperSpider, self).__init__(*args, **kwargs)
        self.domains = domains or []
        self.pause_event = pause_event  
        self.urls_scraped = 0
        self.total_urls = len(self.domains)
        self.visited_urls = set()
        self.processed_urls = set() 
        self.social_media_domains = ['facebook.com', 'twitter.com', 'instagram.com', 'youtube.com']
        self.parent_url_phone_numbers = {}
        self.processed_phone_numbers = set() 
        self.zip_to_country = self.load_zip_to_country(excel_file_path)
        dispatcher.connect(self.spider_closed, signals.spider_closed)

        self.unwanted_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg',
                                    '.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv',
                                    '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.pdf',
                                    '.zip', '.rar', '.tar', '.gz', '.7z',
                                    '.js', '.css')
        
        # self.prioritized_patterns = [
        #     r'\(\d{3}\)\s?\d{3}[-\s]?\d{4}',  
        #     r'\d{3}[\s-]\d{3}[-\s]\d{4}',  
        #     r'\(\d{3}\)\d{3}[-\s]?\d{4}',  
        #     r'\(\d{3}\)[-\s]?\d{3}[-\s]?\d{4}',  
        #     r'\(\d{3}\)\s?\d{3}[-\s]?\d{4}',
        #     r'\d{3}[.]\d{3}[.]\d{4}',  
        #     r'\d{3}-\d{3}-\d{4}',  
        #     r'\+\d{1}[\s]?\d{10}',  
        #     r'\+\d{1}[\s]?\d{3}[.]\d{3}[.]\d{4}',  
        #     r'\+\d{1}[\s]?\d{3}[-]\d{3}[-]\d{4}',  
        #     r'1-\d{3}-\d{3}-\d{4}', 
        #     r'\d{10}',  
        #     r'1\s?\d{10}',  
        #     r'1\s?\d{3}[.]\d{3}[.]\d{4}',  
        #     r'1\s?\d{3}[-]\d{3}[-]\d{4}',  
        #     r'1\(\d{3}\)\d{7}',  
        #     r'1\(\d{3}\)[-]\d{3}[-]\d{4}',  
        #     r'\+\d{1}[\s]?\d{3}[-\s]\d{3}[-\s]\d{4}',  
        #     r'\+\d{1}[\s]?\d{3}[-]\d{3}[-][A-Z]{4}', 
        #     r'\d{3}[-]\d{3}[-][A-Z]{4}', 
        #     r'\d{3}[.]\d{3}[.][A-Z]{4}',  
        #     r'\(\d{3}\)[-]\d{3}[-][A-Z]{4}', 
        #     r'\(\d{3}\)\d{3}[-][A-Z]{4}',  
        #     r'\d{3}\s\d{3}\s[A-Z]{3}',  
        #     r'\d{3}\s\d{3}[-][A-Z]{4}',  
        #     r'\(\d{3}\)\s\d{3}[-][A-Z]{4}',  
        #     r'1-\d{3}-\d{3}-[A-Z]{4}', 
        #     r'\d{3}\s\d{3}\s\d{4}', 
        #     r'1\s\d{3}[.]\d{3}[.][A-Z]{4}',  
        #     r'1\s\d{3}[-]\d{3}[-][A-Z]{4}', 
        #     r'\(\d{3}\)\s?\d{3}[-\s]?\d{4}', 
        #     r'\d{3}[\s-]\d{3}[-\s]\d{4}',     
        #     r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',
        #     r'\(\d{3}\)[\s-]?\d{3}[-\s]?\d{4}', 
        #     r'\+\d{1}[\s]?\d{10}',
        #     r'1[-\s]?\d{3}[-\s]?\d{3}[-\s]?\d{4}', 
        #     r'\d{3}[-\s]\d{3}[-\s]\d{4}',   
        #     r'\+\d{1}[\s]?\d{3}[-\s]?\d{3}[-\s]?\d{4}'
        # ]
        self.prioritized_patterns = [
            r'\(\d{3}\)\s?\d{3}[-\s]?\d{4}',  # Matches: (123) 456-7890, (123) 456 7890
            r'\d{3}[\s-]\d{3}[-\s]\d{4}',     # Matches: 123-456-7890, 123 456 7890
            r'\d{3}[.]\d{3}[.]\d{4}',         # Matches: 123.456.7890
            r'\+\d{1}[\s]?\d{10}',            # Matches: +1 1234567890
            r'\d{10}',                        # Matches: 1234567890
            r'1[-\s]?\d{3}[-\s]?\d{3}[-\s]?\d{4}' # Matches: 1 123-456-7890
        ]

    def load_zip_to_country(self, excel_file_path):
        df = pd.read_csv(excel_file_path)
        zip_to_country = dict(zip(df['Zip'], df['Country']))
        return zip_to_country

    def start_requests(self):
        urls = [self.convert_to_url(domain) for domain in self.domains]
        self.logger.info(f"Starting requests for {len(urls)} URLs")
        for url in urls:
            self.logger.info(f"Requesting URL: {url}")
            while self.pause_event.is_set():  # Check pause event
                self.logger.info(f"Pausing URL request: {url}")
                time.sleep(1)
            yield scrapy.Request(url=url, callback=self.parse, errback=self.errback_handle, meta={'parent_url': url, 'is_parent': True})

    def parse(self, response):
        parent_url = response.meta.get('parent_url')
        is_parent = response.meta.get('is_parent', False)
        self.logger.info(f"Parsing URL: {response.url} with parent: {parent_url}")

        # Skip unwanted file types
        if any(response.url.lower().endswith(ext) for ext in self.unwanted_extensions):
            self.logger.info(f"Skipping unwanted file type: {response.url}")
            return

        # Avoid revisiting the same URL
        if response.url in self.visited_urls:
            self.logger.info(f"Already visited URL: {response.url}")
            return
        self.visited_urls.add(response.url)

        # Extract phone numbers from the current page
        phone_numbers_with_countries = self.extract_phone_numbers(response)
        if phone_numbers_with_countries:
            if parent_url not in self.processed_urls:
                self.processed_urls.add(parent_url)
            if parent_url not in self.parent_url_phone_numbers:
                self.parent_url_phone_numbers[parent_url] = set()
            new_phone_numbers = set(phone_numbers_with_countries) - self.parent_url_phone_numbers[parent_url]
            if new_phone_numbers:
                self.parent_url_phone_numbers[parent_url].update(new_phone_numbers)
                self.logger.info(f"Extracted phone numbers: {new_phone_numbers} from {response.url}")

                # Yield the item with updated phone numbers, only if the URL has not been processed
                print("parent_url not in self.processed_urls")
                print(parent_url in self.processed_urls)
                print(parent_url)
                print(self.processed_urls)
                if parent_url in self.processed_urls:
                    self.logger.info(f"Extracted phone numbers: {new_phone_numbers} to parent {parent_url}")
                    self.processed_urls.add(parent_url)
                    item = PhoneScrapperItem()
                    item['url'] = parent_url
                    for i, (phone_number, country_code) in enumerate(list(self.parent_url_phone_numbers[parent_url])[:3]):
                        item[f'phone_number_{i+1}'] = phone_number
                        item[f'country_{i+1}'] = country_code
                    yield item

                # Stop if we already have 3 phone numbers for this parent URL
                if len(self.parent_url_phone_numbers[parent_url]) >= 3:
                    return

        # Follow only specific links if this is the parent URL
        if is_parent and len(self.parent_url_phone_numbers.get(parent_url, [])) < 3:
            links = response.css('a::attr(href)').getall()
            self.logger.info(f"Found {len(links)} links on {response.url}")
            for link in links:
                if self.is_relevant_link(response.url, link) and not self.is_social_media_link(link):
                    self.logger.info(f"Following relevant link: {link}")
                    while self.pause_event.is_set():  # Check pause event
                        self.logger.info(f"Pausing URL follow: {link}")
                        time.sleep(1)
                    yield response.follow(link, self.parse, meta={'parent_url': parent_url})

    def is_relevant_link(self, base_url, link):
        """
        Check if the link is relevant (i.e., home page, contact us, about us, services).
        """
        keywords = ['contact', 'about', 'service', 'call', 'support', 'help', 'location', 'store', 'quote']
        for keyword in keywords:
            if re.search(keyword, link, re.IGNORECASE):
                return True
        return False

    def is_internal_link(self, base_url, link):
        return link.startswith('/') or base_url in link

    def is_social_media_link(self, link):
        for domain in self.social_media_domains:
            if domain in link:
                self.logger.info(f"Skipping social media link: {link}")
                return True
        return False

    def extract_phone_numbers(self, response):
        phone_numbers_with_countries = []
        seen_numbers = set()

        # Extract ZIP codes from the page
        zip_codes = self.extract_zip_codes(response)

        print("span with class number")
        # Extract phone numbers from span tags with specific class or id
        span_tags = response.xpath('//span[contains(@class, "contact") or contains(@class, "number") or contains(@id, "contact") or contains(@id, "number")]')
        for span in span_tags:
            text = span.xpath('string()').get()
            if text:
                formatted_number = self.format_phone_number(text)
                normalized_number = self.normalize_phone_number(formatted_number)
                print("Format Number ", formatted_number)
                print("Normalized Number ", normalized_number)
                if self.is_valid_phone_number(formatted_number):
                    if formatted_number not in seen_numbers:
                        seen_numbers.add(normalized_number)
                        self.processed_phone_numbers.add(normalized_number)
                        country = self.get_country_from_zip(zip_codes) or self.get_country_from_number(formatted_number)
                        phone_numbers_with_countries.append((formatted_number, country))
                        print(seen_numbers)
                        if len(phone_numbers_with_countries) >= 3:
                                return phone_numbers_with_countries

        print("phone from specific phrases")
        # Extract phone numbers from text with specific phrases
        specific_phrases = response.xpath('//span[contains(text(), "call us at") or contains(text(), "+")]')
        for span in specific_phrases:
            text = span.xpath('string()').get()
            if text:
                # Look for phone numbers after "call us at" or starting with "+"
                phone_pattern = re.compile(r'(?<=call us at\s)\+?\d[\d\s-]+|\+\d[\d\s-]+')
                matches = phone_pattern.findall(text)
                for match in matches:
                    full_number = "".join(match).strip()
                    formatted_number = self.format_phone_number(full_number)
                    normalized_number = self.normalize_phone_number(formatted_number)
                    print("Formatted Number: ", formatted_number)
                    print("Normalized Number ", normalized_number)
                    if self.is_valid_phone_number(formatted_number):
                        if normalized_number not in seen_numbers:
                            seen_numbers.add(normalized_number)
                            self.processed_phone_numbers.add(normalized_number)
                            country = self.get_country_from_zip(zip_codes) or self.get_country_from_number(formatted_number)
                            phone_numbers_with_countries.append((formatted_number, country))
                            print(seen_numbers)
                            if len(phone_numbers_with_countries) >= 3:
                                return phone_numbers_with_countries

        print("tel from")
        # Extract phone numbers from the current page
        hrefs = response.xpath('//a[starts-with(@href, "tel:")]/@href').getall()
        self.logger.info(f"Phone numbers in href: {hrefs}")
        for href in hrefs:
            phone_number = href.split("tel:")[-1]
            formatted_number = self.format_phone_number(phone_number)
            normalized_number = self.normalize_phone_number(formatted_number)
            print("Format Number ", formatted_number)
            print("Normalized Number ", normalized_number)
            if self.is_valid_phone_number(formatted_number):
                if normalized_number not in seen_numbers:
                    seen_numbers.add(normalized_number)
                    self.processed_phone_numbers.add(normalized_number)
                    country = self.get_country_from_zip(zip_codes) or self.get_country_from_number(formatted_number)
                    phone_numbers_with_countries.append((formatted_number, country))
                    print(seen_numbers)
                    if len(phone_numbers_with_countries) >= 3:
                        return phone_numbers_with_countries

        print("phone from others")
        # Extract phone numbers from text content
        tags = response.xpath('//p | //span | //div | //li | //strong | //em | //footer | //section | //header | //aside | //blockquote | //address | //nav | //small | //article | //h1 | //h2 | //h3 | //h4 | //h5 | //h6')
        for tag in tags:
            text = tag.xpath('string()').get()
            # self.logger.info(f"Processing tag content: {text}")
            for pattern in self.prioritized_patterns:
                prioritized_phone_pattern = re.compile(pattern)
                prioritized_matches = prioritized_phone_pattern.findall(text)
                for match in prioritized_matches:
                    full_number = "".join(match).strip()
                    formatted_number = self.format_phone_number(full_number)
                    normalized_number = self.normalize_phone_number(formatted_number)
                    print("Format Number ", formatted_number)
                    print("Normalized Number ", normalized_number)
                    if self.is_valid_phone_number(formatted_number) and not self.is_css_number(tag, full_number):
                        if formatted_number not in seen_numbers:
                            seen_numbers.add(normalized_number) 
                            self.processed_phone_numbers.add(normalized_number)
                            country = self.get_country_from_zip(zip_codes) or self.get_country_from_number(formatted_number)
                            phone_numbers_with_countries.append((formatted_number, country))
                            if len(phone_numbers_with_countries) >= 3:
                                return phone_numbers_with_countries

        return phone_numbers_with_countries

    def extract_zip_codes(self, response):
        # Regular expression for U.S. ZIP codes and Canadian postal codes
        zip_pattern = re.compile(
            r'\b\d{5}(?:-\d{4})?\b'  # U.S. 5-digit or ZIP+4 format
            r'|[A-Za-z]\d[A-Za-z][ ]?\d[A-Za-z]\d'  # Canadian postal code format
        )
        zip_codes = set()
        # Extract text from various tags while excluding phone numbers
        tags = response.xpath('//p | //span | //div | //li | //strong | //em | //footer | //section | //header | //aside | //blockquote | //address | //nav | //small | //article | //h1 | //h2 | //h3 | //h4 | //h5 | //h6')
        for tag in tags:
            text = tag.xpath('string()').get()
            if text:
                # Skip any phone number format by additional checks
                if not any(keyword in text for keyword in ["tel:", "+", "call"]):
                    matches = zip_pattern.findall(text)
                    for match in matches:
                        if match:
                            before, after = text.split(match, 1)
                            if not (before and before[-1].isdigit()) and not (after and after[0].isdigit()):
                                zip_codes.add(match.strip())
                    
        return zip_codes

    def get_country_from_zip(self, zip_codes):
        for zip_code in zip_codes:
            if zip_code in self.zip_to_country:
                return self.zip_to_country[zip_code]
        return None

    def get_country_from_number(self, phone_number):
        try:
            parsed_number = phonenumbers.parse(phone_number)
            if phonenumbers.is_valid_number(parsed_number):
                country = phonenumbers.region_code_for_number(parsed_number)
                if country == '' and (phone_number.startswith('1') or phone_number.startswith('+1')):
                    country = "US"
                if (phone_number.startswith('91') or phone_number.startswith('+91')):
                    country = "IN"
                return country
        except phonenumbers.NumberParseException:
            pass
        return None

    def convert_to_url(self, domain):
        return f"https://{domain}"

    def format_phone_number(self, phone_number):
        # Removing every character except digits and the plus sign
        formatted_number = re.sub(r'[^\d+]', '', phone_number)
        formatted_number = formatted_number.replace(' ', '')
        return formatted_number
    
    def normalize_phone_number(self, phone_number):
        # Normalize phone number by stripping leading '1'
        phone_number = re.sub(r'[^\d]', '', phone_number)
        if phone_number.startswith('1') and len(phone_number) > 10:
            phone_number = phone_number[1:]
        return phone_number

    def is_valid_phone_number(self, phone_number):
        digits = re.sub(r'\D', '', phone_number)
        # self.logger.info(f"Digits extracted: {digits}")
        if len(digits) < 10 or len(digits) > 12:
            # self.logger.info(f"Phone number {phone_number} invalid due to length")
            return False

        if re.match(r'(\d)\1{6,}', digits):  
            # self.logger.info(f"Phone number {phone_number} invalid due to repeated digits")
            return False

        if "168" in digits or len(digits) > 12:  
            # self.logger.info(f"Phone number {phone_number} invalid due to suspicious patterns")
            return False
        
        if re.search(r'[-_]?\d{4}[-_]\d{4}', phone_number):
            # self.logger.info(f"Phone number {phone_number} invalid due to invalid format")
            return False
        
        id_patterns = [
            r'shopify-\w+', 
            r'template--\w+',
            r'section-\w+',
            r'ImageWithText-\w+',
        ]
        for pattern in id_patterns:
            if re.search(pattern, phone_number):
                # self.logger.info(f"Phone number {phone_number} invalid due to matching an ID pattern")
                return False
        
        if digits.startswith("1790"):
            # self.logger.info(f"Phone number {phone_number} invalid due to invalid prefix")
            return False
        
        valid_phone_pattern = re.compile(    
        # r'(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}'
        r'(\+\d{1,2}\s?)?(\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})'
        )
        if valid_phone_pattern.match(phone_number):
            # self.logger.info(f"Phone number {phone_number} is valid")
            return True

        return True

    def is_css_number(self, tag, full_number):
        parent = tag.xpath('parent::*')
        if parent:
            for attr in ['class', 'id', 'style']:
                attr_value = parent.xpath(f'@{attr}').get()
                if attr_value and full_number in attr_value:
                    self.logger.info(f"Phone number {full_number} filtered out as it appears in CSS attribute: {attr}={attr_value}")
                    return True
        return False


    def errback_handle(self, failure):
        self.logger.error(repr(failure))
        
        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error(f"HTTP error on {response.url}: {response.status}")
        elif failure.check(DNSLookupError):
            request = failure.request
            self.logger.error(f"DNS lookup error on {request.url}")
        elif failure.check(TimeoutError):
            request = failure.request
            self.logger.error(f"Timeout error on {request.url}")

    def spider_closed(self, spider):
        self.logger.info(f"Spider closed: {spider.name}")
        for parent_url, phone_numbers_with_countries in self.parent_url_phone_numbers.items():
            item = PhoneScrapperItem()
            item['url'] = parent_url
            for i, (phone_number, country_code) in enumerate(list(phone_numbers_with_countries)[:3]):
                item[f'phone_number_{i+1}'] = phone_number
                item[f'country_{i+1}'] = country_code
            self.crawler.signals.send_catch_log(signal=signals.item_scraped, item=item)
