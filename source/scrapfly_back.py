from scrapfly import ScrapeConfig, ScrapflyClient
from bs4 import BeautifulSoup
from datetime import date
from time import sleep
import logging
import requests
import json
import configparser
import os
import boto3
import math
import time

# Read config file
cd = os.getcwd()
config = configparser.ConfigParser()
config.read(f'{cd}/../config.cfg')

# Get urls
ACCOUNT_URL = 'https://api.scrapfly.io/account'
FIVERR_URL = 'https://www.fiverr.com'
CATEGORIES_URL = f'{FIVERR_URL}/categories'
FILTER_URL = 'seller_language%3Aen%7Cseller_location%3ACA%2CUS'


class ScrapflyBack():

    def __init__(self) -> None:
        """
        Initialize the class with the Scrapfly and S3 clients
        """
        # Get the current date
        current_date = date.today()
        self.today = current_date.strftime('%Y-%m-%d')

        # Logging setup validate if the logging is already setup
        if len(logging.getLogger().handlers) > 0:
            logging.getLogger().setLevel(logging.INFO)
        else:
            logging.basicConfig(filename=f'{self.today}.log', encoding='utf-8', level=logging.INFO)

        # Get the Scrapfly API key and Client
        self.scrapfly_requests = 0
        self.main_key = config.get('scrapyFly', 'api_key')
        self.scrapfly = ScrapflyClient(key=self.main_key)

        # Get the S3 client and bucket name
        self.bucket_name = config.get('S3', 'bucketName')
        region = config.get('S3', 'region')
        access = config.get('S3', 'accessKeyId')
        secret = config.get('S3', 'secretAccessKey')
        self.s3_client = boto3.client(
            's3', region_name=region, aws_access_key_id=access, aws_secret_access_key=secret)

    def account_info(self, key):
        """
        Get account info from Scrapfly API
        """
        response = requests.get(f'{ACCOUNT_URL}?key={key}')
        info = response.json()
        return info

    def get_remaining(self, key):
        """
        Get remaining requests from Scrapfly API
        """
        try:
            account = self.account_info(key)
            subscription = account.get('subscription')
            usage = subscription.get('usage')
            scrape = usage.get('scrape')
            remaining = scrape.get('remaining')
            return remaining
        except:
            logging.error('Error: could not get remaining requests')
            return None

    def save_json(self, data, key):
        """
        save the json data to S3
        """
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(data),
                ContentType='application/json'
            )
        except Exception as e:
            logging.error(f'Error: at save_json: {str(e)}')

    def get_soup(self, url):
        """
        get soup from url scraped from Scrapfly API
        """
        config = ScrapeConfig(url=url)
        response = self.scrapfly.scrape(scrape_config=config)
        html = response.scrape_result['content']
        soup = BeautifulSoup(html, 'html.parser')
        self.scrapfly_requests += 1
        return soup

    def get_category(self, url):
        """
        get category indicators from the url
        """
        items = url.split('/')
        if len(items) < 5:
            items.append(items[3])
        return {
            'url': FIVERR_URL + url,
            'category': items[2],
            'subCategory': items[3],
            'nestedSubCategory': items[4]
        }

    def get_categories(self):
        """
        get the categories from the fiverr website
        """
        try:
            # get categories from s3 bucket
            key = f"fiverr/categories/categories.json"
            s3_object = self.s3_client.get_object(
                Bucket=self.bucket_name, Key=key)['Body']
            return json.loads(s3_object.read().decode('utf-8'))
        except:
            try:
                # get categories from fiverr website
                soup = self.get_soup(CATEGORIES_URL)
                categories = []
                items = soup.select('section > ul > li > a')
                for a in items:
                    href = a.get('href')
                    if href:
                        category = self.get_category(href)
                        categories.append(category)
                return categories
            except Exception as e:
                logging.error(f'Error: at get_categories: {str(e)}')

    def get_data(self, url):
        """
        return the json data from backend returned from the script "perseus-initial-props"
        """
        try:
            logging.info(f'url: {url}')
            sleep(1)
            soup = self.get_soup(url)
            script = soup.select('script#perseus-initial-props')[0].text
            return json.loads(script)
        except Exception as e:
            logging.error(f'Error: {e}')

    def get_pagination(self, data):
        """
        Get the total number of pages and offset from the data
        """
        appData = data.get('appData')
        pagination = appData.get('pagination')
        offset = pagination.get('offset')
        total = pagination.get('total')
        size = pagination.get('page_size')
        pages = math.ceil(total / size)
        return pages, offset

    def get_pages(self, category, max_pages, max_requests):
        """
        Get the total number of pages and data from the category with each service
        """
        # local variables
        services = []
        current = 1

        # partitions in subcategories
        categoryName = category.get("category")
        subCategory = category.get("subCategory")
        nestedSubCategory = category.get("nestedSubCategory")
        logging.info(f"""\n category: {categoryName} \n subCategory: {subCategory} \n nestedSubCategory: {nestedSubCategory} \n""")

        # get category data
        source = 'drop_down_filters'
        url = f'{category.get("url")}?source={source}&ref={FILTER_URL}'
        category_data = self.get_data(url)

        # validate category data
        if category_data:
            category_update = {}
            category_update.update(category_data.get('categoryIds'))
            category_update.update(category_data.get('displayData'))
            category_update.update(category_data.get('facets'))

            # save categories files to s3
            categoryIds = category_data.get('categoryIds')
            title = '-'.join(categoryIds.values())
            key = f'fiverr/categories/{title}.json'
            self.save_json(category_update, key)

            # get total services
            total, offset = self.get_pagination(category_data)
            total = min(total, max_pages)
            while current <= total and category_data:
                services = services + category_data.get('items')
                if current == total:
                    break

                # get next page
                current += 1
                source = 'pagination'
                url = f'{category.get("url")}?source={source}&ref={FILTER_URL}&page={current}&offset={offset}'
                category_data = self.get_data(url)

            # restrict the number of requests and log the number of requests
            services = services[:max_requests]
            logging.info(f'\n total services: {len(services)} \n')

            # save services files to s3
            for i, service in enumerate(services):
                # get service info
                gig_url = FIVERR_URL + service.get('gig_url')
                gig_data = self.get_data(gig_url)

                if gig_data:
                    category['date'] = self.today
                    category['status'] = 'success'

                    gig_data['url'] = gig_url
                    gig_data['rank'] = i + 1
                    gig_data['date'] = self.today

                    # save to s3
                    title = gig_data.get("general", {}).get("gigId")
                    key = f'fiverr/accounts/{categoryName}/{subCategory}/{nestedSubCategory}/{title}.json'
                    self.save_json(gig_data, key)
        else:
            logging.error(f'Error: could not get category data for {url}')
        return category

    def run(self, max_pages=2, max_requests=100, max_categories=None):
        """
        Run the script
        """
        logging.info(f'---------------------[START]---------------------')
        # start timer
        start = time.time()

        # Get the remaining requests
        remaining_before = self.get_remaining(self.main_key)

        # Get the categories
        categories = self.get_categories()
        logging.info(f'categories: {len(categories)}')
        cant = 0

        # Get the categories data
        for category in categories:
            try:
                # validate max_categories constraint
                if max_categories and max_categories <= cant:
                    logging.info(f'Info: max categories reached {max_categories}')
                    break

                # validate remaining requests constraint
                remaining = self.get_remaining(self.main_key)
                if remaining < max_pages + max_requests:
                    logging.error(f'Error: not enought remaining requests {remaining}')
                    break

                # validate if the category is already scraped today
                category_date = category.get('date')
                category_status = category.get('status')
                if category_date != self.today and category_status != 'success':
                    category = self.get_pages(
                        category, max_pages, max_requests)
                    cant += 1
                else:
                    categoryName = category.get("category")
                    logging.info(f'Info: category {categoryName} already scraped')
            except Exception as e:
                logging.error(f'Error: {str(e)}')
                continue

        # save categories files to s3
        key = f'fiverr/categories/categories.json'
        self.save_json(categories, key)

        # Get the remaining requests
        remaining_after = self.get_remaining(self.main_key)
        end = time.time()
        time_elapsed = end - start

        # results
        logging.info(f'requests before: {remaining_before}')
        logging.info(f'requests after: {remaining_after}')
        logging.info(f'requests done: {self.scrapfly_requests}')
        logging.info(f'elapsed time: {time_elapsed} seg')
        logging.info(f'average time: {time_elapsed / self.scrapfly_requests} seg')
        logging.info(f'---------------------[END]---------------------')


if __name__ == '__main__':
    fiverr = ScrapflyBack()
    # max number of pages to scrape per category
    pages = 2
    # max number of requests to scrape per category
    max_req = 50 * pages
    # max number of categories to scrape
    bucks = 2
    # run the script
    fiverr.run(max_pages=pages, max_requests=max_req, max_categories=bucks)

