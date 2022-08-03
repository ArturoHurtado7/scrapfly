from scrapfly import ScrapeConfig, ScrapflyClient
from bs4 import BeautifulSoup
from datetime import date
import concurrent.futures
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

        # Logging setup
        logging.basicConfig(
            filename=f'{self.today}.log',
            format='%(asctime)s %(levelname)-8s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            encoding='utf-8',
            level=logging.INFO
        )

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

    def chunks(self, items_list, size):
        """
        Yield successive n-sized chunks from items list
        """
        for i in range(0, len(items_list), size):
            border = i + size
            yield items_list[i: border]

    def account_info(self, key):
        """
        Get account info from Scrapfly API
        """
        try:
            response = requests.get(f'{ACCOUNT_URL}?key={key}')
            info = response.json()
            return info
        except Exception as e:
            logging.error(f'Error: at account_info: {str(e)}')

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
            logging.info(f'Saved: {key}')
        except Exception as e:
            logging.error(f'Error: at save_json: {str(e)}')

    def get_soup(self, url):
        """
        get soup from url scraped from Scrapfly API
        """
        try:
            config = ScrapeConfig(url=url)
            response = self.scrapfly.scrape(scrape_config=config)
            html = response.scrape_result['content']
            soup = BeautifulSoup(html, 'html.parser')
            self.scrapfly_requests += 1
            return soup
        except Exception as e:
            logging.error(f'Error: at get_soup: {str(e)}')

    def get_category(self, url):
        """
        get category indicators from the url
        """
        response = {}
        response['url'] = FIVERR_URL + url
        items = url.split('/')
        if len(items) > 2:
            response['category'] = items[2]
        if len(items) > 3:
            response['subCategory'] = items[3]
        if len(items) < 5 and len(items) > 3:
            items.append(items[3])
        if len(items) > 4:
            response['nestedSubCategory'] = items[4]
        return response

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
            soup = self.get_soup(url)
            script = soup.select('script#perseus-initial-props')[0].text
            return json.loads(script)
        except Exception as e:
            logging.error(f'Error at get_data {e}')

    def get_concurrent_data(self, url_list):
        """
        get data collection from url list concurrently
        """
        data_collection = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_data = {executor.submit(
                self.get_data, url): url for url in url_list}
            for future in concurrent.futures.as_completed(future_data):
                url = future_data[future]
                try:
                    data = future.result()
                    data['url'] = url
                    data['date'] = self.today
                    data_collection.append(data)
                except Exception as exc:
                    logging.error(f'{url} generated an exception: {exc}')
        return data_collection

    def save_concurrent_json(self, data_collection):
        """
        get data collection from url list concurrently
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_data = {executor.submit(
                self.save_json, data[0], data[1]): data for data in data_collection}
            for future in concurrent.futures.as_completed(future_data):
                try:
                    future.result()
                except Exception as exc:
                    logging.error(f'save json generated an exception: {exc}')

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
        # get category first page data
        url = f'{category.get("url")}?source=drop_down_filters&ref={FILTER_URL}'
        category_data = self.get_data(url)

        # validate if there is category data
        if category_data:
            category_update = {}
            category_update.update(category_data.get('categoryIds'))
            category_update.update(category_data.get('displayData'))
            category_update.update(category_data.get('facets'))

            # save category basic info in s3
            categoryIds = category_data.get('categoryIds')
            title = '-'.join(categoryIds.values())
            key = f'fiverr/categories/{title}.json'
            self.save_json(category_update, key)

            # get total pages and offset and url pages for pagination
            total, offset = self.get_pagination(category_data)
            total = min(total, max_pages)
            url_list = []
            current = 2
            while current <= total:
                url_list.append(
                    f'{category.get("url")}?source=pagination&ref={FILTER_URL}&page={current}&offset={offset}')
                current += 1

            # get url for each service in pages
            services = category_data.get('items')
            services_list = [FIVERR_URL +
                             service.get('gig_url') for service in services]

            # concurrent get data from url list every 3 pages
            chunk_list = self.chunks(url_list, 3)
            for chunk in chunk_list:
                data_collection = self.get_concurrent_data(chunk)
                for category_data in data_collection:
                    items = category_data.get('items')
                    services_list = services_list + \
                        [FIVERR_URL + item.get('gig_url') for item in items]

            # restrict the number of requests and log the number of requests
            services_list = services_list[:max_requests]
            logging.info(f'total services to scrape: {len(services_list)}')

            # get data from url pages
            has_data = False
            chunk_list = self.chunks(services_list, 12)
            for chunk in chunk_list:
                # colection of data to be saved in s3
                collection = []
                data_collection = self.get_concurrent_data(chunk)
                # save to s3
                for gig_data in data_collection:
                    if gig_data:
                        title = gig_data.get("general", {}).get(
                            "gigId", "default")
                        key = f'fiverr/accounts/{category.get("category")}/{category.get("subCategory")}/{category.get("nestedSubCategory")}/{title}.json'
                        collection.append((gig_data, key))
                        has_data = True
                if collection:
                    logging.info(f'total services to save: {len(collection)}')
                    self.save_concurrent_json(collection)

            if has_data:
                category['date'] = self.today
                category['status'] = 'success'
        else:
            logging.error(f'Error: could not get category data for {url}')
        return category

    def run(self, max_pages=10, max_requests=100, max_categories=None):
        """
        Run the script
        """
        # start timer
        start = time.time()

        # Get the remaining requests
        remaining_before = self.get_remaining(self.main_key)

        # Get the categories
        categories = self.get_categories()
        logging.info(f'categories: {len(categories)}')
        scraped_categories = 0

        # Get the categories data
        for category in categories:
            try:
                # validate max_categories constraint
                if max_categories and max_categories <= scraped_categories:
                    logging.info(
                        f'Info: max categories reached {max_categories}')
                    break

                # validate remaining requests constraint
                remaining = self.get_remaining(self.main_key)
                if remaining < max_pages + max_requests:
                    logging.error(
                        f'Error: not enought remaining requests {remaining}')
                    break

                # validate if the category is already scraped today
                category_date = category.get('date')
                category_status = category.get('status')

                # if the category is already scraped today, skip it
                categoryName = category.get("category")
                subCategory = category.get("subCategory")
                nestedSubCategory = category.get("nestedSubCategory")
                if category_date != self.today or category_status != 'success':
                    logging.info(
                        f'category to scrape: {categoryName}/{subCategory}/{nestedSubCategory}')
                    category = self.get_pages(
                        category, max_pages, max_requests)
                    scraped_categories += 1
                else:
                    logging.info(
                        f'category: {categoryName}/{subCategory}/{nestedSubCategory} already scraped')
            except Exception as e:
                logging.error(f'Error at run {str(e)}')

        # save categories files to s3
        key = f'fiverr/categories/categories.json'
        self.save_json(categories, key)

        # Get the remaining requests
        remaining_after = self.get_remaining(self.main_key)
        end = time.time()
        time_elapsed = end - start

        # logging info
        logging.info(f'requests before: {remaining_before}')
        logging.info(f'requests after: {remaining_after}')
        logging.info(f'requests done: {self.scrapfly_requests}')
        logging.info(f'requests elapsed: {remaining_before - remaining_after}')
        logging.info(f'elapsed time: {time_elapsed} seg')
        if self.scrapfly_requests > 0:
            logging.info(
                f'average time: {time_elapsed / self.scrapfly_requests} seg')

        return scraped_categories


if __name__ == '__main__':
    fiverr = ScrapflyBack()
    # total categories to scrape
    total = 30
    # max number of pages to scrape per category
    pages = 2
    # max number of requests to scrape per category
    max_req = 48 * pages
    # max number of categories to scrape
    bucks = 2

    # loging info about the script
    logging.info(
        f'\n\n-------------------[START]-------------------\n\n')
    logging.info(f'total categories to scrape: {total} \n')
    logging.info(f'max number of pages to scrape per category: {pages}')
    logging.info(f'max number of requests to scrape per category: {max_req}')
    logging.info(f'max number of categories to scrape at once: {bucks} \n')

    # run the script
    scraped = fiverr.run(pages, max_req, bucks)
    # remove the scraped to the total categories
    total = total - scraped if scraped > 0 else 0

    # loging info about the script
    logging.info(f'left categories to scrape: {total}')
    logging.info(
        f'\n\n--------------------[END]--------------------\n\n')

