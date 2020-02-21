import scrapy
import json
import os
from datetime import datetime
from utils import connect_firestore, flatten_json
import requests
import logging


class SrealitySpider(scrapy.Spider):
    name = "sreality"
    _db = connect_firestore("../notebooks/estates-b0e1b-f074b83c9111.json")
    _stopping_count = 0

    def start_requests(self):
        # catch sample response and extract result size to calculate how many pages we need to scrape to get all estates
        # if there are 500 estates per page
        r = requests.get("https://www.sreality.cz/api/cs/v2/estates?per_page=1&page=1")
        results_count = r.json()["result_size"]
        page_count = results_count // 500 + 1
        urls = [
            f"https://www.sreality.cz/api/cs/v2/estates?per_page=500&page={num}"
            for num in range(page_count)
        ]
        for url in urls:
            if self._stopping_count < 18293:
                yield scrapy.Request(url=url, callback=self.parse)
            else:
                logging.info(f'Stopping count reached in start_requests. Stopping count: {self._stopping_count}.')
                break

    def parse(self, response):
        r = json.loads(response.body)
        # parsing links to estates in initial api response
        for estate in r["_embedded"]["estates"]:
            # url is provided as '/cs/v2/estates/1077595740'
            url_suffix = estate["_links"]["self"]["href"]
            # check if ID in db.collection
            estate_id = url_suffix.split("/")[-1]
            estate_exists = self._db.collection("sreality").document(estate_id).get().exists
            if not estate_exists:
                if self._stopping_count < 18293:
                    # we combine it with the prefix from supplied url in start_requests
                    logging.info(f'Processing estate {estate_id}.')
                    url_prefix = response.url.split("/cs")[0]
                    url_estate = url_prefix + url_suffix
                    if url_estate is not None:
                        yield response.follow(url_estate, callback=self.save_estate)
                else:
                    logging.info(f'Stopping count reached in parse. Stopping count: {self._stopping_count}.')
                    break

    def save_estate(self, response):
        # construct json to save
        data = json.loads(response.body)
        data["date_scraped"] = datetime.utcnow().strftime("%Y/%m/%d %H:%M:%S")
        # gc firestore doesn't allow nested arrays
        # implemented primitive<<< preprocessing of dicts
        data = flatten_json(data)
        # create document reference within sreality collection in db
        estate_id = response.url.split("/")[-1]
        doc_ref = self._db.collection("sreality").document(estate_id)
        doc_ref.set(data)
        logging.info(f'Estate {estate_id} saved to collection.')
        # increment by one to stop after 20000 requests due to limit
        self._stopping_count += 1
