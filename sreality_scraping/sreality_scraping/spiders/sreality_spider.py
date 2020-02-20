import scrapy
import json
import os
from datetime import datetime
from utils import load_cache, update_cache, connect_firestore, flatten_json


class SrealitySpider(scrapy.Spider):
    name = "sreality"
    per_page = 500
    cache = load_cache()
    db = connect_firestore("../notebooks/estates-b0e1b-f074b83c9111.json")
    stopping_count = 0
    def start_requests(self):
        url = (
            f"https://www.sreality.cz/api/cs/v2/estates?per_page={self.per_page}&page=1"
        )
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        r = json.loads(response.body)
        # get number of pages as ... 'Number of total estates/number of estates per page'
        page_count = r["result_size"] // self.per_page + 1
        # parsing links to estates in initial api response
        for estate in r["_embedded"]["estates"]:
            # url is provided as '/cs/v2/estates/1077595740'
            url_suffix = estate["_links"]["self"]["href"]
            estate_id = url_suffix.split("/")[-1]
            if estate_id not in self.cache:
                # we combine it with the prefix from supplied url in self.start_requests
                url_prefix = response.url.split("/cs")[0]
                url_estate = url_prefix + url_suffix
                if url_estate is not None:
                    yield response.follow(url_estate, callback=self.save_estate)

        # we need to generate new page after we scrape estates on the current one
        # (2, page_count + 1) due to how range works and we already have the first page
        for page_num in range(2, page_count):
            # scrape only non-scraped estates
            if self.stopping_count <= 2000:
                # split at "&page=\d" with the next page's number and callback to parse
                first_page = response.url.split("&")[0]
                next_page = first_page + f"&page={page_num}"
                if next_page is not None:
                    yield response.follow(next_page, callback=self.parse)

    def save_estate(self, response):
        # construct json to save
        data = json.loads(response.body)
        data["date_scraped"] = datetime.utcnow().strftime("%Y/%m/%d %H:%M:%S")
        # gc firestore doesn't allow nested arrays
        # implemented primitive<<< preprocessing of dicts
        data = flatten_json(data)
        # create document reference within sreality collection in db
        estate_id = response.url.split("/")[-1]
        doc_ref = self.db.collection("sreality").document(estate_id)
        doc_ref.set(data)
        # increment by one to stop after 2000 requests due to limit
        self.stopping_count += 1
        self.cache.append(estate_id)
        update_cache(self.cache)
