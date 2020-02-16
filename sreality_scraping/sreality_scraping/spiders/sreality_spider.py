import scrapy
import json
import os
from datetime import datetime

class SrealitySpider(scrapy.Spider):
    name = "sreality"
    per_page = 500
    stopping_page_number = None

    def new_estate(self, url_suffix):
        """Check if estate has already been scraped or not by comparing scraped id to the ones in the estate folder.
        
        Arguments:
            url_suffix {string} -- url is provided as '/cs/v2/estates/1077595740'
        
        Returns:
            bool -- True if estate_id isn't already scraped (there's no json with this id in estates folder)
        """
        # read already scraped IDs
        folder = 'estates/'
        files = os.listdir(folder)
        old_ids = [file.split('.')[0] for file in files]
        # extract newly scraped ID from url
        new_id = url_suffix.split('/')[-1]
        # if new_id is not in old_ids, it is a new estate that hasn't been scraped
        return not new_id in old_ids

    def start_requests(self):
        url = f"https://www.sreality.cz/api/cs/v2/estates?per_page={self.per_page}&page=1"
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        r = json.loads(response.body)
        # get number of pages as ... 'Number of total estates/number of estates per page'
        page_count = r["result_size"] // self.per_page + 1
        # parsing links to estates in initial api response
        for estate in r["_embedded"]["estates"]:
            # url is provided as '/cs/v2/estates/1077595740'
            url_suffix = estate["_links"]["self"]["href"]
            if self.new_estate(url_suffix):    
                # we combine it with the prefix from supplied url in self.start_requests
                url_prefix = response.url.split("/cs")[0]
                url_estate = url_prefix + url_suffix
                if url_estate is not None:
                    yield response.follow(url_estate, callback=self.save_estate)
            # the api sorts by date, newest first
            # if the hit an already scraped estate we want to stop
            else:
                # update the page number when we want to stop scraping
                self.stopping_page_number = int(response.url.split('=')[-1])

        # we need to generate new page after we scrape estates on the current one
        # (2, page_count + 1) due to how range works and we already have the first page
        for page_num in range(2, page_count):
            # scrape only non-scraped estates
            if self.stopping_page_number is None and page_num <= self.stopping_page_number:
                # split at "&page=\d" with the next page's number and callback to parse
                first_page = response.url.split("&")[0]
                next_page = first_page + f"&page={page_num}"
                if next_page is not None:
                    yield response.follow(next_page, callback=self.parse)


    def save_estate(self, response):
        # get estate id from url and save to corresponding json
        estate_id = response.url.split("/")[-1]
        # construct json to save
        data = json.loads(response.body)
        data['estate_id'] = estate_id
        data['date_scraped'] = datetime.utcnow().strftime("%Y/%m/%d %H:%M:%S")
        with open(f"estates/{estate_id}.json", "w") as js:
            json.dump(data, js)
