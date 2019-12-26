import scrapy
import json


class SrealitySpider(scrapy.Spider):
    name = "sreality"

    def start_requests(self):
        url = "https://www.sreality.cz/api/cs/v2/estates?per_page=500&page=1"
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        r = json.loads(response.body)
        # parsing links to estates in initial api response
        for estate in r["_embedded"]["estates"]:
            # hash id as a unique identifier > we do not want to scrape the same estate N times
            hash_id = estate["hash_id"]
            # url is provided as '/cs/v2/estates/1077595740'
            url_suffix = estate["_links"]["self"]["href"]
            # so we combine it with the prefix from supplied url in self.start_requests
            url_prefix = response.url.split("/cs")[0]
            url_estate = url_prefix + url_suffix
            if url_estate is not None:
                yield response.follow(url_estate, callback=self.save_estate)

        # get number of pages as ... 'Number of total estates/number of estates per page'
        page_count = r["result_size"] // 500 + 1
        # we need to generate new page after we scrape estates on the current one
        # (2, page_count + 1) due to how range works and we already have the first page
        for page_num in range(2, 5):
            # split at "&page=\d" with the next page's number and callback to parse
            first_page = response.url.split("&")[0]
            next_page = first_page + f"&page={page_num}"
            if next_page is not None:
                yield response.follow(next_page, callback=self.parse)

    def save_estate(self, response):
        data = json.loads(response.body)
        # get estate id from url and save to corresponding json
        estate_id = response.url.split("/")[-1]
        with open(f"estates/{estate_id}.json", "w") as js:
            json.dump(data, js)
