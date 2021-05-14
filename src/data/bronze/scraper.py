import logging
from typing import Dict

import requests


def start_requests(last_estate_id: str) -> Dict:
    """Scrape all newly added estates from API

    Args:
        last_estate_id (str): estate_id of the most recent estate added (from last scrape)

    Returns:
        Dict: result dict in format {estate_id_1: {estate_info_1}, ... estate_id_N: {estate_info_N}}
    """
    # Calculate number of API pages based on result size and estates per page
    base_url = 'https://www.sreality.cz/api/'
    res = requests.get(base_url + 'cs/v2/estates?per_page=1&page=1')
    num_pages = res.json()['result_size'] // 500

    # Obtain url suffix for each estate up until the newest from last scrape
    estate_urls = {}
    for page in range(num_pages):
        url = base_url + f'cs/v2/estates?per_page=500&page={page}'
        res = requests.get(url)
        estates = res.json()["_embedded"]["estates"]

        for estate in estates:
            estate_url = estate["_links"]["self"]["href"]
            estate_id = estate_url.split("/")[-1]

            # Break out of nested loop
            already_scraped = estate_id == last_estate_id
            if already_scraped:
                break

            estate_urls[estate_id] = estate_url

        # TODO: Replace with something smarter?
        if already_scraped:
            break

    if not estate_urls:
        return {}

    estates = {}
    logging.info('Scraping %i estates' % len(estate_urls))
    for _id, suffix in estate_urls.items():
        url = base_url + suffix
        res = requests.get(url)
        if res.status_code == 200:
            estates[_id] = res.json()
    return estates
