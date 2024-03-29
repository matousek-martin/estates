import logging
from functools import reduce
from typing import Dict, Any

import requests


def get_estate_urls(last_estate_id: str) -> Dict:
    """Fetch urls of newly added estates

    Args:
        last_estate_id (str): estate_id of the most recent estate added (from last scrape)

    Returns:
        Dict: result dict in format {estate_id_1: {estate_url_1}, ... estate_id_N: {estate_url_N}}
    """
    # Calculate number of API pages based on result size and estates per page
    base_url = 'https://www.sreality.cz/api/'
    res = requests.get(base_url + 'cs/v2/estates?per_page=1&page=1')
    num_pages = res.json()['result_size'] // 500

    # Obtain url suffix for each estate up until the newest from last scrape
    estate_urls = {}
    for page in range(1, num_pages):
        url = base_url + f'cs/v2/estates?per_page=500&page={page}'

        # EAFP
        try:
            res = requests.get(url)
            res.raise_for_status()
        except requests.exceptions.HTTPError as error:
            logging.error(error)

        # Some API responses are missing the content
        # which causes the entire scraper to fail
        res = res.json().get("_embedded")
        if res is None:
            continue
        estates = res["estates"]

        for estate in estates:
            estate_url = estate["_links"]["self"]["href"]
            estate_id = estate_url.split("/")[-1]

            # Break once we hit an estate from last scraping
            already_scraped = estate_id == last_estate_id
            if already_scraped:
                return estate_urls

            estate_urls[estate_id] = estate_url
    return estate_urls


def start_requests(urls: Dict) -> Dict:
    """Scrape newly added estates

    Args:
        urls (Dict): urls to scrape

    Returns:
        Dict: result dict in format {estate_id_1: {estate_items_1}, ... estate_id_N: {estate_items_N}}
    """
    base_url = 'https://www.sreality.cz/api/'
    estates = {}
    for _id, suffix in urls.items():
        url = base_url + suffix
        res = requests.get(url)
        if res.status_code == 200:
            estate = res.json()
            estates[_id] = parse(estate)
    return estates


def parse(estate: Dict) -> Dict:
    """Parse Sreality API response into a flat dictionary

    Args:
        estate (Dict): a dictionary containing estate information in the Sreality API format

    Returns:
        Dict: Flattened dictionary with data for the price prediction model and data for the website
    """
    estate_flattened = {
        # Web
        # Estate
        "estate_title": extract(estate, "name.value"),
        "estate_description_short": extract(estate, "meta_description"),
        "estate_description_long": extract(estate, "text.value"),
        "address": extract(estate, 'locality.value'),
        "estate_category_main_cb": extract(estate, "seo.category_main_cb"),
        "estate_disposition": extract(estate, "seo.category_sub_cb"),
        "estate_rental_or_sell": extract(estate, "seo.category_type_cb"),
        "estate_locality_district": extract(estate, "locality_district_id"),
        "estate_longitude": extract(estate, "map.lon"),
        "estate_latitude": extract(estate, "map.lat"),
        'estate_map_zoom': extract(estate, 'map.zoom'),
        "estate_images": [
            image["_links"]["view"]["href"]
            for image in extract(estate, "_embedded.images", default=[])
        ],
        # Seller
        "seller_ico": extract(estate, "_embedded.seller._embedded.premise.ico"),
        "seller_email": extract(estate, "_embedded.seller._embedded.premise.email"),
        "seller_numbers": [
            tel["code"] + tel["number"]
            for tel in extract(estate, "_embedded.seller._embedded.premise.phones", default=[])
        ],
        "seller_web": extract(estate, "_embedded.seller._embedded.premise.www"),
        "seller_address": extract(estate, "_embedded.seller._embedded.premise.address"),
        "seller_name": extract(estate, "_embedded.seller._embedded.premise.name")
    }

    # Model
    # Points of interest
    for poi in extract(estate, "poi", default=[]):
        estate_flattened[poi["name"]] = poi["distance"]
    # Items
    for item in extract(estate, "items", default=[]):
        if item['type'] == 'set':
            for val in item['value']:
                estate_flattened[val['name']] = val['value']
        else:
            estate_flattened[item["name"]] = item["value"]

    return estate_flattened


def extract(dictionary: Dict, keys: str, default: Any = None) -> Any:
    """Wrap around dictionary item, return None or empty list in case it does not exist
    # https://stackoverflow.com/questions/25833613/safe-method-to-get-value-of-nested-dictionary

    Args:
        dictionary: dictionary to parse
        keys: path to item, e.g. dict['level1']['level2'] => level1.level2
        default: what to return in case of None

    Returns:
        Any: Extracted

    """
    return reduce(
        lambda d, key: d.get(key, default)
        if isinstance(d, dict)
        else default, keys.split("."), dictionary
    )
