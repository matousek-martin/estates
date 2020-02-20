import os
import pickle
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


def new_estate(self, url_suffix):
    """Check if estate has already been scraped or not by comparing scraped id to the ones in the estate folder.
        
        Arguments:
            url_suffix {string} -- url is provided as '/cs/v2/estates/1077595740'
        
        Returns:
            bool -- True if estate_id isn't already scraped (there's no json with this id in estates folder)
        """
    # read already scraped IDs
    folder = "estates/"
    files = os.listdir(folder)
    old_ids = [file.split(".")[0] for file in files]
    # extract newly scraped ID from url
    new_id = url_suffix.split("/")[-1]
    # if new_id is not in old_ids, it is a new estate that hasn't been scraped
    return not new_id in old_ids


def update_cache(scraped_estates: list) -> None:
    """Updates cache.txt with newly scraped estate IDs. This was used during the initial filling of NoSQL to avoid too many reads per day.
    
    Args:
        scraped_estates (list): updated list of estate IDs
    """
    with open("cache.txt", "wb") as fp:  # Pickling
        pickle.dump(scraped_estates, fp)
    return None


def load_cache() -> None:
    """Loads cache.txt. This was used during the initial filling of NoSQL to avoid too many reads per day.
    
    Returns:
        list: loaded cache file, list containing estate IDs
    """
    with open("cache.txt", "rb") as fp:  # Unpickling
        cache = pickle.load(fp)
    return cache


def connect_firestore(path: str):
    """Creates a Google Cloud Firestore instance.
    
    Args:
        path (str): path to json credentials
    
    Returns:
        class: Google Cloud Firestore instance
    """
    # Using a service account
    cred = credentials.Certificate(path)
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    return db
