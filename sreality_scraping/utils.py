import os


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

