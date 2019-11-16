# GeocodeCZ-and-Sreality-crawler

### This repo consist of two parts. First, it contains classes for geocoding an address and second, there is a sreality crawler with some basic visuaisation.


## Geocode
This set of classes serve for geocoding an address -  you have a string of address and your goal is to obtain its coordinates.

You will find a Source_geocode_address.ipynb jupyter which serves for testing and developing. Then there is a f_geocode_address.py which contains final functions that can be called by any software supporting python script and finally, there is a Run_geocode_address.ipynb script which is an example of how to use the function.

In addition to this, there is also a parsing script (organized in an identical manner - Source_parse_adress.ipynb, f_parse_adress.py, Run_parse_address.ipynb) that serves to parsing an address into its components (i.e. street, street number, city, zip code).

## Sreality
This script downloads and parse data from sreality API. The structure is identical to previous case. Source_sreality.ipynb script that serves as dev environment, f_sreality.py - python script that contains final version of code and Run_sreality.ipynb that is an example of how to run the function.

Furthermore, you can find Sreality_analysis_of_data.ipynb which is a descriptive analytics of data obtained through sreality crawler.
