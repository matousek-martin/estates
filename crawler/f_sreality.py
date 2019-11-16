#class for calling sreality api

import requests
from bs4 import BeautifulSoup
import math
import tqdm
import json
import pandas as pd
import numpy as np
import datetime
import time


class sreality:
    
    def __init__(self):
        self.baselink = 'https://www.sreality.cz/api/cs/v1/estates'
        self.baselink_short = 'https://www.sreality.cz/api'
        self.resthreshold = 997 #threshold for results per iteration (forced by Seznam.cz)
        self.dictParsUrl = {#categories for url, human translation
                            'kraj'          : 'locality_region_id',
                            'okres'         : 'locality_district_id',
                            'podkategorie'  : 'category_sub_cb',
                            'kategorie'     : 'category_main_cb',
                            'kategorie_typ' : 'category_type_cb'
                            } 
        
        #load mapping dictionary
        with open("srealityParams.json", "r") as read_file:
            self.sreality_pars = json.load(read_file)
            
        #date of download
        td = datetime.date.today()
        self.downloaddate = td.strftime("%d/%m/%Y")

                    
        
    def runOne(self, params):
        '''
        method that runs one specific query based on provided parameters and returnsa json output
        '''
        
        #first call to see the number of existing records
        call = requests.get(self.baselink, params=params)
        
        
        #convert to json
        call_json = call.json()
        
        return call_json
    
    
    def getBaiscInfoAndDetailedUrls(self, nrecords=None):
        '''
        this method gets a list of detailed URLs and basic info about estates
        '''
        #basic parameters for the call
        param = {
            'category_main_cb': '1',
            'category_type_cb': '2',
            'locality_region_id': '10',
            'per_page': '50',
        }
        #sort from the newest to the oldest
        
        

        #first call to see the number of existing records
        call = requests.get(self.baselink, params=param)
        
        #get the number of records
        call_all_nrecords = self.nOfResults(call)
        
        #this is the number of iterations in our following loops
        numiter = math.ceil(call_all_nrecords/ int(param['per_page']))
        
        #if you wann see just a few results (debugging etc.) you can specify number of testing records
        if nrecords:
            numiter = nrecords
        
        self.masterdata = {}
        for i in tqdm.tqdm_notebook(range(0,numiter)):
            
            #limit to slightly more iterations than the threshold limit... it will show us whether seznam changed the limit or not...
            if i < self.resthreshold*1.3:
                param['page'] = i+1

                #get 60 results
                call = requests.get(self.baselink, params=param)    

                #get the body
                body_tmp = call.json()

                #check whether key elements of the json exist
                if body_tmp.get('_embedded', None) and body_tmp['_embedded'].get('estates', None):
                    body = body_tmp['_embedded']['estates']

                    #iterate through the body and get urls to details
                    for pg in body:
                        self.pg = pg
                        try:
                            #scrape info about one record
                            idone, onerecinfo = self.basicInfoFromOne(pg)

                            #add it to master dictionary
                            self.masterdata[idone] = onerecinfo

                        except:
                            print('you have reach maximul allowed number of pages or particular record could not be parsed.')
                else:
                    print(' in iteration number {}, embedded and estates parameters are missing. Skipping this row. If the iteration is less than {} you should not worry about it.'.format(i, self.resthreshold))

        print('detailed urls stored.')
        
    
    def addDetailedInfoToMaster(self):
        '''
        after initializing all detailed urls and basic info, one can download the detailed data and add it to the master dictionary
        '''
       
        #master data has to exists!!!
        if self.masterdata:
        
            #all detailed urls are in the master under inside every record. thus, we iterate through estate ID (keys of master dictionary)
            for idestate in tqdm.tqdm_notebook(self.masterdata.keys()):

                #get the url to detailed page
                url_detail_one_tmp = self.masterdata[idestate]['linkdetail']
                #construct the whole url
                url_detail_one = self.baselink_short + url_detail_one_tmp

                #scrape this URL
                detailed_result_one = self.detailedInfoFromOne(url_detail_one)

                #append it to master df
                self.masterdata[idestate].update(detailed_result_one)
            
            print('Data downloaded and added to the master.')
        else:
            print('Master data are not assigned to the class. You should consider running getBaiscInfoAndDetailedUrls()')
                
    
    
    ##########################        
    # methods below serve as intermediaries or as utilities functions
    ####################################
    def basicInfoFromOne(self, oneBasicRecord):
            
        x=oneBasicRecord

        #first get id of the record
        url_id_raw = x['_links']['self']['href']
        idHere = url_id_raw.partition('/cs/v1/estates/')[2]


        infoHighlevel = {}

        #aukce
        infoHighlevel['aukce'] = x['is_auction']
        infoHighlevel['cena_aukce'] = x['auctionPrice']

        #lokalita
        infoHighlevel['lokalita'] = x['locality']

        #cena
        infoHighlevel['cena'] = x['price']
        infoHighlevel['cena_czk_value'] = x['price_czk']['value_raw']
        infoHighlevel['cena_czk_unit'] = x['price_czk']['unit']
        infoHighlevel['cena_czk_name'] = x['price_czk']['name']

        #additional info
        infoHighlevel['name'] = x['name']

        #geo
        infoHighlevel['lat'] = x['gps']['lat']
        infoHighlevel['lon'] = x['gps']['lon']

        #id seznam
        infoHighlevel['hash'] = x['hash_id']
        

        #generate URL from frontend of sreality
        base = 'https://www.sreality.cz/detail/'
        #has to be converted to text with dict
        typ  = self.sreality_pars['kategorie_typ'][str(x['type'])]
        main = self.sreality_pars['kategorie'][str(x['seo']['category_main_cb'])]
        sub =  x['seo']['category_sub_cb']
        local_url = x['seo']['locality']
        url_id = x['_links']['self']['href']

        #link to sreality web
        infoHighlevel['urlSreality'] = self.buildUrl(x)

        #link to details of the record
        infoHighlevel['linkdetail'] = x['_links']['self']['href']
        
        #download date
        infoHighlevel['download_date'] =  self.downloaddate

        return (idHere, infoHighlevel)
    
    
    def detailedInfoFromOne(self, url_detail_one):
        '''
        a method that downloads selected detailed data from provided detailed url
        '''
        #create flag of last detailed processed - for debugging
        self.lastDetailProcessed = url_detail_one       
        
        ###call and get data
        #basic parameters for the call
        param = {'per_page':'1', #records per page
                 'sort'    :'0'}  #sort from the newest to the oldest

        #call to get detailed data
        call = requests.get(url_detail_one, params=param)

        jsondata = call.json()
        
        
        
        mapDictionaries = self.dictParsUrl

        #initialize a dataframe with detaiuled info
        desc = {}
        desc["prague_district"] = jsondata["locality_district_id"]
        #description
        desc['description'] = jsondata.get('meta_description', None)

        #high level name
        if jsondata.get('name',None):
            desc['name'] = jsondata['name'].get('value',None)
        else:
            desc['name'] = None
                                                
        #geometry data that will be used futher in GIS
        desc['geometry'] = jsondata.get('map', None)

        #lokalita
        if jsondata.get('locality',None):
            desc['locality_typ'] = jsondata['locality'].get('name',None)
            desc['locality_value'] = jsondata['locality'].get('value', None)
        else:
            desc['locality_typ'] = None
            desc['locality_value'] = None            

        #text description (for further text analytics)
        desc['text'] = jsondata.get('text', None)

        #price info
        if jsondata.get('price_czk',None):
            desc['price_whole'] = jsondata['price_czk'].get('value_raw', None)
            desc['price_freq'] = jsondata['price_czk'].get('unit', None)
            
            if jsondata['price_czk'].get('alt',None):
                desc['price_recalculated'] = jsondata['price_czk']['alt'].get('value_raw', None)
                desc['price_recalculated_perunit'] = jsondata['price_czk']['alt'].get('unit', None)
            else:
                desc['price_recalculated'] = None
                desc['price_recalculated_perunit'] = None  
        else:
            desc['price_whole'] = None
            desc['price_freq'] = None            
            desc['price_recalculated'] = None
            desc['price_recalculated_perunit'] = None
        
        #categorical types
        if jsondata.get('seo',None):
            desc['typ_inzeratu'] =  self.sreality_pars['kategorie_typ'][str(jsondata['seo']['category_type_cb'])]
            desc['kategorie'] = self.sreality_pars['kategorie'][str(jsondata['seo']['category_main_cb'])]
            desc['podkategorie'] = self.sreality_pars['podkategorie'][str(jsondata['seo']['category_sub_cb'])]
        else:
            desc['typ_inzeratu'] =  None
            desc['kategorie'] = None
            desc['podkategorie'] = None           

        #additional info about seller
        if jsondata.get('_embedded',None):
            if jsondata['_embedded'].get('seller',None):
                desc['s_userid'] = jsondata['_embedded']['seller']['user_id']

                if jsondata['_embedded']['seller'].get('phones',None):
                    desc['s_mobile'] = jsondata['_embedded']['seller']['phones'][0]['number']    
                else:
                    desc['s_mobile'] = None
                desc['s_active'] = jsondata['_embedded']['seller'].get('active', None)

                if jsondata['_embedded']['seller'].get('_embedded', None):
                    if jsondata['_embedded']['seller']['_embedded'].get('premise', None):
                        desc['s_web']    = jsondata['_embedded']['seller']['_embedded']['premise']['www']
                        desc['s_name'] = jsondata['_embedded']['seller']['_embedded']['premise']['name']
                        desc['s_email'] = jsondata['_embedded']['seller']['_embedded']['premise']['email']
        else:
            desc['s_userid'] = None
            desc['s_mobile'] = None
            desc['s_active'] = None
            desc['s_web']    = None
            desc['s_name']   = None
            desc['s_email']  = None
            
        #link to image of the estate
        try:
            desc['image'] = jsondata['_embedded']['images'][0]['_links']['dynamicDown']['href']
        except:
            desc['image'] = None
            
        items = jsondata["items"]
        
        for item in items:
            if item["name"] == "Stavba":
                try:
                    desc["building"] = item["value"]
                except:
                    desc["building"] = np.nan
            if item["name"] == "Stav objektu":
                try:
                    desc["property_state"] = item["value"]
                except:
                    desc["property_state"] = np.nan
            if item["name"] == "Vlastnictví":
                try:
                    desc["ownership"] = item["value"]
                except:
                    desc["ownership"] = np.nan
            if item["name"] == "Podlaží":
                try:
                    desc["floor"] = item["value"]
                except:
                    desc["floor"] = np.nan
            if item["name"] == "Užitná plocha":
                try:
                    desc["area"] = item["value"]
                except:
                    desc["area"] = np.nan
            if item["name"] == "Balkón":
                try:
                    desc["balcony_area"] = item["value"]
                except:
                    desc["balcony_area"] = np.nan
                try:
                    desc["balcony_unit"] = item["unit"]
                except:
                    desc["balcony_unit"] = np.nan
            if item["name"] == "Sklep":
                try:
                    desc["basement"] = item["value"]
                except:
                    desc["basement"] = np.nan
            if item["name"] == "Garáž":
                try:
                    desc["garage"] = item["value"]
                except:
                    desc["garage"] = np.nan
            if item["name"] == "Výtah":
                try:
                    desc["elavator"] = item["value"]
                except:
                    desc["elavator"] = np.nan
            if item["name"] == "Výtah":
                try:
                    desc["elavator"] = item["value"]
                except:
                    desc["elavator"] = np.nan
            if item["name"] == "Energetická náročnost budovy":
                try:
                    desc["efficiency"] = item["value"]
                except:
                    desc["efficiency"] = np.nan
            

        return desc
        
    
    def buildUrl(self,rec):
        '''
        build a sreality url from a json returned from the api
        '''
        base = 'https://www.sreality.cz/detail/'
        typ  = self.sreality_pars['kategorie_typ'][str(rec['type'])]
        main = self.sreality_pars['kategorie'][str(rec['seo']['category_main_cb'])]
        sub = self.sreality_pars['podkategorie'][str(rec['seo']['category_sub_cb'])]
        locality = rec['seo']['locality']

        url_id_raw = rec['_links']['self']['href']
        url_id = url_id_raw.partition('/cs/v1/estates/')[2]
    
        #join the eleents
        url_all = base + '/'.join([typ,main,sub,locality,url_id])


        return url_all
        
            
    def nOfResults(self, req):
        '''
        A method that returns number of existing records per given call. Expects requests.get() output
        '''
        #convert to json
        reqjson = req.json()
        
        # number of results
        nresult = reqjson['result_size']
        
        return nresult
    
    def createOutputDF(self):
        '''
        a wrapper for pandas data frame. Takes masterdata dictionary and converts it to a dataframe
        '''
        
        self.master_pandas = pd.DataFrame.from_dict(self.masterdata,orient='index').reset_index(level=0)
        
    def writeToCsv(self):
        '''
        a wrapper for storing final pandas dataframe into csv of the current directory
        '''
        
        #filename
        filename = 'sreality_output_{}'.format(self.downloaddate.replace('/','')) + '.csv'
        
        self.master_pandas.to_csv(filename, index=False)
      
    def auto(self, store=False, nofrecords=None):
        '''
        A simple wrapper that calls all functions given number of records as a parameter and optionally stores obtained data as csv
        '''
        #get basic info
        self.getBaiscInfoAndDetailedUrls(nrecords=nofrecords)
        
        #get detailed info
        self.addDetailedInfoToMaster()
        
        #create outpudf
        self.createOutputDF()
        
        #store it as a csv
        if store==True:
            self.writeToCsv()
            print("Data stored to local.")