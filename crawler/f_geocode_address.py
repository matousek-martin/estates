#set of functions needed to geocode a dataframe of address
import requests 
from bs4 import BeautifulSoup
from IPython.core.debugger import Tracer
import ast
import pandas as pd
import numpy as np
import tqdm


#load api key for ggoogle api
import apicka as api

API = api.api

class geoRUIAN:
    '''
    a class for geocode using RUIAN's API
    '''
    def __init__(self):
        #define the API's link
        self.ruianlink = 'http://ags.cuzk.cz/arcgis/rest/services/RUIAN/Vyhledavaci_sluzba_nad_daty_RUIAN/MapServer/exts/GeocodeSOE/findAddressCandidates'
        
    def testConnection(self):
        '''
        a method that tests connection to the API
        '''
        try:
            _ = requests.get(self.ruianlink)
            print('Connection was succesfull.')
            return True
        except requests.ConnectionError:
            print("No internet connection available or another issues.")
        return False
    
    def sendRequest(self,adresaString):
        '''
        
        '''
        self.adresaString = adresaString
        pars = {'SingleLine':adresaString,
               'outSR':      '',
               'maxLocations': 100,
               'searchExtent': '',
               'f':'html'}
        
        self.r = requests.get(self.ruianlink, params=pars)
        self.r.encoding = 'utf8'
        
        
        self.soup = BeautifulSoup(self.r.text,'lxml')
         #pridat blbuvzdornost

    def parseOutput(self):
        
        #locate the table
        output_raw = self.soup.findAll('table','formTable')
        
        #there are two formTables on the page. the second one contains the resuls
        results_raw = output_raw[1]
        
        #our element is 'pre'
        allResults = results_raw.find('pre')
        
        #convert the unicode format into something human readible. i.e. a dictionary
        allResultsString = ast.literal_eval(allResults.text)
        
        #slice only 'candidates' key of the dictionary. The output is a list.
        if allResultsString.get('candidates'):
            self.candidatesAll = allResultsString['candidates']
        else:
            self.candidatesAll = False
        
    
    def testAddressOutput(self):
        '''
        The geocoding service might not be able to return the searched address GPS. This method tests whether the API detected the address correctly.
        '''
        #get all 'Type' of the dictionary
        if self.candidatesAll: 
            #detect whether at least one of the candidates is 'AdresniMisto'
            self.types = [d['attributes']['Type'] for d in self.candidatesAll]
            if 'AdresniMisto' in self.types:
                self.AdresniMistoFound = True
                return True
            else:
                self.AdresniMistoFound = False
                self.somethingFound    = True
        else:
            self.AdresniMistoFound = False
            self.somethingFound    = False
            self.notFoundAtAll = True

    
    def selectAddressCandidate(self, method='First', printOutput=True):
        '''
        In some cases RUIAN might return multiple outputs. This method selects one of them.
        method muze byt first nebo centroid
        '''
        if self.AdresniMistoFound:
                #subset only AdresniMista
                AdresniMistoFlag = [ d for d in self.candidatesAll if d['attributes']['Type']=='AdresniMisto' ]
                
                self.adresniMistaAll = AdresniMistoFlag
                
                #if htere is only one candidate, we are done. Otherwise sort candidates according to score...
                #in case multiple candidates have the same score, create an average of GPS and create a flag
                if len(self.adresniMistaAll) == 1:
                    matchedAddress = self.adresniMistaAll[0]['address']
                    xMean     = np.mean([self.adresniMistaAll[0]['attributes']['Xmax'],
                                        self.adresniMistaAll[0]['attributes']['Xmin']])
                    
                    yMean     = np.mean([self.adresniMistaAll[0]['attributes']['Ymax'],
                                        self.adresniMistaAll[0]['attributes']['Ymin']])
                    
                    #we create a flag to indicate how certain we are about the returned result
                    flagConfidence = 1
                    
                
                elif method == 'First':
                    #using this method we select the first offered adresniMisto by RUIAN
                    matchedAddress = self.adresniMistaAll[0]['address']
                    xMean     = np.mean([self.adresniMistaAll[0]['attributes']['Xmax'],
                                        self.adresniMistaAll[0]['attributes']['Xmin']])
                    
                    yMean     = np.mean([self.adresniMistaAll[0]['attributes']['Ymax'],
                                        self.adresniMistaAll[0]['attributes']['Ymin']])
                    
                    #we create a flag to indicate how certain we are about the returned result
                    flagConfidence = 2


                elif method == 'Centroid':
                    #using this method as GOS coordinates we return an average of the given addresses
                    matchedAddress = self.adresniMistaAll[0]['address']
                    xMean     = np.mean([d['attributes']['Xmax'] for d in self.adresniMistaAll] +
                                        [d['attributes']['Xmin'] for d in self.adresniMistaAll])
                    
                    yMean     = np.mean([d['attributes']['Ymax'] for d in self.adresniMistaAll] +
                                        [d['attributes']['Ymin'] for d in self.adresniMistaAll])
                    
                    #we create a flag to indicate how certain we are about the returned result
                    flagConfidence = 3
                    #list of output variables
                    self.geocodedAddress = {'originalAddress':self.adresaString, 
                                            'matchedAddress':matchedAddress,
                                            'lon': xMean,
                                            'lat': yMean, 
                                            'confidenceFlag': flagConfidence}
                    
                else:
                    print('method is not defined!')
        
        #create flag if something was return by RUIAN but it is not an address point
        elif self.somethingFound:
            matchedAddress = ''
            xMean = ''
            yMean = ''
            flagConfidence = 4          
        
        else:
            matchedAddress = ''
            xMean = ''
            yMean = ''
            flagConfidence = 5
            #print('address not found.')

        #list of output variables
        self.geocodedAddress = {'originalAddress':self.adresaString, 
                                'matchedAddress':matchedAddress,
                                'lon': xMean,
                                'lat': yMean, 
                                'confidenceFlag': flagConfidence}            
            
        #optionally print the output
        if printOutput:
            return self.geocodedAddress
        
        #explanation of flagConfidence
        # 1 - string matched as address, only one result from RUIAN
        # 2 - string matched as address, multiple results from RUIAN
        # 3 - not define
        # 4 - something was found, but it was not defined as address
        # 5 - nothing found at all
    
    
    
    def auto(self, add, method='First',printOutput=True):
        '''
        Simple wrapper for calling all steps in one. if you trust the function, you should use it.
        '''
        self.sendRequest(add)
        self.parseOutput()
        self.testAddressOutput()
        return self.selectAddressCandidate()
        

        
        
#############################################################
# google api
#############################################################
class geoGoogle:
    '''
    a class for geocode using google's API
    Expects a string of an address upon initialization
    '''
    
    def __init__(self, API):
        self.apiLinkGen  = 'https://maps.googleapis.com/maps/api/geocode/json?address={}'
        self.apikey      = API
        self.apiLink     = 'https://maps.googleapis.com/maps/api/geocode/json?address={}' + '&key={}'.format(API)
        
        self.testAddress = 'opletalova 26, Nove Mesto'
        
    def testConnection(self):
        '''
        a method that serves to test the response from the API
        '''
        self.testLink = self.apiLink.format(self.testAddress)
        self.req = requests.get(self.testLink)
        
        #test wthether success
        if self.req.status_code == 200:
            self.status=True
            #print('Connection successfull')

        else:
            self.status=False
            print('Something happened. Code:' + str(self.req.status_code))
        
        return self.status
    
    
    def geocodeOne(self, address):
        '''
        a method that serves to geocode a single address
        '''
        #check whether connection was made
        stat = self.testConnection()
        if stat == True:
        
            #call the api
            getRes = self.apiLink.format(address)

            #pull data
            addReq = requests.get(getRes)

            #convert to json if somtehing returned
            self.apiOutput = addReq.json()

            if len(addReq.json()['results']) > 0:
                self.addResult = self.apiOutput['results'][0]
                nOfRes = 999
            else:
                nOfRes = 0

            #test whether some result was returned or not
            if nOfRes ==0:
                output = {
                    "confidenceFlag": 5,
                    "source_address" : address,
                    "formatted_address" : None,
                    "latitude": None,
                    "longitude": None
                    #"google_place_id": None,
                }
            else:
                #GPS coordinates
                lat = self.addResult['geometry']['location']['lat']
                lng = self.addResult['geometry']['location']['lng']

                #formatted address
                formAddress = self.addResult['formatted_address']

                #type of location identified
                typ = self.addResult['types']
                #accuracy
                acc = self.addResult['geometry']['location_type']

                #google place ID
                placeid = self.addResult['place_id']


                #explanation of flagConfidence
                # 1 - string matched as address, only one result from RUIAN
                # 2 - string matched, multiple results from Google, a centroid is returned
                # 3 - not define
                # 4 - something was found, but it was not defined as address
                # 5 - nothing found at all
                tp = self.addResult['geometry']['location_type']
                if tp == 'ROOFTOP':
                    conf = 1
                elif tp == 'RANGE_INTERPOLATED':
                    conf = 2
                elif tp == 'GEOMETRIC_CENTER':
                    conf = 4
                elif tp == 'APPROXIMATE':
                    conf = 5
                else:
                    conf='error'

                #assign determinated values to output list
                output={
                    "confidenceFlag" : conf,
                    "latitude": lat,
                    "longitude": lng,
                    "matchedAddress" : formAddress,
                    "originalAddress" : address,

                    "accuracy": acc,
                    "google_place_id": placeid,
                    "type": typ
                }

                #print crying emoji
                print('right now you have spent 0.01USD :(')

            return output
        
        else:
            print("A confirmation of connection is not provided. Try to run method testconnection()")

            
##################################################################################
## join method
class geoJoin:
    
    def __init__(self):
        #self.srctable_path = "/Users/vojtechnedved/Downloads/adresyAll.csv"
        self.srctable_path = "adresyAllMerge.csv"
        self.srccols = ['obec','castobce','ulice','cp','co', 'psc']
        
        #load all addresses and convert them to lowercase
        src = pd.read_csv(self.srctable_path).convert_objects(convert_numeric=True)
        self.srctable = src.applymap(lambda s:s.lower() if type(s) == str else s)
        
        #remap joining columns
        remap = {
            'Název obce' : 'obec',
            'Název části obce' : 'castobce', 
            'Název ulice': 'ulice',
            'Číslo domovní': 'cp',
            'Číslo orientační' : 'co',
            'PSČ': 'psc',
            'Souřadnice Y': 'lat',
            'Souřadnice X': 'lng'
        }
        #self.srctable =  self.srctable.rename(columns=remap, index=str)
        
        
    def validColumns(self, addr):
        '''
        a support method for finding non-null columns of the address. Is used inside joinOne() method. Required a pandas dataframe.
        '''
        
        #get index of non-null columns
        ind = addr.notnull()

        ind = ind.values[0]
        
        #subset those columns
        adSelected = addr.iloc[0, ind]
        
        #subset columns presented in both dataframes
        adSelected = [ x for x in adSelected.index.values.tolist() if x in self.srccols]
        
        self.colsMerge =  adSelected
        
        
    def joinOne(self, addresa):
        '''
        This method expects a list of one adress containing several attributes. Moreover, you should specify merging columns.
        '''
        
        #we expect a pandas series as input... then we convert it to pandas dataframe (for joining purposes; not elegant but working)
        #if we are provided with one row dataframe, no transofrmation is needed
        
        #pandas DF conversion
        self.addresaPD = pd.DataFrame(addresa).transpose()
        
        #get on what columns we will perform the join
        self.validColumns(self.addresaPD)
        
        #force conversion to numbers of the address
        self.addresaPD_beforeConversion = self.addresaPD.convert_objects(convert_numeric=True)
        
        #change joining columns of the input to lowercase
        self.addresaPD = self.addresaPD.applymap(lambda s:s.lower() if type(s) == str else s)
        
        #join the master address table wiht the current record
        self.joined = pd.merge(self.srctable, self.addresaPD ,on=self.colsMerge, how='inner')
     
        #check if unique result is returned. If not, input None... we have to geocode it using some of the APIs
        if self.joined.shape[0] > 1:
            self.lat = None
            self.lng = None
            #print('MANYYY')
        #maybe we did not match anything at all
        elif self.joined.shape[0] == 0:
            self.lat = None
            self.lng = None
        #or we have aexactly one result
        else:
            #get longitude and latitude
            self.lat = self.joined['lat'][0]
            self.lng = self.joined['lng'][0]       
            #print(self.lat, self.lng)
        return (self.lat, self.lng)


##########################################################################################
## string generator for full address
##########################################################################################

def generateAddressString(pandasSer):
    '''
    Givne a pandas series object containing at least any of ['obec', 'castobce', 'ulice', 'cp', 'co', 'psc'] it returns a string of the address
    '''
    #join cislo popisne and orientacni
    cisla = (str(potentialData['cp']) if potentialData['cp'] else "") + ('/' + str(potentialData['co']) if potentialData['co'] else "")
    
    #join the rest
    addString = (str(potentialData['ulice']) if potentialData['ulice'] else "") +\
        (' '  + str(cisla) if cisla else "") +\
        (', ' + str(potentialData['castobce']) if potentialData['castobce'] else "") +\
        (', ' + str(potentialData['obec']) if potentialData['obec'] else "") +\
        (', ' + str(potentialData['psc']) if potentialData['psc'] else "")
    
    return addString


############################################################################################################
## wrapper for geocoding
############################################################################################################
class geoWrapper:
    '''
    this class serves as a wrapper for all geocode approaches. You have to pass a dataframe containing your address into the function with proper column names. 
    '''
    
    def __init__(self, dataFrame, API_KEY_Google=None):
        #initialize geo classes
        self.ruian = geoRUIAN()
        self.join  = geoJoin()
        
        #check if google Key was provided
        if API_KEY_Google is not None:
            self.ggl = geoGoogle(API_KEY_Google)
            self.gglAPI = True
        else:
            self.gglAPI = False
            
        #store data
        self.dataFrame = dataFrame
        
    def runJoin(self):
        '''
        applies joining method for geocode
        '''
        #iterate over the file and assign results
        for i,j in tqdm.tqdm_notebook(self.dataFrame.iterrows()):
            #geocode current row
            # print(j)
            out = self.join.joinOne(j)
            
            lat = out[0] or np.nan
            lng = out[1] or np.nan
            
            #print(fileToScore)
            #assign the value to df
            self.dataFrame.loc[i,'latitude'] = lat 
            self.dataFrame.loc[i,'longitude'] = lng
            self.dataFrame.loc[i,'methodUsed'] = 'join' if np.isfinite(lat) else -1
            self.dataFrame.loc[i,'confidence'] = 1      if np.isfinite(lat) else -1
            
    def runRuian(self):
        '''
        A wrapper for calling  RUIANs API
        '''
        
        #iterate over the file and assign results
        for i,j in tqdm.tqdm_notebook(self.dataFrame.iterrows()):

            #check whether it was not already scored by joinign method
            if j['methodUsed'] is None or j['methodUsed'] == np.nan or j['methodUsed'] == -1:
                #prepare data for call; if data are presented in separated way we have to merge them, Otherwise, use initially provided string
                self.j=j
                potentialData =  j[self.join.srccols]

                
                #generate address String
                if j['fulladdress']:
                    addString = j['fulladdress']
                    
                else:
                    addString = generateAddressString(potentialData)

                #call RUIAN
                res = self.ruian.auto(addString)

                #assign the value to df
                self.dataFrame.loc[i,'latitude'] = res['lat']
                self.dataFrame.loc[i,'longitude'] = res['lon']
                self.dataFrame.loc[i,'methodUsed'] = 'ruian'
                self.dataFrame.loc[i,'confidence'] = res['confidenceFlag']

                
                
                
    def runGoogle(self):
        '''
        A wrapper for calling Geocode class using google's API
        '''
        #as this service is paid, count non-matched records and estimate price
        nMiss = max(self.dataFrame['latitude'].isna().sum(),self.dataFrame['longitude'].isna().sum())
        estPrice = nMiss*0.1
        
        #raise a dialog box whether you want to proceed or not
        txt = input("Estimated price of the query is {} USD. Do you wanna proceed? yes/no".format(estPrice))
        
        
        if self.gglAPI and txt == 'yes':
        #iterate over the file and assign results
            for i,j in self.dataFrame.iterrows():

                #check whether it was not already scored by joinign method
                if j['methodUsed'] is None or j['methodUsed'] == np.nan or j['methodUsed'] == -1:
                    #prepare data for call; if data are presented in separated way we have to merge them, Otherwise, use initially provided string
                    self.j=j
                    potentialData =  j[self.join.srccols]
                    
                    
                    #generate address String
                    if j['fulladdress']:
                        addString = j['fulladdress']
                        
                    else:
                        addString = generateAddressString(potentialData)

                    #call ggl
                    res = self.ggl.geocodeOne(addString)

                    #assign the value to df
                    self.dataFrame.loc[i,'latitude'] = res['latitude']
                    self.dataFrame.loc[i,'longitude'] = res['longitude']
                    self.dataFrame.loc[i,'methodUsed'] = 'google'
                    self.dataFrame.loc[i,'confidence'] = res['confidenceFlag']
        elif txt != 'yes':
            print('you refused to pay. Function stops.')
        else:
            print('google API not provided!')
        
    def auto(self,ggl=False):
        '''
        A wrapper that runs joining and RUIAN method. optionally also google api. 
        '''
        # joining
        self.runJoin()
        
        #ruian
        self.runRuian()
        
        #google
        if ggl == True:
            self.runGoogle()

