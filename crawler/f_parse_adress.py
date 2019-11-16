#this set of functions serve for parsing an address into its elements

import re
import os
import pandas as pd
import numpy as np

def contains_digit(s):
    '''
    Simple help function for testing presence of a number in the string
    '''
    isdigit = str.isdigit
    return any(map(isdigit,s))


class parseAddressDataFrame():
    '''
    a class that takes a dataframe containing a column of addresses and returns the same df enriched by specific parts of the address (street, number, city, part of the city, zip)
    '''
    
    def __init__(self, dataframe):
        self.dataframe = dataframe
    
    def parseOne(self, adresa):
        '''
        This method servers as a parser for address. Input is a string containing an address that should be unique in the Czech Republic. i.e. a specific address of a house
        Output is a dictionary containing parsed elements. It works well for standard address format, however, it might struggle to parse uncommon formats.
        '''
        adr = {}

        #just to be sure of correct input format
        adresa = str(adresa)
        
        #basic splits by comma and space and other characters
        commaSplit = re.split(",",adresa)
        barSplit_ini = [re.split("-", q) for q in commaSplit]
        barSplit = sum(barSplit_ini, [])
        #slashSplit_ini = [re.split("/", q) for q in barSplit]
        #slashSplit = sum(slashSplit_ini, [])
        spaceSplit_ini = [q.split() for q in barSplit]
        spaceSplit     = sum(spaceSplit_ini, [])


        #locate first (resp second) element which is a number (or begins with) - we expect the first element to be an string + number which is a street and its number
        i = 1
        while i <= len(spaceSplit):
            if spaceSplit[i][0].isdigit():
                    locCP = i
                    break
            i = i+1

        #solve streer (supposed to be at the beginning)
        street = " ".join(spaceSplit[:locCP])
        adr['ulice'] = street

        #cut the list with info
        splitCut1 = spaceSplit[locCP:]

        ################################################
        ### This chunk determines street and its number
        #expecting cislo popisne or/and orientacni
        if os.sep in splitCut1[0]:
            spl = splitCut1[0].split(os.sep)
            cp = spl[0]
            co = spl[1]
            #drop the parsed cislo 
            splitCut2 = splitCut1[1:]
        elif splitCut1[0].isdigit() & splitCut1[1][0].isalpha():
            cp = splitCut1[0]
            co = "NotFound"
            #drop the parsed cislo 
            splitCut2 = splitCut1[1:]
        elif splitCut1[0].isdigit():
            cp = splitCut1[0]
            co = "NotFound"
            #drop the parsed cislo 
            splitCut2 = splitCut1[1:]       
        elif splitCut1[0][0].isalpha():
            cp = "ExpNum"
            co = "ExpNum"
            #drop the parsed cislo 
            splitCut2 = splitCut1
            failToParse = True
        else:
            cp = "UnknownError"
            co = "UnknowError"
            splitCut2 = splitCut1
            failToParse = True

        adr['cp'] = cp
        adr['co'] = co

        ####################################################    
        ### guess psc - at the end of the list
        #sometimes remaining liest will be short (only city... ) in that case, it is probably only the city
        if( len(splitCut2) < 2):
            #psc is probably not presented
            if splitCut2[0].isdigit():
                psc   = splitCut2[0]
            else:
                psc   = 'NotFound'
            splitCut3 = splitCut2
        #switch to normal decision where psc, city and/or ctvrt are presented
        else:
            if (len(splitCut2[-1]) == 5) & splitCut2[-1].isdigit():
                psc = splitCut2[-1]
                #drop from the list psc
                splitCut3 = splitCut2[:(len(splitCut2)-1)]
            elif (len(splitCut2[-1]) + len(splitCut2[-2]) == 5) & splitCut2[-1].isdigit() & splitCut2[-2].isdigit():
                psc = splitCut2[-2]  + splitCut2[-1]
                splitCut3 = splitCut2[:(len(splitCut2)-2)]
            elif (len(splitCut2[0]) == 5) & splitCut2[0].isdigit():
                psc = splitCut2[0]
                splitCut3 = splitCut2[1:]
            elif (len(splitCut2[0]) + len(splitCut2[1]) == 5) & splitCut2[0].isdigit() & splitCut2[1].isdigit():
                psc = splitCut2[0]  + splitCut2[1]
                splitCut3 = splitCut2[2:]
            else:
                psc = "NotFound"
                splitCut3 = splitCut2

        adr['psc'] = psc

        ##################################################
        ### guess city and part of the city
        # the rest of the list should contain city and/or part of city
        i = 0

        while i < len(splitCut3):
            if splitCut3[i][0].isdigit():
                    locNum = i
                    break
            else:
                locNum = 0
            i = i+1

        if (len(splitCut3)-1) > locNum :
            l = locNum+1
            mesto1 = " ".join(splitCut3[:l])
            mesto2 = " ".join(splitCut3[l:])
        elif (len(splitCut3)-1) == locNum:
            mesto1 = " ".join(splitCut3[:-2])
            mesto2 = " ".join(splitCut3[-2:])
        else:
            mesto1 = splitCut3
            mesto2 = splitCut3
            failToParse = True
        adr['obec'] = mesto1 if mesto1 != "" else "NotFound"
        adr['castobce'] = mesto2 if mesto2 != "" else "NotFound"


        #print("Input:", adresa)

        return adr
    
    def processDF(self):
        '''
        this function serves as an iterator over rows of the dataframe and add the parsed parts to the df
        '''
        self.parsed = {}
        
        #iterate over the file and assign results
        for i,j in self.dataframe.itertuples():
            #apply the parsing function
            parsedone = self.parseOne(j)
            
            #add it to results dictionary
            self.parsed[i] = parsedone
            
    def addprocessed(self):
        '''
        this method adds separated address elements to the original dataframe and creates one output file
        '''
        #processed as pandas
        dfpandas = pd.DataFrame.from_dict(self.parsed, orient='index')
        
        #replace NotFound by nan
        dfpandas.replace("NotFound",np.nan, inplace=True)
        
        #replace not found by None        
        self.outputdf =  pd.concat([self.dataframe, dfpandas], axis=1, sort=False)