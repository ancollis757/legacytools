# -*- coding: utf-8 -*-
"""
Created on Sun Sep 23 09:33:02 2018

@author: nicko
"""

import os
from datetime import date as dt
import pandas as pd

def stringtodate(datelist_strings, bank): #Takes a list of dates in a certain string format and outputs a list of datetime.date objects.

    daylist_applied = []
    monthlist_applied = []
    yearlist_applied = []
    
    if bank == 'Clyde':
    
        i = 0
        for i in range(len(datelist_strings)):
            
            daylist_applied.append(datelist_strings[i][:2])
            monthlist_applied.append(datelist_strings[i][3:5])
            yearlist_applied.append(datelist_strings[i][6:10])
            
            i = i + 1
    
        datelist_dates = [] #A list of datetime.date objects.
        
        i = 0
        for i in range(len(daylist_applied)):
            
            #Put days in the required format by removing the leading zero.
            day = int(daylist_applied[i].lstrip('0'))
            
            month = int(monthlist_applied[i])
            year = int(yearlist_applied[i])
            
            date = dt(year,month,day)  
    
            datelist_dates.append(date)
            
            i = i + 1
       
    elif bank == 'HSBC':
        
        i = 0
        for i in range(len(datelist_strings)):
            
            daylist_applied.append(datelist_strings[i][:2])
            monthlist_applied.append(datelist_strings[i][3:5])
            yearlist_applied.append(datelist_strings[i][6:10])
            
            i = i + 1
    
        datelist_dates = [] #A list of datetime.date objects.
        
        i = 0
        for i in range(len(daylist_applied)):
            
            #Put days in the required format by removing the leading zero.
            day = int(daylist_applied[i].lstrip('0'))
            
            month = int(monthlist_applied[i])
            year = int(yearlist_applied[i])
            
            date = dt(year,month,day)  
    
            datelist_dates.append(date)
            
            i = i + 1
      
    else:
        
        print ("Bank name not recognised")
        
    return datelist_dates

def monthindexer(monthabre):

    return{
            'Jan' : 1,
            'Feb' : 2,
            'Mar' : 3,
            'Apr' : 4,
            'May' : 5,
            'Jun' : 6,
            'Jul' : 7,
            'Aug' : 8,
            'Sep' : 9, 
            'Oct' : 10,
            'Nov' : 11,
            'Dec' : 12
    }[monthabre]
    
class clyde_data:
                                                   
    def __init__(self, clyderaw):
       
       self.clyderaw = clyderaw

    def run_clyde(self):
        
        #PROCESS CLYDE
        
        clyderaw = self.clyderaw
        
        clyderawcollist = ['Date of Transaction STRING','Amount','Balance','Currency','Description']
        clyderaw.columns = clyderawcollist
        datelistClyde = stringtodate(clyderaw['Date of Transaction STRING'], 'Clyde')
        clyderaw['Date of Transaction'] = pd.Series(datelistClyde, index=clyderaw.index)
        clydeprocessed = clyderaw
        clydeprocessed.drop(['Balance', 'Currency','Date of Transaction STRING'], axis=1, inplace = True)
        self.clydeprocessed = clydeprocessed
        #Set all amounnts to be numeric.
        cols = ['Amount']
        self.clydeprocessed[cols] = self.clydeprocessed[cols].apply(pd.to_numeric, errors='coerce')
        #Applying the sign convention:
        #Debits are positive.
        #Credits are set to 0.
        self.clydeprocessed.loc[self.clydeprocessed['Amount'] > 0, 'Amount'] = 0 #Setting all credits to 0.
        self.clydeprocessed['Amount'] = -1 * self.clydeprocessed['Amount'] #Setting all credits to 0.
        
        self.clydeprocessed.reset_index(inplace = True) #Index messed up for some reason; maybe I've since changed this.
        self.clydeprocessed.drop(['index'], axis = 1, inplace = True)
        self.clydeprocessed.sort_values(by=['Date of Transaction'], inplace = True)
        self.clydeprocessed['Date of Transaction'] = pd.to_datetime(self.clydeprocessed['Date of Transaction'])

    
#Firstly checking whether there are any new files to process.
rawdata_tracker = pd.read_csv('SR_rawdata_tracker.csv')

#WHAT ABOUT HISTORIC TRANSACTIONS AND WHAT IF THERE ARE NO NEW RAW DATA FILES?
#WHAT ABOUT RE-WRITING THE RAW DATA TRACKER?

clyde_toprocess = rawdata_tracker.loc[(rawdata_tracker['Provider'] == 'Clyde') & (rawdata_tracker['Processed'] == False)]

if len(clyde_toprocess) > 0:

    #Creating a DF from the first DF to process, then appending to it in the list.
    firstclyde = clyde_toprocess['Filename'].iloc[0]
    clyderaw = pd.read_csv(firstclyde)
    
    i = 1 #Starting at the 2nd file (which in python is i = 1).
    
    while i < len(clyde_toprocess):
    
       clyde_filename = clyde_toprocess['Filename'].iloc[i] 
       raw_toappend = pd.read_csv(clyde_filename)
       
       #Adding the additional DF to the original.
       frames = [clyderaw, raw_toappend]
       clyderaw = pd.concat(frames) #Re-writing clyderaw; can I do this or are DFs immutable?
    
       i = i + 1
    
    clyde = clyde_data(clyderaw) #Naming terminologies here are repetitive and confusing; need to re-do at some point.
    clyde.run_clyde()
    
else:
    
    print ('There were no new Clyde files to process.')
    
clyde.clydeprocessed.to_csv('Clyde_LIVE.csv')


















