# -*- coding: utf-8 -*-
"""
Created on Sun Apr 29 2018

@author: nicko

PURPOSE AND USAGE:

Processes HSBC midata download from HSBC. Spend is categorised into "Fuel", "FFSM" and "Other" for use in monthly financial reviews.
    
KNOWN BUGS:
    
(1) There is heavy use of the old way of pandas DF slicing; hence multiple warnings;
See the caveats in the documentation: http://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
"""

"""
MODULES
"""

import pandas as pd

"""
RUNNING THE SCRIPT FROM HERE ONWARDS.
"""

try:
    hsbc_midata = pd.read_csv("midata5786.csv", encoding = "utf-8") #Try this encoding first, then try the below until something works.
except UnicodeDecodeError:
    hsbc_midata = pd.read_csv("midata5786.csv", encoding = "ISO-8859-1")

hsbc_midata.drop(hsbc_midata.tail(2).index, inplace=True) #Delete the last two rows, which are metadata that is not needed here.
hsbc_midata['Date'] = pd.to_datetime(hsbc_midata[' Date'], format = '%d/%m/%Y') #Create new column of pandas datetime type.
hsbc_midata.drop([' Date'], axis = 1, inplace = True) #Drop the old date column.

hsbc_midata['Debit/Credit'].replace('[\£,]', '', regex=True, inplace = True)
hsbc_midata['Debit/Credit'] = hsbc_midata['Debit/Credit'].astype(float)

"""
TIDYING THE DATASET TO TRANSACTIONS ONLY
"""

spendtypes = [')))','VIS','ATM'] #Only these types of transcations are monthly spend.
hsbc_midata_spend = hsbc_midata[hsbc_midata['Type'].isin(spendtypes)] #Keeping only the relevant transcations.
hsbc_midata_spend['Category'] = 'NOT YET DEFINED'


"""
APPLYING LABELS TO THE TRANSACTIONS
"""

#Manually identified "other" transactions.
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('INSURANCE', regex=False)] = 'Other' #This is case sensitive.
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('BANK', regex=False)] = 'Other' #This is case sensitive.
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('CREDIT', regex=False)] = 'Other' #This is case sensitive.
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('BARCLAYCARD', regex=False)] = 'Other' #This is case sensitive.
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('MICROSOFT', regex=False)] = 'Other' #This is case sensitive.
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('CENTRCARDIFF', regex=False)] = 'Other' #Servicing at City Service Centre
#hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('PREMIER INN', regex=False)] = 'Other' #Servicing at City Service Centre
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('TRAVELODGE', regex=False)] = 'Other' #Servicing at City Service Centre
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('LEGAL & GENERAL', regex=False)] = 'Other' #Servicing at City Service Centre
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('LEGAL & GENERAL', regex=False)] = 'Other' #Servicing at City Service Centre
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('ENTERPRISE', regex=False)] = 'Other' #Car rental, not a standard activity.
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('DVLA', regex=False)] = 'Other' #Car tax, not a standard activity.
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('PREMIER INN*******HATFIELD', regex=False)] = 'Other' #Car tax, not a standard activity.

#Fuel transactions categorised; again mostly manual.
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('PETROL', regex=False)] = 'Fuel' #This is case sensitive.
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('Petrol', regex=False)] = 'Fuel' #This is case sensitive.
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('Diesel', regex=False)] = 'Fuel' #This is case sensitive.
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('DIESEL', regex=False)] = 'Fuel' #This is case sensitive.
hsbc_midata_spend['Category'][(hsbc_midata_spend['Merchant/Description'].str.contains('SAINSBURYS', regex=False) == True) & (hsbc_midata_spend['Debit/Credit'] <= -60)] = 'Fuel'
hsbc_midata_spend['Category'][(hsbc_midata_spend['Merchant/Description'].str.contains('ASDA', regex=False) == True) & (hsbc_midata_spend['Debit/Credit'] <= -60)] = 'Fuel'
hsbc_midata_spend['Category'][(hsbc_midata_spend['Merchant/Description'].str.contains('MBNA', regex=False) == True) & (hsbc_midata_spend['Debit/Credit'] <= -60)] = 'Fuel'

#A line specifically to ensure that cash withdrawels are set to FFSM, can e.g. be picked up as fuel if taken out at a supermarket cashpoint.
hsbc_midata_spend['Category'][hsbc_midata_spend['Merchant/Description'].str.contains('CASH', regex=False)] = 'FFSM' #Car tax, not a standard activity.                 
#This line is for major cash withdrawels, that would not be FFSM.
hsbc_midata_spend['Category'][(hsbc_midata_spend['Merchant/Description'].str.contains('CASH', regex=False) == True) & (hsbc_midata_spend['Debit/Credit'] <= -50)] = 'Other'

                 
#Finally, set everything left to FFSM
hsbc_midata_spend['Category'][(hsbc_midata_spend['Category'] == 'NOT YET DEFINED')] = 'FFSM'

"""
SUMMARISE TRANSACTIONS
"""

#Add month and year data for easier grouping.
hsbc_midata_spend['Year'] = pd.DatetimeIndex(hsbc_midata_spend['Date']).year
hsbc_midata_spend['Month'] = pd.DatetimeIndex(hsbc_midata_spend['Date']).month

monthlyspend = pd.DataFrame(hsbc_midata_spend.groupby(['Category','Year','Month'])['Debit/Credit'].sum()) 

print(monthlyspend)





















