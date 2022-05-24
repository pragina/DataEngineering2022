from itertools import count
import pandas as pd
import numpy as np
import re


# ACS DATA
def load_county():
    dfcounty = pd.read_csv('acs2017_census_tract_data.csv', usecols=['TractId','State','County','TotalPop',
                                                                'Poverty','IncomePerCap'])
    dfcounty.rename(columns = ({'TractId': 'ID', 'County':'Name', 'TotalPop': 'Population',
                        'IncomePerCap':'PerCapitaIncome'}), inplace=True)
    dfcounty = dfcounty.dropna(axis=0)
    return dfcounty


# function to clean/remove word "County" from county = Name field 
def clean_county_name(county):
    temp = []
    for key, value in county['Name'].items():
        if "County" in value:
            temp_new = re.sub(" County", "", value)
        else:
            temp_new = value
        temp.append(temp_new)
    county['Name'] = temp
    return county


def distinct_statecounty(county):
    county['CountyAndState'] = county['Name']+', '+county['State']
    county= county['CountyAndState'].unique()
    return county

def county_list(county_name):
    c_list = []
    i = 1

    for values in county_name:
        c_list.append(values)
        i+=1
    return c_list


# create "unique id" for each county in county df
def create_countyid(county, county_name, county_list1):
    ID = []
    Poverty_value = []
    cnt = 1
    for i, row in county.iterrows():
        if row.Name+', '+row.State in county_name:
            ID.append(county_list1.index(row.Name+', '+row.State)+1)
            Poverty_value.append((row.Poverty/100)*row.Population)

    county['ID'] = ID
    county['Poverty_val'] = Poverty_value

    return county


def updated_county(county):

    dfcounty_info = county.groupby(['Name','State','ID'])
    new_info = dfcounty_info.sum()
    new_info = new_info.reset_index()
    new_info['Poverty_percent'] = (new_info['Poverty_val'] / new_info['Population']) * 100

    return new_info

def print_acsdata(transformed_county):
    new_info = transformed_county
    print(new_info[(new_info['Name'] == 'Loudoun') & (new_info['State'] == 'Virginia')])
    print(new_info[(new_info['Name'] == 'Washington') & (new_info['State'] == 'Oregon')])
    print(new_info[(new_info['Name'] == 'Harlan') & (new_info['State'] == 'Kentucky')])
    print(new_info[(new_info['Name'] == 'Malheur') & (new_info['State'] == 'Oregon')])
    print(new_info[new_info['Population'] == new_info['Population'].max()])
    print(new_info[new_info['Population'] == new_info['Population'].min()])


def countyinfo(transformed_countyinfo):
    dfcounty_transformed = transformed_countyinfo[["ID","Name","State","Population","PerCapitaIncome","Poverty_val"]]
    return dfcounty_transformed


# COVID DATA
def load_coviddata():
    dfcovid = pd.read_csv('COVID_county_data.csv')
    dfcovid['Month'] = pd.DatetimeIndex(dfcovid['date']).month
    dfcovid['Year'] = pd.DatetimeIndex(dfcovid['date']).year
    dfcovid.rename(columns=({'county':'County','state':'State','cases':'Cases', 'deaths':'Deaths'}), inplace=True)
    del dfcovid['fips']
    dfcovid = dfcovid.dropna()

    return dfcovid


def create_covidid(coviddata, county_list1):
    ID = []
    cnt = 1
    for i, row in coviddata.iterrows():
        if row.County+', '+row.State in county_list1:
            ID.append(county_list1.index(row.County+', '+row.State)+1)
        else: 
            ID.append(9999)

    coviddata['ID'] = ID

    return coviddata


def transform_cdata(covid_info):
    dfcovid_info = covid_info.groupby(['Month','Year','County','State','ID'])
    new_dfcovid = dfcovid_info.sum()
    new_dfcovid = new_dfcovid.reset_index()

    print(new_dfcovid[(new_dfcovid['County'] == 'Malheur')
                & (new_dfcovid['State'] == 'Oregon')
                & (new_dfcovid['Month'] == 2) 
                & (new_dfcovid['Year'] == 2021)])

    print(new_dfcovid[(new_dfcovid['County'] == 'Malheur')
                & (new_dfcovid['State'] == 'Oregon')
                & (new_dfcovid['Month'] == 8) 
                & (new_dfcovid['Year'] == 2020)])

    print(new_dfcovid[(new_dfcovid['County'] == 'Malheur')
                & (new_dfcovid['State'] == 'Oregon')
                & (new_dfcovid['Month'] == 1) 
                & (new_dfcovid['Year'] == 2021)]) 
    

def summary_coviddata(covid_info):
    covid_summary = covid_info.groupby(['ID']).agg({'Cases':sum, 'Deaths':sum})
    covid_summary = covid_summary.reset_index()
    return covid_summary



def integrate_covid_acs(final_countyinfo, agg_cdata):
    dfcovid_summary = pd.merge(final_countyinfo, agg_cdata, on = ["ID"])
    dfcovid_summary['PopulationPer100K'] = dfcovid_summary['Population']/100000
    dfcovid_summary['TotalCasesPer100K'] = dfcovid_summary['Cases']/dfcovid_summary['PopulationPer100K']
    dfcovid_summary['TotalDeathsPer100K'] = dfcovid_summary['Deaths']/dfcovid_summary['PopulationPer100K']
    
    dfcovid_summary['Poverty_percent'] = (dfcovid_summary['Poverty_val'] / dfcovid_summary['Population']) * 100

    return dfcovid_summary
    
def print_covidsummary(covid_summary):
    print(covid_summary[(covid_summary['Name']=='Washington') 
                & (covid_summary['State']=='Oregon')])
    print(covid_summary[(covid_summary['Name']=='Malheur') & (covid_summary['State']=='Oregon')])
    print(covid_summary[(covid_summary['Name']=='Loudoun') & (covid_summary['State']=='Virginia')])
    print(covid_summary[(covid_summary['Name']=='Harlan') & (covid_summary['State']=='Kentucky')])



print('=======A. ACS Data ======================')
county = load_county()
county = clean_county_name(county)
county_name = distinct_statecounty(county)
county_list1 = county_list(county_name)
county_info = create_countyid(county, county_name, county_list1)
transformed_county = updated_county(county_info)
print_acsdata(transformed_county)

print('=======B. COVID Data======================')

coviddata = load_coviddata()
covid_info = create_covidid(coviddata, county_list1)
transform_cdata(covid_info)
summary_cdata = summary_coviddata(covid_info)


print('========C. COVID Summary======================')
final_countyinfo = countyinfo(transformed_county)
covid_summary = integrate_covid_acs(final_countyinfo,summary_cdata)
print_covidsummary(covid_summary)


print('=========D. Analysis R value===================')
R = covid_summary.groupby('State')[['TotalCasesPer100K','Poverty_percent']].corr().unstack().iloc[:,1]
print('For Oregon Counties only: correlation between % poverty and COVID cases: \n',R.Oregon)
print()
R = covid_summary['TotalCasesPer100K'].corr(covid_summary['Population'])
print('For all counties: correlation between population and COVID cases: \n',R)
print()
R = covid_summary.groupby('State')[['TotalDeathsPer100K','PerCapitaIncome']].corr().unstack().iloc[:,1]
print('For Oregon counties only: correlation between PerCapitaIncome and COVID deaths: \n',R.Oregon)
print()
R = covid_summary['TotalCasesPer100K'].corr(covid_summary['PerCapitaIncome'])
print('For all USA counties: correlation between PerCapitaIncome and COVID cases : \n',R)
print()
R = covid_summary['TotalCasesPer100K'].corr(covid_summary['TotalDeathsPer100K'])
print('For all USA counties: correlation between COVID deaths and COVID cases: \n',R)
print()