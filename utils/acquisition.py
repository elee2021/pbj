import pandas as pd
from pathlib import Path
import glob
import numpy as np
from datetime import date
from datetime import datetime
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib as mpl

# database
import sqlalchemy as db
engine = db.create_engine('postgresql+psycopg2://postgres:1111@localhost/postgres')
connection = engine.connect()
metadata = db.MetaData()
occupancy = db.Table('occ', metadata, autoload=True, autoload_with=engine)

    
def generate_occ_table(df):
        occupancy_table = pd.pivot_table(df, values=['occupancy'], index=['provnum'], columns=['year', 'month'])
        occupancy_table['provnum'] = occupancy_table.index
        occupancy_table = occupancy_table.reset_index(drop=True)

        return occupancy_table

def get_acquired_dates_df():
     # ac_dates_df: dataframe with acquisiton dates
        ac_dates_df = pd.read_csv('all/acquisition_dates.csv')
        ac_dates_df['provnum'] = ac_dates_df['provnum'].astype('int')
        ac_dates_df['provnum'] = ac_dates_df['provnum'].astype(str)
        ac_dates_df['provnum'] = ac_dates_df['provnum'].apply(lambda x: str(x).zfill(6))
        
        return ac_dates_df
    
def get_merged_table():
        
        ac_dates_df = get_acquired_dates_df()
        

        # occ_df: dataframe with occupancy
        occ_query=db.select([occupancy]).where(occupancy.columns.ensign == True)
        df = pd.read_sql_query(occ_query, con=engine)
        df['date'] = pd.to_datetime(df[['year', 'month']].assign(DAY=1)) # combine year and month columns and assign day=1
        df['cna_ctr_percentage'] = df['hrs_cna_ctr']/df['hrs_cna']
        df_features = ['provnum', 'provname', 'state', 'county_name', 'city', 'date', 'year', 'month' ,
                       'occupancy', 'cna_ctr_percentage']
        occ_df = df[df_features]

        # merge_df: dataframe with occupancy and acquisition dates for each provider
        merge_df = pd.merge(
                            occ_df,
                            ac_dates_df,
                            how = 'left',
                            left_on = 'provnum',
                            right_on = 'provnum',
                            validate = 'many_to_one')

        merge_df = merge_df[merge_df['acquired_year']>=2017]
        merge_df = merge_df[merge_df['acquired_year']<=2018]
        merge_df['provnum'].nunique() # 65 unique providers
        merge_df['acquired_year'].unique() # 2017(12), 2018(7), 2019(26), 2020(6), 2021(14)
        merge_df['state'].unique() # 'AZ', 'CA', 'CO', 'ID', 'IA', 'KS', 'TX', 'UT', 'WA'
        new_ensign_providers = merge_df['provnum'].unique().tolist()
        new_ensign_providers  # provider numbers acquired after 2017

        new_prov_dict = {'provnum':[], 'provname':[], 'state':[], 'county':[], 'acquisitiondate':[]}

        # generate dataframe that map provnums to provnames
        for p in new_ensign_providers:

            query=db.select([occupancy]).where(db.and_(occupancy.columns.ensign == True,
                                                  occupancy.columns.provnum ==p))
            df_prov = pd.read_sql_query(query, con=engine)
            provname = df_prov['provname'].str.split(';\s*')[0][0]
            state = df_prov['state'].str.split(';\s*')[0][0]
            county = df_prov['county_name'].str.split(';\s*')[0][0]
            ac_date = ac_dates_df[ac_dates_df['provnum']==p]['acquisitiondate'].values[0]
            new_prov_dict['provnum'].append(p)
            new_prov_dict['provname'].append(provname)
            new_prov_dict['state'].append(state)
            new_prov_dict['county'].append(county)
            new_prov_dict['acquisitiondate'].append(ac_date)

        new_prov_df = pd.DataFrame(data=new_prov_dict)


        # only get the rows after acquisition date
        occ_after_acquisition_df = merge_df[merge_df['date'] >= merge_df['acquisitiondate']]

        # occupancy_table
        acquisition_occupancy_table = generate_occ_table(occ_after_acquisition_df)
        
        states_with_new_providers = new_prov_df['state'].unique().tolist()
        states_with_new_providers  # list of states with facilities acquired after 2017

        state_facility_dict = {}  # state - facilities dictionary

        for state in states_with_new_providers:
            state_facility_dict[state] = new_prov_df[new_prov_df['state']==state]['provname'].values.tolist()

        states_prov = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in state_facility_dict.items() ]))
        states_prov = states_prov.fillna(0) # facilities acquired after 2017
        states_prov

        return acquisition_occupancy_table, new_prov_df, states_with_new_providers, state_facility_dict

