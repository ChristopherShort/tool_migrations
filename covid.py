'''
Functions for managing covid scenarios for given NOM output
'''

import pandas as pd



def add_nom(df):
    '''
    add nom for each visa group and a total nom (with arrivals, departures and non)
    
    Parameters:
    -----------
    df: a dataframe with multiindex columns of visa_group by (arrivals, departures)
    
    Returns
    -------:
    df: extended with nom for each visa_group plus total nom
    '''


    ## Create nom for each visa grouop
    nom_monthly = df.swaplevel(axis=1).arrivals - df.swaplevel(axis=1).departures
    nom_monthly.columns = pd.MultiIndex.from_product([nom_monthly.columns, ["nom"]])
    df = pd.concat([df, nom_monthly], axis=1).sort_index(axis=1)
    

    ## Create nom total
    nom_total_monthly = df.sum(axis=1, level=1)
    nom_total_monthly.columns = pd.MultiIndex.from_product([["nom"], nom_total_monthly.columns])

    return pd.concat([df, nom_total_monthly], axis=1)

    nom_scenario_monthly.loc[start:stop]