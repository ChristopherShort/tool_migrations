'''
Utilities for examining grant data for pathways and survival
'''

import pickle
from pathlib import Path
import pandas as pd
import matplotlib as mpl
from matplotlib import pyplot as plt
import matplotlib.dates as mdates

from chris_utilities import adjust_chart, set_fin_year_axis
import seaborn as sns
import nom_forecast as nomf


def set_path_3412(fpath=None):
    '''
    Set a path object to the ABS 3412 dictionary

    Returns
    -------
    fpath: Path object
    '''

    # TODO think about whether the object passed in should be everything
    # exept Path.hom()

    if fpath is None:
        fpath = Path.home() / 'Documents' / 'Analysis' / 'Australian economy' / \
            'Data' / 'Dictionaries' / 'ABS - Visacode3412mapping.xlsx'
    return fpath


def read_all_grant_data(file_path='RFI22541_EXTRACT_02_FILE01.parquet', 
                        columns=['TR_PERSON_ID', 
                                'TR_VISA_SUBCLASS_CD', 
                                'TR_VISA_GRANT_DT', 
                                'TR_VISA_IN_EFFECT_UNTIL_DT']):

    '''
    Read in the grant data from a previously created parquet file

    Parameters:
    ----------
    filepath: Path object to parquet file
    columns: fields to keep

    Returns:
    -------
    data frame of data
    '''

    # TODO genearlise the file path
    df = (pd.read_parquet(file_path,columns=columns)
            .drop_duplicates(subset=['TR_PERSON_ID', 'TR_VISA_SUBCLASS_CD', 'TR_VISA_GRANT_DT'])
            .sort_values(['TR_PERSON_ID', 'TR_VISA_GRANT_DT'])
          )

    return df


def get_visa_group(df, vsc, start_date=pd.Timestamp(2017, 6, 30), end_date=pd.Timestamp(2018, 7, 1)):
    '''
    '''

    if not isinstance(vsc, list):
        raise ValueError('Chris - vsc is expected to be a list visa subclasses')

    return (df
            .where(lambda x: x.TR_VISA_SUBCLASS_CD.isin(vsc))
            # .where(lambda x: (x.TR_VISA_GRANT_DT > pd.Timestamp(2017,6,30)) & (x.TR_VISA_GRANT_DT < pd.Timestamp(2018,7,1)))
            .dropna(how='all')
            .drop_duplicates(subset=['TR_PERSON_ID'])
            )


def get_visa_codes(file_path):
    '''
    Return a dataframe with ABS visa groupings (in cat no. 3412) by subclass
    See ABS Migration unit for updated copies of excel file

    Parameters:
    -----------
    file_path: Path or str object
        filepath to ABS excel file

    Returns:
    -------
    dataframe
    '''

    visa_codes = (pd.read_excel(file_path, sheet_name='codes')
                    .rename(columns=str.lower)
                    .rename(columns=lambda x: x.replace(' ', '_'))
                  # make sure visa subclass code is a string
                    .assign(code=lambda x: x['code'].astype(str))
                    .assign(visa_subclass_code=lambda x: x.visa_subclass_code.astype(str))
                    .set_index('visa_subclass_code')
                  )

    visa_codes_mapper = visa_codes.code.to_dict()

    return visa_codes, visa_codes_mapper


def get_pathway_summaries(df_, pathways_):
    '''
    A generator to loop over pathways to provide summary of time
    '''

    for pathway in pathways_:
        idx = df_.pathway_adjusted == pathway

        yield (df_.loc[idx, 'delta'].describe()).rename(str(pathway))


