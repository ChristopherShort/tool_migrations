"""Utilities to manage OECD data from the World Bank

"""

from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from IPython.core.display import HTML, display_html

from pandas_datareader import wb
from chris_utilities import cagr
from scipy.stats.mstats import gmean



def get_wb_indicator(indicator, countries, start=1960, end=2018):
    """[summary]
    
    Parameters
    ----------
    indicator : [type]
        [description]
    countries : [type]
        [description]
    start : int, optional
        [description], by default 1960
    end : int, optional
        [description], by default 2018
    
    Returns
    -------
    [type]
        [description]
    """
    def index_to_date(df):
        df.index = pd.to_datetime(df.index.astype(str) + "-12-31")
        return df
                     
    
    df = (wb
          .download(indicator=indicator, 
                 country=countries, # .iso3c.values, 
                 start=start, end=end)
          .squeeze()
          .unstack("country")
          .pipe(index_to_date)
    )
    return df


def get_wb_countries():
    countries = (wb.get_countries()
        .set_index("name")
    )

    #remove country groupings
    idx = countries.lendingType != "Aggregates"
    return countries[idx]


def get_wb_population():
    #TODO finish this from Tudge test deep dive workbook
    return


def get_oecd_members():
    oecd_members = {
        "Australia": "7-Jun-1971",
        "Austria": "29-Sep-1961",
        "Belgium": "13-Sep-1961",
        "Canada": "10-Apr-1961",
        "Chile": "7-May-2010",
        "Czech Republic": "21-Dec-1995",
        "Denmark": "30-May-1961",
        "Estonia": "9-Dec-2010",
        "Finland": "28-Jan-1969",
        "France": "7-Aug-1961",
        "Germany": "27-Sep-1961",
        "Greece": "27-Sep-1961",
        "Hungary": "7-May-1996",
        "Iceland": "5-Jun-1961",
        "Ireland": "17-Aug-1961",
        "Israel": "7-Sep-2010",
        "Italy": "29-Mar-1962",
        "Japan": "28-Apr-1964",
        "Korea, Rep.": "12-Dec-1996",
        "Latvia": "1-Jul-2016",
        "Luxembourg": "7-Dec-1961",
        "Mexico": "18-May-1994",
        "Netherlands": "13-Nov-1961",
        "New Zealand": "29-May-1973",
        "Norway": "4-Jul-1961",
        "Poland": "22-Nov-1996",
        "Portugal": "4-Aug-1961",
        "Slovak Republic": "14-Dec-2000",
        "Slovenia": "21-Jul-2010",
        "Spain": "3-Aug-1961",
        "Sweden": "28-Sep-1961",
        "Switzerland": "28-Sep-1961",
        "Turkey": "2-Aug-1961",
        "United Kingdom": "2-May-1961",
        "United States": "12-Apr-1961",
    }

    return pd.Series(oecd_members, dtype="datetime64[ns]").sort_values()