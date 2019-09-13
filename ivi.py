"""Utilities for manipulating the Jobs "Labour Market Internet Portal"
http:lmip.gov.au
"""

from pathlib import Path
import pandas as pd
from requests_html import HTMLSession

import chris_utilities as cu

DATA_FOLDER_VACANCY = Path.home() / "Documents/Analysis/Australian economy/Data/internet_vacancy"


def download_vacancy_file():
    session = HTMLSession()
    r = session.get("http://lmip.gov.au/default.aspx?LMIP/VacancyReport")

    if "http://lmip.gov.au/PortalFile.axd?FieldID=2790180&.xlsx" in r.html.absolute_links:
        return True
    else:
        return False

def make_vacancy_parquet(
    data_folder=DATA_FOLDER_VACANCY,
    fname="IVI_DATA_regional - May 2010 onwards.xlsx",
    sheetname = "Averaged",
    ):

    def tidyup(df):
        df.columns = df.columns.rename("date")
        df = df.stack()
        df = df.rename("vacancies")
        df = cu.clean_column_names(df.reset_index())
        return df

    fpath = data_folder/ fname
    df= (pd
              .read_excel(fpath, 
                      sheet_name=sheetname,
                      index_col=[0,1,2,3,4]
                     )
              .pipe(tidyup)
              .assign(anzsco_code=lambda x: x.anzsco_code.astype(str))
              .assign(date = lambda x:x.date + pd.offsets.MonthEnd(0))
           )

    df.to_parquet(fpath.parent / f"{fpath.stem}.parquet")

    return df


def read_vacancy(
    data_folder=DATA_FOLDER_VACANCY,
    fname="IVI_DATA_regional - May 2010 onwards.parquet",
    ):

    return pd.read_parquet(data_folder / fname)


def regional_vacancies(
    vacancies=None,
    exclude_capitals=["Sydney", "Melbourne", "Brisbane"],
    ):
    """Vacancy dataframe with with states by dates
    
    Parameters
    ----------
    vacancy : df
        tidy dataframe of vacancy datata (read_vacancy)
    
    exclude_capitals : list
        capital cities to exclude to make a "regional vacancy index"

    Returns
    ------- 
    states: df
        States aggregates of regional vacancy data.
        That is, all the regional vacancy data excluding those in Sydney, Melbourne, Brisbane
    
    The "level" column takes values of 1, 2 or 3.
    1 = Total for Region
    2 = Total for Region by 1 digit ANZCO
    3 = Total for Region by 2 digit ANZCO
    """
    if vacancies is None:
            vacancies = read_vacancy()
    
    idx_level = vacancies.level == 1
    idx_region = vacancies.region.isin(exclude_capitals)

    states = (vacancies[idx_level & ~idx_region]
                .groupby(["date", "state", ])
                .vacancies
                .sum()
                .unstack(["state", ])
                .assign(Total = lambda x:x.sum(axis=1))
            )

    col_order = ["NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT", "Total"]

    return states[col_order]


def regional_vacancies_exclude_mainland_state_capitals(
    vacancies = None,
    exclude_capitals=["Sydney", "Melbourne", "Brisbane", "Adelaide", "Perth"],
    ):
    """[summary]
    
    Parameters
    ----------
    vacancies : [type], optional
        [description], by default None
    exclude_capitals : list, optional
        [description], by default ["Sydney", "Melbourne", "Brisbane", "Adelaide", "Perth"]
    
    Returns
    -------
    [type]
        [description]
    """
    if vacancies is None:
            vacancies = read_vacancy()

    return (regional_vacancies(vacancies, exclude_capitals)
                .drop(columns=["Total"])
                .sum(axis="columns")
                .rename("Total (excludes mainland state capitals)")
    )


def QTB_vacancy_table(vacancies=None, month=None):
    """Make the QTB table containing Regional and a total exlucding mainlain state capitals
    
    Parameters
    ----------
    vacancies : pandas dataframe, optional
        A tidy versions of the regional internet vacancy data, by default None
    month : int, optional
        A month number, 1-12, by default None which indicates use last month in the dataframe   
    
    Returns
    -------
    df : pandas dataframe
        The QTB as a dataframe
    """
    if vacancies is None:
        vacancies = read_vacancy()

    df= (pd
            .concat(
                [regional_vacancies(vacancies),
                regional_vacancies_exclude_mainland_state_capitals(vacancies),
                ],
                axis="columns"
            )
        )
    
    # Return year values for month of last entry
    if month is None:
        idx = df.index.month == df.index.month[-1]
        return df[idx]
    elif isinstance(month, int):
        idx = df.index.month == month
        return df[idx]
    else:
        return df

    

def one_digit_anzsco(
    vacancies=None,
    match_date=None,
    exclude_capitals=["Sydney", "Melbourne", "Brisbane"],
    ):
    """Return vacancy totals for 1 digint ANZSCO
    
    
    Parameters
    ----------
    vacancies : pandas dataframe, optional
        vacancies dataframe by state, by default None
    match_date : date, optional
        the month to extract the summary, by default None
    
    Returns
    -------
    pandas dataframe
        [description]
    """
    if vacancies is None:
        vacancies = read_vacancy()

    if match_date is None:
        match_date = vacancies.date.sort_values().iloc[-1]

    print(f"Summary data for {match_date:%B %Y}\n")

    idx = (
            (vacancies.date == match_date) & 
            (vacancies.anzsco_code.str.len() == 1) & 
            (vacancies.anzsco_code != "0") & 
            (~vacancies.region.isin(exclude_capitals))
        )

    
    def index_string_title(df):
        df.index = df.index.str.title()
        return df
    
    def add_column_totals(df):
        df.loc['Total', :] = df.sum(axis=0)
        return df

    df = (vacancies[idx]
            .groupby(["state", "anzsco_title"])
            .vacancies
            .sum()
            .unstack("state")
            .assign(Total=lambda x: x.sum(axis=1))
            .pipe(index_string_title)
            .pipe(add_column_totals)
            .round()
    )

    return df


