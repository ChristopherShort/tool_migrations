"""Utilities for manipulating the Jobs "Labour Market Internet Portal"
http:lmip.gov.au
"""

from pathlib import Path
import pandas as pd
import re
import requests
from requests_html import HTMLSession

import chris_utilities as cu
import file_paths

DATA_FOLDER_VACANCY = file_paths.internet_vacancy_folder


EXCEL_FILE_NAME = "IVI_DATA_regional - May 2010 onwards"

COL_ORDER = ["NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT", "Total"]

def is_file_type_downloadable(url):
    """
    Check if the content type is an excel file

    Do this by looking in the content_type of the header
    """

    h = requests.head(url, allow_redirects=True)
    header = h.headers
    content_type = header.get("content-type")

    if content_type.lower() in ["application/vnd.ms-excel", "application/x-zip", "octet-stream"]:
        return True
    return False


def download_vacancy_file():
    """Download "IVI_DATA_regional - May 2010 onwards.xlsx" from Employment's Labour Market 
    Information Portal
    
    #TODO Add error checking
    #TODO get/check excel file parameters at URL to confirm it is latest version 
            (ie - covers upto current month)
    #TODO fix returned "downloaded" & "failed" to proper returns as part of error checking

    Returns
    -------
    [type]
        [description]
    """
    url_lmip = "http://lmip.gov.au/default.aspx?LMIP/VacancyReport"
    url_regional_data = "http://lmip.gov.au/PortalFile.axd?FieldID=2790180&.xlsx"
    url_region_data_code = "2790180"
    
    session = HTMLSession()
    r = session.get(url_lmip)

    url_regional_data = [url for url in r.html.absolute_links if url_region_data_code in url]

    if len(url_regional_data) == 1:
        url_regional_data = url_regional_data[0]
    else:
        raise ValueError(f"The regional data file with {url_region_data_code} in it's link is not on this page")

    
    if is_file_type_downloadable(url_regional_data):
        xl_file = session.get(url_regional_data)
        with open(DATA_FOLDER_VACANCY / f"{EXCEL_FILE_NAME}.xlsx", "wb") as output:
            output.write(xl_file.content)
        return "downloaded"
    else:
        raise ValueError(f"File link {url_region_data_code} was not downloadable")


def make_vacancy_parquet(
    data_folder=DATA_FOLDER_VACANCY,
    fname="IVI_DATA_regional - May 2010 onwards.xlsx",
    sheetname="Averaged",
):
    def tidyup(df):
        df.columns = df.columns.rename("date")
        df = df.stack()
        df = df.rename("vacancies")
        df = cu.clean_column_names(df.reset_index())
        return df

    # download_vacancy_file()

    fpath = data_folder / fname
    df = (
        pd.read_excel(fpath, sheet_name=sheetname, index_col=[0, 1, 2, 3, 4])
        .pipe(tidyup)
        .assign(anzsco_code=lambda x: x.anzsco_code.astype(str))
        .assign(date=lambda x: x.date + pd.offsets.MonthEnd(0))
    )

    df.to_parquet(fpath.parent / f"{fpath.stem}.parquet")

    return df


def read_vacancy(
    data_folder=DATA_FOLDER_VACANCY,
    fname="IVI_DATA_regional - May 2010 onwards.parquet",
):

    return pd.read_parquet(data_folder / fname)


def QTB_para(df):
    """[summary]
    #TODO - break out as variables the "round(df.iloc[-1,8],0):,.0f" to improve clarity
    Parameters
    ----------
    df : [type]
        [description]
    
    Returns
    -------
    [type]
        [description]
    """
    return f"Internet Vacancy Index data shows that outside Sydney, Melbourne and Brisbane \
there were around {round(df.iloc[-1,8],0):,.0f} job vacancies in {df.index[-1].month_name()} {df.index[-1].year}. \
This is similar to the {round(df.iloc[-2,8],0):,.0f} vacancies a year earlier, and an increase of \
{(df.iloc[-1,8] / df.iloc[-3,8] - 1):.0%} per cent from around {round(df.iloc[-3,8],0):,.0f} two years ago. \
This includes around {round(df.iloc[-1,9],0):,.0f} job vacancies in the regions outside all State and Territory capitals.\n"


def regional_vacancies(
    vacancies=None,
    exclude_capitals=["Sydney", "Melbourne", "Brisbane"],
    total_only=False,
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

    total_excludes_string = "Total excludes " + ", ".join(exclude_capitals)

    states = (
        vacancies[idx_level & ~idx_region]
        .groupby(["date", "state",])
        .vacancies.sum()
        .unstack(["state",])
        .assign(Total=lambda x: x.sum(axis=1))
        # .rename(columns={"Total": total_excludes_string})
    )

    if total_only:
        return states.Total.rename(total_excludes_string)
    else:
        if "ACT" in states.columns:
            return states[COL_ORDER].rename(columns={"Total": total_excludes_string})
        else:
            col_order_ex_ACT = [
                state_name for state_name in COL_ORDER if state_name != "ACT"
            ]
            return states[col_order_ex_ACT].rename(
                columns={"Total": total_excludes_string}
            )


def regional_vacancies_exclude_mainland_state_capitals(
    vacancies=None,
    exclude_capitals=["Sydney", "Melbourne", "Brisbane", "Adelaide", "Perth"],
    total_only=True,
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

    states = regional_vacancies(vacancies, exclude_capitals, total_only)

    return states

    # return (regional_vacancies(vacancies, exclude_capitals)
    #             .drop(columns=["Total"])
    #             .sum(axis="columns")
    #             .rename("Total (excludes mainland state capitals)")
    # )


def regional_vacancies_exclude_all_capitals_(
    exclude_capitals=[
        "Sydney",
        "Melbourne",
        "Brisbane",
        "Adelaide",
        "Perth",
        "Hobart & Southeast Tasmania",
        "Darwin",
        "Canberra & ACT",
    ],
    total_only=True,
):
    return regional_vacancies(exclude_capitals, total_only)


def regional_vacancies_exclude_all_capitals(
    vacancies=None,
    exclude_capitals=[
        "Sydney",
        "Melbourne",
        "Brisbane",
        "Adelaide",
        "Perth",
        "Hobart & Southeast Tasmania",
        "Darwin",
        "Canberra & ACT",
    ],
    total_only=True,
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

    states = regional_vacancies(vacancies, exclude_capitals, total_only)

    return states


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

    df = pd.concat(
        [
            regional_vacancies(vacancies),
            regional_vacancies_exclude_all_capitals(vacancies, total_only=True),
            regional_vacancies_exclude_mainland_state_capitals(
                vacancies, total_only=True
            ),
        ],
        axis="columns",
    )

    # Return year values for month of last entry
    if month is None:
        idx = df.index.month == df.index.month[-1]
        print(QTB_para(df[idx]))
        return df[idx]
    elif isinstance(month, int):
        idx = df.index.month == month
        print(QTB_para(df[idx]))
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
        (vacancies.date == match_date)
        & (vacancies.anzsco_code.str.len() == 1)
        & (vacancies.anzsco_code != "0")
        & (~vacancies.region.isin(exclude_capitals))
    )

    def index_string_title(df):
        df.index = df.index.str.title()
        return df

    def add_column_totals(df):
        df.loc["Total", :] = df.sum(axis=0)
        return df

    df = (
        vacancies[idx]
        .groupby(["state", "anzsco_title"])
        .vacancies.sum()
        .unstack("state")
        .assign(Total=lambda x: x.sum(axis=1))
        .pipe(index_string_title)
        .pipe(add_column_totals)
        .round()
    )

    return df[COL_ORDER]


def odyseus_data():
    return QTB_vacancy_table(month="all")
