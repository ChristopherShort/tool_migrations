"""
A set of utilites to extract and manipulate ABS data

Use 'ABS' instead of 'abs' to avoid conflict with built in abs
"""
import re
from pathlib import Path
import time
from urllib.parse import urlparse  # , parse_qs
from requests_html import HTMLSession, requests
from IPython.display import HTML, display  # , clear_output
import pandas as pd
import numpy as np
from pandasdmx import Request


# Absolute paths
DATA_FOLDER = Path.home() / "Documents/Analysis/Australian economy/Data/ABS/"
DICT_FOLDER = Path.home() / "Documents/Analysis/Australian economy/Data/Dictionaries/"
DATA_FOLDER_AUDIT = Path.home() / "Documents/Analysis/Australian economy/Data/ABS/ABS data audit"


def get_downloads_page_url(url, allow_redirects=True):
    """
    Get the url for the Downloads/Details Page

    Parameters
    ----------
    url: str
      The canonical landing page for the latest version of a release.
      eg labour force: 'http://www.abs.gov.au/ausstats/abs@.nsf/mf/6202.0'

    Returns
    -------
    The url for the download page of the latest release
    """

    session = HTMLSession()
    r = session.get(url, allow_redirects=allow_redirects)

    url_details_page = [url for url in r.html.absolute_links if "DetailsPage" in url]

    if len(url_details_page) > 1:
        print(url_details_page)
        raise ValueError("Chris: More than 1 link found for Downloads/DetailsPage")

    return url_details_page[0]


def download_file(first_url, data_folder=DATA_FOLDER):
    file_params = file_details(first_url)
    file_name = file_params["filename"]
    session = HTMLSession()

    if is_file_type_downloadable(first_url):
        r = session.get(first_url)

        with open(data_folder / file_name, "wb") as output:
            output.write(r.content)

    else:
        print("No excel or zip file contained in the url:", first_url)


def is_file_type_downloadable(url):
    """
    Check if the content type is an excel file

    Do this by looking in the content_type of the header
    """

    h = requests.head(url, allow_redirects=True)
    header = h.headers
    content_type = header.get("content-type")

    if content_type.lower() in ["application/vnd.ms-excel", "application/x-zip"]:
        return True
    return False


def is_excel_file(url):
    """
    Check if the content type is an excel file

    Do this by looking in the content_type of the header
    """
    session = HTMLSession()

    h = session.head(url, allow_redirects=True)
    header = h.headers
    content_type = header.get("content-type")

    if "ms-excel" in content_type.lower():
        return True
    return False


def file_details(url_table):
    file_details_labels = [
        "agent",
        "filename",
        "catalog_no",
        "data_type",
        "unknow_param_0",
        "unknow_param_1",
        "data_date",
        "release_date",
        "version",
    ]
    file_details_parse = urlparse(url_table).query.split("&")

    file_params = dict(zip(file_details_labels, file_details_parse))

    return file_params


def get_file_name_from_url(url):
    m = re.search(r"log\\?openagent&(\d+\\.xls)", url)

    if m is None:
        raise ValueError("Chris: filename not found in url")
    else:
        return m.group(1)


def make_true_hiearchical(df, index_names=None):
    """
    ABS often places a hierarchical index in one column
    Create pandas hiearchical by joining a duplicated index that has full names of the
    level 0 hiearchy joined with original index
    and removing superfluous rows
    """

    # use copy to ensure df_hist index not interfered with
    outer = pd.Series(df.index.values).copy()

    # level 0 identified by rows that are all blank
    # that rows that are all np.nan
    # need to remove index values not assocciated with those rows

    # find all blank rows
    idx = df.isnull().all(axis=1).values

    # in outer - set the value to be null
    outer.loc[~idx] = np.nan

    # then forward fill all null values
    outer = outer.ffill()

    # create hierarchical index by joining outer and original index
    df.index = pd.MultiIndex.from_arrays([outer.values, df.index.values])

    # remove any rows with all nan -
    # these will be the duplicate values in the hierarchical index
    idx = df.isnull().all(axis=1)

    if index_names is not None:
        df.index.names = index_names

    return df[~idx]


def gen_ABS_3412(file_path, calendar=None):
    """
    A generator to read in ABS Migration Australia data

    Parameters:
    file_path: str or file path object

    calendar: boolean
        True for calendar year data, False for financial year data[]

    """

    if not isinstance(calendar, bool):
        raise ValueError("Chris: boolean for calendar or financial year not set")

    sheets_dict = pd.read_excel(file_path, sheet_name=None)

    for sheet_name, df_ in sheets_dict.items():

        # Ignore contents and other non-data pages
        if "Table" not in sheet_name:
            continue

        # Table descriptor in 4th row of worksheet - 3rd row of dataframe

        table = re.match(r"Table_* * \d\.\d+", df_.iloc[3, 0]).group()

        if calendar:
            year_string = re.search(r"\d\d\d\d", df_.iloc[3, 0]).group()
            year_int = int(year_string)
        else:
            year_string = re.search(r"\d\d\d\d-\d\d", df_.iloc[3, 0]).group()
            year_int = int("20" + year_string[-2:])

        print(year_string, table)

        df_.columns = [
            "state",
            "major_groupings",
            "minor_groupings",
            "nom_arrival",
            "nom_departure",
            "nom",
        ]

        # strip descriptor rows at top of worksheet
        df_ = df_.iloc[5:].copy()

        df_.loc[:, "state"] = df_["state"].fillna(method="ffill")
        df_.loc[:, "major_groupings"] = df_["major_groupings"].fillna(method="ffill")
        df_.loc[:, "minor_groupings"] = df_.loc[:, "minor_groupings"].fillna(
            value="Total"
        )
        df_ = df_.dropna(axis="rows", thresh=4)
        df_.state = df_.state.str.replace(
            r"Australia\([^\)]\)", "Australia", regex=True
        )

        if calendar:
            df_["year"] = pd.datetime(year_int, 12, 31)
        else:
            df_["year"] = pd.datetime(year_int, 6, 30)

        yield df_


def strip_footnote_marks(df, col_name=None, is_index=False):
    """
    ABS columns (often states) have footnotes (eg (a), (b) etc)
    Remove them
    """

    if is_index:
        df.index = df.index.str.replace(r" (.\)", "").str.strip()
    else:
        df[col_name] = df[col_name].str.replace(r" (.\)", "").str.strip()

    return df


def make_year_date(df_index, is_calendar=True):
    """
    For certain historical data, ABS often only supplies the year number, not a date
    Convert index to a time stamp

    Only coverts for financial and calendar year

    Parameters
    ----------
    df_index: pandas index (index or columns)
      contains a set of (non-contiguous) year values

    is_calendar: boolean
      True for calendar year, False for financial year
    """
    # assert df_index is ?  how to check a
    if is_calendar:
        df_index = [pd.datetime(year, 12, 31) for year in df_index]
    else:
        df_index = [pd.datetime(year, 6, 30) for year in df_index]

    return df_index


def download_abs_file(url, xl_file_name, data_folder=DATA_FOLDER):
    """
    Download the excel file given by the url
    """
    session = HTMLSession()

    if is_file_type_downloadable(url):
        xl_file = session.get(url)
        with open(data_folder / xl_file_name, "wb") as output:
            output.write(xl_file.content)
    else:
        raise ValueError(f"Chris - Not valide excel file: {url}, {xl_file_name}.")

    # print(f'{file_name} donwloaded.')
    return None


def download_abs_catalog_excel_files(
    cat_no="3101.0", url_cat_downloads_page=None, download_folder=DATA_FOLDER_AUDIT
    ):
    """
    Download all excel files associated with a given catalog number
    """
    print(url_cat_downloads_page)
    session = HTMLSession()

    if url_cat_downloads_page is None:
        latest_release_base_url = "http://www.abs.gov.au/ausstats/abs@.nsf/mf/"
        cat_no = latest_release_base_url + cat_no
        url_cat_downloads_page = get_downloads_page_url(cat_no)

    excel_downloads_page = session.get(url_cat_downloads_page)

    # all downloads are tr elements of class 'listentry'
    links_list = excel_downloads_page.html.find("tr.listentry")

    # pattern to find excel links - eg 31010do003_200106.xls
    pat = r"log\?openagent&([^\.]+\.xls)"

    Path.mkdir(download_folder, exist_ok=True)

    for entry in links_list:
        # each links_list class contains 1 or 2 links: when it's two,
        # it's for an excel and a zip file
        for link in list(entry.absolute_links):
            # check, and get, if it's an exel file
            file_search = re.search(pat, link, re.IGNORECASE)
            if file_search:
                xl_file_name = file_search.group(1)
                display(HTML(f'<a href="{link}">{entry.text}</a>, {xl_file_name}'))
                download_abs_file(link, xl_file_name, download_folder)
                time.sleep(2)

                # if is_file_type_downloadable(link):
                #     xl_file = session.get(link)
                #     with open(data_folder / xl_file_name, 'wb') as output:
                #         output.write(xl_file.content)
                time.sleep(1)  # small break so as not to hammer ABS
    return


#  ------------- ASGS material  -------------


def ASGS_definitions(dict_folder=DICT_FOLDER):
    """
    Return a dataframe containing ASGS 2016 links

    Parameters:
    ----------

    Returns:
    -------
    asgs: a datafram of all ASGS regions
    asgs_mapper: a pandas Series mapping ASGS codes (index) to ASGS names
    """
    asgs = pd.read_csv(dict_folder / "SA2_2016_AUST.csv")

    code_2_name_dict = {
        "SA2_MAINCODE_2016": "SA2_NAME_2016",
        "SA3_CODE_2016": "SA3_NAME_2016",
        "SA4_CODE_2016": "SA4_NAME_2016",
        "GCCSA_CODE_2016": "GCCSA_NAME_2016",
        "STATE_CODE_2016": "STATE_NAME_2016",
    }

    def gen_make_code_mapper(code_2_name_dict, asgs):
        for code, name in code_2_name_dict.items():
            code_map = (
                asgs.set_index(code, drop=True)[name]
                .drop_duplicates()
                .rename_axis("code")
                .rename("name")
            )
            code_map.index = code_map.index.astype(str)
            yield code_map

    asgs_mapper = pd.concat(gen_make_code_mapper(code_2_name_dict, asgs))

    return asgs, asgs_mapper


def get_state_gccsa_dict(asgs=None):
    if asgs is None:
        asgs, asgs_mapper = ASGS_definitions()

    gccsa_state_dict = (
        asgs[["GCCSA_NAME_2016", "STATE_NAME_2016"]]
        .drop_duplicates()
        .set_index("GCCSA_NAME_2016")
        .squeeze()
        .to_dict()
    )

    other_caps_dict = {
        "Sydney": "New South Wales",
        "Melbourne": "Victoria",
        "Brisbane": "Queensland",
        "Adelaide": "South Australia",
        "Perth": "Western Australia",
        "Hobart": "Tasmania",
        "Darwin": "Northern Territory",
        "Canberra": "Australian Capital Territory",
        "Total": "Total",
        "total": "total",
    }

    return {**gccsa_state_dict, **other_caps_dict}


def get_state_gccsa_dict(asgs=None):
    if asgs is None:
        asgs, asgs_mapper = ASGS_definitions()

    gccsa_state_dict = (
        asgs[["GCCSA_NAME_2016", "STATE_NAME_2016"]]
        .drop_duplicates()
        .set_index("GCCSA_NAME_2016")
        .squeeze()
        .to_dict()
    )

    other_caps_dict = {
        "Sydney": "New South Wales",
        "Melbourne": "Victoria",
        "Brisbane": "Queensland",
        "Adelaide": "South Australia",
        "Perth": "Western Australia",
        "Hobart": "Tasmania",
        "Darwin": "Northern Territory",
        "Canberra": "Australian Capital Territory",
        "Total": "Total",
        "total": "total",
    }

    return {**gccsa_state_dict, **other_caps_dict}


def get_sa2_gccsa_dict(asgs=None):
    if asgs is None:
        asgs, asgs_mapper = ASGS_definitions()

    sa2_state_dict = (
        asgs[["SA2_NAME_2016", "STATE_NAME_2016"]]
        .drop_duplicates()
        .set_index("SA2_NAME_2016")
        .squeeze()
        .to_dict()
    )

    return sa2_state_dict


def pop_component_definitions():
    component_2_name_dict = {
        "1": "births",
        "2": "deaths",
        "3": "natural_increase",
        "4": "nim_arrivals",
        "5": "nim_departures",
        "6": "nim",
        "7": "nom_arrivals",
        "8": "nom_departures",
        "9": "nom",
        "10": "erp",
    }
    return component_2_name_dict


def index_to_datetime(df):
    df.index = pd.to_datetime(df.index.astype(str) + "-06-30")
    df.index.name = "date"
    return df


def drop_columns(df, columns):
    df.columns = df.columns.droplevel(columns)
    return df


def drop_level(df, levels):
    df.columns = df.columns.droplevel(levels)
    return df


def abs_stat_sdmx(sdmx="ABS_ANNUAL_ERP_ASGS2016", drop_levels=["FREQUENCY", "MEASURE"]):
    """
    Return the ERP for all ASGS levels SA2 and above via ABS SDMX interface

    Parameters
    ----------
    sdmx: str, sdmx dataset to extract
    drop_levels: list, levels to drop from column multi-index returned by SDMX
    """

    ABS_request = Request("ABS")

    df = (
        ABS_request.data(sdmx)
        .write()
        .pipe(index_to_datetime)
        .pipe(drop_level, levels=drop_levels)
        .sort_index(axis="index")
        .sort_index(axis="columns")
        .unstack()
        .rename("value")
        .reset_index()
    )

    return df


# ------------- Redundant functions?  -------------


def get_url_dict(url_downloads_page):
    """
    Create a dictionary containing the table name and url of all downloadable excel files.

    The order of the dict is the order is implicilty the table order on the page

    Parameters:
    ----------
    url_downloads_page: str
      The downloads page for any catalog item

    Returns
    -------
    url_dict: dictionary
      the key is the table name, the value is the url
    """

    # TODO: reassess whether download excel or zip
    # if zip, need to upack prior to storing
    # Datacubes are zips anyway
    session = HTMLSession()
    r = session.get(url_downloads_page)

    url_dict = dict()

    # each row in the html table that contains an ABS table  has class attribute 'listentry'
    # find all such rows
    tables = r.html.find("tr.listentry")
    for t in tables:
        # only want the links associated with each ABS table
        if t.text.lower().startswith("table"):
            # the links in each row are both zip, and xls - only want xls
            for link in t.absolute_links:
                if ".xls" in link:
                    url_dict[t.text] = link

    return url_dict


def get_table_url(url, file_name):  # eg 310101 or LM5
    """
    Get the url for a Table, eg Table 1 from 3101 or LM5 from 6291 etc,
    from the Downloads page, the relevant catelog item

    Parameters
    ----------
    url: str
        The url of the list of downloads associated with the catalog
    file_name: str
        A unique part of the filename that will be in the appropriate donwload link

    Returns
    ------
    links[0]: str  (first, and only element from a filtered list comprehension of URLs)
        The url of the data for the relevant file
    """
    file_name = file_name.lower()

    session = HTMLSession()
    r = session.get(url)

    links = [link for link in r.html.absolute_links if file_name in link.lower()]

    if len(links) > 1:
        raise ValueError(
            f"Chris: Expected unique link, but found than 1 link found: {links}"
        )

    return links


def get_first_table_details(url_dict):
    for i, table_and_url in enumerate(url_dict.items()):
        if i == 0:
            table, url = table_and_url
            break
    return table, url


def display_url_dict(url_dict):
    for table, url in url_dict.items():
        display(HTML(f'<a href="{url}">{table}</a>'))
