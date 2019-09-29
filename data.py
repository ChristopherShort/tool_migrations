"""utilities to tidy data and make parquet files
"""

from pathlib import Path
import re

import pandas as pd

import chris_utilities as cu


DATA_ABS_PATH = Path.home() / "Documents/Analysis/Australian economy/Data/ABS"

capital_names = {"Greater Sydney": "Sydney",
                 "Greater Melbourne": "Melbourne",
                 "Greater Brisbane": "Brisbane",
                 "Greater Perth": "Perth",
                 "Greater Adelaide": "Adelaide",
                 "Greater Hobart": "Hobart",
                 "Greater Darwin": "Darwin",
                "Australian Capital Territory": "Canberra",
            }

regional_names = {"Rest of NSW": "NSW",
                "Rest of Vic.": "Vic",
                "Rest of Qld": "Qld",
                "Rest of WA": "WA",
                "Rest of SA": "SA",
                "Rest of Tas.": "Tas",
                "Rest of NT": "NT"}


def series_id_3101():
    series_id = {
        "births": "A2133244X",
        "deaths": "A2133245A",
        "natural_increase": "A2133252X",
        "interstate_arrivals": "A2133246C",
        "interstate_departures": "A2133247F",
        "overseas_arrivals": "A2133248J",
        "overseas_departures": "A2133249K",
        "net_permanent_and_long_term_movement": "A2133253A",
        "migration_adjustment": "A2133250V",
        "net_overseas_migration": "A2133254C",
        "estimated_resident_population": "A2133251W",
    }
    return series_id


def read_abs_data(
    folder_path=DATA_ABS_PATH, fname="310101.xls", series_id=None, sheet_name="Data1"
    ):
    """ Extract data from an ABS time series file
    fname="310101.xls",
    series_id=None,   eg{"births": "A2133244X", "deaths": "A2133245A"...}
    sheet_name="Data1"

    Time series data stored in worksheets labeled "Data1, Data2, etc or Table 9.1 etc
    First 9 rows contains meta data (description, unit, Series Type, Data type, Frequency,
    colleciton month, start date, end date, number of observations)
    Row 10 contains series ID
    First column contains the dates.
    ABS dates often are start of month - so adjust below

    Sometimes there is trailing data containing footnotes

    # Add in date of release

    """

    fpath = folder_path / fname

    df = pd.read_excel(
        fpath, sheet_name=sheet_name, skiprows=9, index_col=0, na_values=["", "-", " "]
    )

    # Make dates end of month
    df.index = df.index + pd.offsets.MonthEnd()
    df.index.name = "date"

    if series_id is None:
        # Return all data with ABS variable names
        # Consider returning meta data as well
        # How will user know requesting both meta and data?
        return df

    # else - keep selected series
    series_names_to_keep = list(series_id.values())
    df = df[series_names_to_keep]

    df.columns = list(series_id.keys())

    return df


def read_abs_notes(data_folder=DATA_ABS_PATH, fname="310101.xls", sheet_name="Data1"):
    """
    Read notes in a data table
    Start in column A
    Notes commence with an open parenthesis "("
    identify range of notes by commencing with "("
        This assumes that last note does not have a line continuation, or is not
        a line continuation itself
    """
    fpath = data_folder / fname

    notes = pd.read_excel(fpath, sheet_name=sheet_name, use_cols="A", names=["note"])
    # Print release date:
    idx = notes.note.str.lower().str.contains("released").fillna(False)
    print(notes[idx].to_string(index=False, header=False))
    print()

    # identify index range containing notes
    notes_rows = notes[notes.note.str[0] == "("].index

    note_start = notes_rows[0]
    note_end = notes_rows[-1]

    for note in notes.loc[note_start:note_end].values:
        print(note[0])
        print()

    return


def read_abs_meta_data(data_folder=DATA_ABS_PATH, fname="310101.xls", sheet_name="Data1"):
    """
    Return met data for all sereis from an ABS time series worksheet.

    # TODO: think about multiple data sheets, should this single sheet function be generalised
    
    Parameters
    ----------
    folder_path : Path object, optional
        path to folder containing ABS workbook 
    fname : str, optional
        abs timeseries workbook, by default "310101.xls"
    sheet_name : str, optional
        Data1, Data2, etc, by default "Data1"description_labels=None
    
    Returns
    -------
    dataframe
        a dataframe containing ABS time series meta data, index is Series ID, columns are ABS meta data
    """

    # Meta data contained in the first 10 rows
    nrows = 10

    fpath = Path(data_folder) / fname

    meta = pd.read_excel(fpath, sheet_name=sheet_name, header=None, nrows=nrows)

    # set column names to series_id (last row), and remove the last row
    meta.columns = meta.iloc[-1]
    meta = meta[:-1]

    if meta.iloc[:, 0].isna()[0]:
        meta.iloc[0, 0] = "Description"
    else:
        meta.iloc[0, 0] = meta.iloc[0, 0].replace(
            " *> ", "", regex=True
        )  # wonder what ABS workbooks needed this?

    return meta.set_index("Series ID").rename_axis(columns=None).T


def meta_description_split(df, label_list):
    """Split ABS Description field into components.  Components are delimited with ";"
    
    Parameters
    ----------
    df : dataframe
        a 
    label_list : list
        contains labels for the delimited field in Description column

    Returns
    -------
    dataframe :
        with index of ABS Series ID and columns of the Description elements
    """

    def drop_last_column(df):
        """ABS Descriptions use ";" as separator and the last character is always a ";"
            Pandas doesn"t allow a lambda function in the drop columns, so this pipe funciton
        
        Parameters
        ----------
        df : dataframe
        
        Returns
        -------
        dataframe :
            
        """
        cols = df.columns
        return df.drop(columns=cols[-1])

    if "Description" not in df.columns:
        raise ValueError("'Description' not found in columns")

    df = (
        df.Description.str.split(pat=r" *; *", expand=True)
        .pipe(drop_last_column)
        .rename_axis(index="Series_ID", columns=None)
    )

    df.columns = label_list

    return df


def remove_note_references(df):
    """Remove references such as "(a)" from label rows in ABS non-timeseries worksheets
    
    Parameters
    ----------
    df : dataframe
        a dataframe containing an ABS worksheet with row labels in first colum 
    """
    pat = r"(.+)(\([^\)]+\))"
    df.iloc[:, 0] = df.iloc[:, 0].str.replace(pat, r"\1")

    pat = r"\.$"
    df.iloc[:, 0] = df.iloc[:, 0].str.replace(pat, "")

    return df


def components_state_to_parquet(
    data_folder=DATA_ABS_PATH, filenames=["310102.xls", "3101016a.xls", "3101016b.xls"]
    ):
    """
    Extract the state level components from 310102 (TABLE 2. Population Change, Components) and
    from 3101016a&b (TABLE 16A/B. Interstate Arrivals/Departures)
    Return a tidy dataframe
    """

    def gen_components(data_folder):
        for filename in filenames:
            meta = read_abs_meta_data(data_folder=DATA_ABS_PATH, fname=filename)


            # Extract "component" and "state" from Description, will have trailing column due to ";"
            tidy_labels = (
                meta["Description"]
                .str.split(";", expand=True)
                .rename(columns={0: "component", 1: "state"})
                .assign(component=lambda x: x.component.str.strip())
                .assign(state=lambda x: x.state.str.strip())
                .drop(columns=[2])
                .rename_axis(index="series_id")
            )

            components = (
                read_abs_data(data_folder, filename)
                .rename_axis(columns="series_id", index="date")
                .unstack()
                .rename("value")
                .reset_index()
                .assign(state=lambda x: x["series_id"].map(tidy_labels.state))
                .assign(component=lambda x: x["series_id"].map(tidy_labels.component))
            )

            if filename == "310102.xls":
                # remove any "Change over previous quarter" rows
                idx = components.component == "Change Over Previous Quarter"
                components = components[~idx]

            yield components

    components = pd.concat(gen_components(data_folder), sort=False)
    components = components[["date", "state", "component", "value"]]

    components.to_parquet(DATA_ABS_PATH / "310102_state_components.parquet")

    return components


def components_sa2_to_parquet(data_folder=DATA_ABS_PATH, fname=None):
    """Create parquet file of 3218 SA2 level component data
    
    Parameters
    ----------
    data_folder : [type], optional
        [description], by default DATA_ABS_PATH
    fname : [type], optional
        [description], by default None
    
    Returns
    -------
    [type]
        [description]
    """
    if fname is None:
        fname = "32180ds0001_2017-18.xls"

    col_names = [
        "S_T_code",
        "S_T_name",
        "GCCSA_code",
        "GCCSA_name", 
        "SA4_code",
        "SA4_name",
        "SA3_code",
        "SA3_name",
        "SA2_code",
        "SA2_name",
        "ERP_first",
        "ERP_second",
        "ERP_delta_levels",
        "erp_delta_percent",
        "natural_increase",
        "NIM",
        "NOM",
        "Area_km2",
        "population_density",
    ]

    if "2016-17" in fname:
        ### ABS has ERP delta and percent after nim, nom etc for 2016-17
        col_names = [
            "S_T_code",
            "S_T_name",
            "GCCSA_code",
            "GCCSA_name", 
            "SA4_code",
            "SA4_name",
            "SA3_code",
            "SA3_name",
            "SA2_code",
            "SA2_name",
            "ERP_first",
            "ERP_second",
            "natural_increase",
            "NIM",
            "NOM",
            "erp_delta_percent",
            "ERP_delta_levels",
            "Area_km2",
            "population_density",
        ]
    col_names = replace_erp_year(col_names, fname)
    
    table_sheet_range = range(1,9)
    
    df = (pd
            .concat(gen_states(fname, col_names, table_sheet_range), sort=False)
            .pipe(cu.clean_column_names)
    )

    fparquet_name = (
            (DATA_ABS_PATH / fname)
            .stem
            .replace("-", "_") 
            + "_sa2.parquet"
    )
    
    df.to_parquet(DATA_ABS_PATH / fparquet_name)

    return df


def components_lga_to_parquet(data_folder=DATA_ABS_PATH, fname=None):
    col_names = [
        "lga_code",
        "local_government_area",
        "ERP_first",
        "ERP_second",
        "erp_delta_levels",
        "erp_delta_percent",
        "natural_increase",
        "nim",
        "nom",
        "area_km2",
        "population_density",
    ]

    col_names = replace_erp_year(col_names, fname)
    table_sheet_range = range(1,8)
    
    df = (pd
            .concat(gen_states(fname, col_names, table_sheet_range))
            .pipe(cu.clean_column_names)
    )

    fparquet_name = (
            (DATA_ABS_PATH / fname)
            .stem
            .replace("-", "_") 
            + "_lga.parquet"
    )

    df.to_parquet(DATA_ABS_PATH / fparquet_name)

    return df


def gen_states(fname, col_names, table_sheet_range):
    """generator to read 3218 workbooks
    
    Parameters
    ----------
    fname : [type]
        [description]
    col_names : [type]
        [description]
    """
    for table_no in table_sheet_range:
        df = pd.read_excel(DATA_ABS_PATH / fname,
                            sheet_name="Table " + str(table_no),
                            skiprows=7,
                                skipfooter=7,
                                )

        # Only want components, drop empty rows, columns as well as summary state column
        df = (df
                .dropna(axis=1, how="all")
                .dropna(axis=0, thresh=11)
        )

        df.columns = col_names

        for col in df.select_dtypes(include=["float"]).columns:
            if col[:17] not in ["erp_delta_percent", "population_densit"]:
                df[col] = df[col].astype(int)
        
        yield df


def replace_erp_year(col_names, fname):
    """replace place holders ERP_first and ERP_second in col_names with year in fname
    
    Parameters
    ----------
    colnames : [type]
        [description]
    fname : [type]
        [description]
    """
    
    m = re.search(r"(20\d\d)-(\d\d)", fname)

    #replace ERP_first in col_names
    col_names = ["erp_" + m.group(1) if col == "ERP_first" else col for col in col_names]

    #replace ERP_second in col_names
    col_names = ["erp_20" + m.group(2) if col == "ERP_second" else col for col in col_names]

    #set year for population_density
    return ["population_density_20" + m.group(2) if col == "population_density" else col for col in col_names]

