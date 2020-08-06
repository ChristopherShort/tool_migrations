from pathlib import Path
import pandas as pd
import file_paths

import chris_utilities as cu


# TODO automatically download the datacubes and convert

DATA_ABS_PATH = file_paths.abs_data_folder


def convert_lm7_excel_to_parquet(data_folder):
    """
    Read LM7.xlsx, clean and convert to parquet file

    Parameters
    ----------
    data_folder: Path objectdd
        expect f'{Path.home()}/Documents/Analysis/Australian economy/Data/ABS'

    Returns
    -------

    TODO: automatically download latest LM7 file
    """

    col_names = {
        "Month": "date",
        "Sex": "sex",
        "Main English-speaking countries": "MESC",
        "Elapsed years since arrival": "elapsed_years_since_arrival",
        "State and territory (STT): ASGS (2011)": "state",
        "Employed full-time ('000)": "employed_full_time",
        "Employed part-time ('000)": "employed_part_time",
        "Unemployed looked for full-time work ('000)": "unemployed_looked_full_time",
        "Unemployed looked for only part-time work ('000)": "unemployed_looked_part_time_only",
        "Not in the labour force (NILF) ('000)": "nilf",
    }

    osb = {
        "Main English-speaking countries": "overseas",
        "Other than main English-speaking countries": "overseas",
        "Australia (includes External Territories)": "Australia",
        "Not Stated / Inadequately Described / Born at sea": "unknown",
    }

    idx_labor_force = [
        "employed_full_time",
        "employed_part_time",
        "unemployed_looked_full_time",
        "unemployed_looked_part_time_only",
    ]

    df = (
        pd.read_excel(
            data_folder / "LM7.xlsx",
            usecols="A:J",
            sheet_name="Data 1",
            skiprows=3,
            parse_dates=[0],
            infer_datetime_format=True,
        )
        .rename(columns=col_names)
        .assign(date=lambda x: x.date + pd.offsets.MonthEnd(0))
        .assign(COB=lambda x: x.MESC.map(osb))
        .assign(labor_force=lambda x: x[idx_labor_force].sum(axis=1))
        .assign(employed_total=lambda x: x.employed_full_time + x.employed_part_time)
        .assign(population=lambda x: x.nilf + x.labor_force)
        .set_index("date")
    )

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype("string")

    df.to_parquet(data_folder / "LM7.parquet")

    return df


def convert_lm5_excel_to_parquet(data_folder):
    """
    Read in LM5.xlsx, clean and convert to parquet file

    Parameters
    ----------
    data_folder: Path objectdd
        expect f'{Path.home()}/Documents/Analysis/Australian economy/Data/ABS'

    Returns
    -------

    TODO: automatically download latest LM5 file
    """

    col_names = {
        "Month": "date",
        "Sex": "sex",
        "Age": "age_group",
        "Major country group (subcontinent) of birth: SACC (2011)": "cob",
        "Employed full-time ('000)": "employed_full_time",
        "Employed part-time ('000)": "employed_part_time",
        "Unemployed looked for full-time work ('000)": "unemployed_looked_full_time",
        "Unemployed looked for only part-time work ('000)": "unemployed_looked_part_time_only",
        "Not in the labour force (NILF) ('000)": "nilf",
    }

    idx_labor_force = [
        "employed_full_time",
        "employed_part_time",
        "unemployed_looked_full_time",
        "unemployed_looked_part_time_only",
    ]

    df = (
        pd.read_excel(
            data_folder / "LM5.xlsx",
            usecols="A:I",
            sheet_name="Data 1",
            skiprows=3,
            parse_dates=[0],
            infer_datetime_format=True,
        )
        .rename(columns=col_names)
        .assign(date=lambda x: x.date + pd.offsets.MonthEnd(0))
        .assign(employed_total=lambda x: x.employed_full_time + x.employed_part_time)
        .assign(labor_force=lambda x: x[idx_labor_force].sum(axis=1))
        .assign(population=lambda x: x.nilf + x.labor_force)
        .assign(participation_rate=lambda x: x.labor_force.divide(x.population) * 100)
        .set_index("date")
    )

    # for col in df.select_dtypes(include='object').columns:
    #     df[col] = df[col].astype('category')

    df.to_parquet(data_folder / "LM5.parquet")

    return df


def convert_lm1(data_folder=DATA_ABS_PATH):
    """
    Read in LM1.xlsx, clean and convert to parquet file

    Parameters
    ----------
    data_folder: Path objectdd
        expect f'{Path.home()}/Documents/Analysis/Australian economy/Data/ABS'

    Returns
    -------

    TODO: automatically download latest LM5 file
    """

    col_names = {
            "Month": "date",
            "Sex": "sex",
            "Age": "age_group",
            "Social marital status": "social_marital_status",
            "Greater capital city and rest of state (GCCSA): ASGS (2011)": "gccsa",
            "Employed full-time ('000)": "employed_full_time",
            "Employed part-time ('000)": "employed_part_time",
            "Unemployed looked for full-time work ('000)": "unemployed_looked_full_time",
            "Unemployed looked for only part-time work ('000)": "unemployed_looked_part_time_only",
            "Not in the labour force (NILF) ('000)": "nilf",
        }

    idx_labor_force = [
            "employed_full_time",
            "employed_part_time",
            "unemployed_looked_full_time",
            "unemployed_looked_part_time_only",
        ]
    df = (pd
        .read_excel(
            DATA_ABS_PATH / "LM1.xlsx",
            usecols="A:J",
            sheet_name="Data 1",
            skiprows=3,
            parse_dates=[0],
            infer_datetime_format=True,
        )
        .rename(columns=col_names)
        .assign(date=lambda x: x.date + pd.offsets.MonthEnd(0))
        .assign(employed_total=lambda x: x.employed_full_time + x.employed_part_time)
        .assign(labor_force=lambda x: x[idx_labor_force].sum(axis=1))
        .assign(population=lambda x: x.nilf + x.labor_force)
        .assign(participation_rate=lambda x: x.labor_force.divide(x.population) * 100)
        .set_index("date")
    )


    df.to_parquet(data_folder / "LM1.parquet")

    return df


def read_lm1(data_folder=DATA_ABS_PATH):
    return pd.read_parquet(data_folder / "LM1.parquet")


def read_lm5(data_folder=DATA_ABS_PATH, delete_unknown_COB=True, age_mapping=None):
    """[summary]
    
    Parameters
    ----------
    data_folder : [type], optional
        [description], by default DATA_FOLDER
    delete_unknown_COB : bool, optional
        [description], by default True
    age_mapping : [type], optional
        [description], by default None
    """

    df = pd.read_parquet(data_folder / "LM5.parquet")

    if delete_unknown_COB:
        # Work around for COB being a category variable
        # TODO figure out how run "remove_unknown" containing category variables
        df["COB"] = df["COB"].astype("string")
        df = remove_unknown_COB(df)

    if age_mapping is None:
        return df
    else:
        return set_age_groups(df, age_mapping=age_mapping)


def read_lm7(data_folder=DATA_ABS_PATH):
    """[summary]
    
    Parameters
    ----------
    data_folder : [type], optional
        [description], by default DATA_ABS_PATH
    """
    return pd.read_parquet(DATA_ABS_PATH / "LM7.parquet")


def remove_unknown_COB(df):
    """
    When estimating share of migrants, ABS removes those without country of birth
    This is equivalent toa ssuming the missing COB information is uniformly distributed across Aus. Born and OSB
    Parameters
    ----------
    df : [type]
        [description]
    """
    # Remove unknown COB observation
    idx = (df.COB == "Inadequately Described / Born at Sea") | (df.COB == "Not stated")
    df = df.loc[~idx].copy()

    # rename - Australia and OS born
    idx = df.COB == "Australia (includes External Territories)"

    df.loc[idx, "COB"] = "Australian-born"
    df.loc[~idx, "COB"] = "Overseas-born"

    return df


def lf_hierarchical(df=None):
    """

    
    Parameters
    ----------
    df : [type], optional
        [description], by default None
    """

    if df is None:
        df = read_lm5()
        df = set_age_groups(df)

    return (
        df.groupby(["date", "sex", "age", "COB"])[["labour_force", "population"]]
        .sum()
        .unstack(["COB", "sex", "age"])
        .sort_index(axis="columns")
    )


def gender_population(df=None, delete_unknown_COB=True):
    """[summary]
    """

    if df is None:
        df = read_lm5()
        if delete_unknown_COB:
            df = remove_unknown_COB(df)

    return df.groupby(["date", "sex"])["labour_force", "population"].sum().unstack()


def rename_col_index(df, label):
    """rename level 0 of column indes to "label"
    
    Parameters
    ----------
    df : dataframe
        labour force and population timeseries data
    label : string
        name of level 0 of multiindex
    
    Returns
    -------
    df
        dataframe with column multiindex level 0 relabelled 
    """
    df.columns.names = [label] + df.columns.names[1:]
    return df


def cob_population(df=None, delete_unknown_COB=True):
    """[summary]
    """

    if df is None:
        df = read_lm5()
        if delete_unknown_COB:
            df = remove_unknown_COB(df)

    return (
        df.groupby(["date", "COB"])["labour_force", "population"].sum().unstack().pipe(rename_col_index, "lf_pop")
    )


def set_age_groups(df, age_mapping=None):
    """[summary]
    
    Parameters
    ----------
    df : [type]
        [description]
    age_mapping : [type], optional
        [description], by default None
    """
    if age_mapping is None:
        age_mapping = {
            "15-19 years": "15-19 years",
            "20-24 years": "20-24 years",
            "25-29 years": "25-34 years",
            "30-34 years": "25-34 years",
            "35-39 years": "35-44 years",
            "40-44 years": "35-44 years",
            "45-49 years": "45-54 years",
            "50-54 years": "45-54 years",
            "55-59 years": "55-64 years",
            "60-64 years": "55-64 years",
            "65 years and over": "65 years and over",
        }

    df.age = df.age.map(age_mapping)

    # remove data not required for this analysis
    return df.drop(
        columns=[
            "employed_full_time",
            "employed_part_time",
            "unemployed_looked_full_time",
            "unemployed_looked_part_time_only",
            "nilf",
        ]
    )


## Australian born & migrant contribution to employment growth
def LM7_organised(df=None, with_missing_COB=False, category="employed_total"):
    """reate data frame of native born and migrant by tim ein Australia by mont
    
    Parameters
    ----------
    df : dataframe, optional
        LM7 datacube, by default None
    
    Returns
    -------
    dataframe
        dataframe with columns:
        Born in Australia; Arrived within last 5 years; Arrived 5-9 years ago; 
        Arrived 20 or more years ago; Arrived 15-19 years ago; Arrived 10-14 years ago
    """

    if df is None:
        df = read_lm7()
        
    
    arrived_order = [
        "Born in Australia",
        "Arrived within last 5 years",
        "Arrived 5-9 years ago",
        "Arrived 10-14 years ago",
        "Arrived 15-19 years ago",
        "Arrived 20 or more years ago",
    ]
    
    if with_missing_COB:
        return (df
            .groupby([df.index, 'elapsed_years_since_arrival'])[category]
            .sum()
            .unstack('elapsed_years_since_arrival')
            .sort_index(axis=1, ascending=False)
        )[col_order]
    else:
        return (df
            .groupby([df.index, 'elapsed_years_since_arrival'])[category]
            .sum()
            .unstack('elapsed_years_since_arrival')
            .drop(columns=['Not stated / Inadequately described / Born at sea'])
            .sort_index(axis=1, ascending=False)
            .reindex(labels=arrived_order, axis='columns')
            .assign(total = lambda x: x.sum(axis='columns'))
        )


def delta_by_duration(df=None, month=6, delta=5, category="employed_total"):
    """[summary]

    Parameters
    ----------
    df : [type], optional
        [description], by default None
    month : int, optional
        [description], by default 6
    delta : int, optional
        number of years, by default 5
    category : str, optional
        [description], by default "employed_total"

    Returns
    -------
    [type]
        [description]
    """
    print(category.replace("_", " ").capitalize())

    if df is None:
        df = LM7_organised(category=category)


    if month:
        idx = df.index.month == month
        df = df[idx]
        # as employed has annual year data
        time_delta = delta
    else:
        # employed has monthly data
        time_delta = 60


    idx = ['Born in Australia', 'total' ]

    delta = df[idx].diff(time_delta)


    delta_order = [
        'Born in Australia',
        'Arrived within last 5 years',
        'arrived_more_than_5_years',
        'total'
                ]

    delta = (pd
                .concat([delta, df['Arrived within last 5 years']], axis='columns')
                .assign(arrived_more_than_5_years = lambda x: x.total - x['Born in Australia'] - x['Arrived within last 5 years'])
                .reindex(labels=delta_order, axis='columns')
                .dropna()
            )
         

    return delta


def share_by_duration(delta=None, month=6, category="employed_total", as_int=False):

    if delta is None:
        delta = delta_by_duration(month=month, category=category)
    
    delta_share = delta.divide(delta.total, axis='rows') * 100
    
    if as_int:
        return (delta_share
            .dropna(axis='index', how='any')
            .round(0)
            .astype(int)
        )
    else:
        return delta_share


def make_c_by_duration(df=None, month=6):
    """ Extract employment levels for Aus. born, and OS born by time in Australia
    
    Parameters:
    -----------
        df: the LM7 dataset (ie sheet: Data 1 from LM7 loaded in a dataframe)
        month:integer or None
            the month to use (eg 6 for financial) if doing annual calculations, if None then return all data
        
    Returns
    -------
        employed: pandas dataframe
    """

    if df is None:
        df = read_lm7()

    arrived_order = ['Born in Australia',
                        'Arrived within last 5 years',
                        'Arrived 5-9 years ago',
                        'Arrived 10-14 years ago',
                        'Arrived 15-19 years ago',
                        'Arrived 20 or more years ago',
                        'total'
                    ]

    # Remove unknown COB
    idx = df.MESC != 'Not Stated / Inadequately Described / Born at sea'


    employed = (df.loc[idx]
                    .groupby(['date', 'elapsed_years_since_arrival'])['employed_total']
                    .sum()
                    .unstack('elapsed_years_since_arrival')
                    .drop(columns=['Not stated / Inadequately described / Born at sea'])
                    .sort_index(axis=1, ascending=False)
                    .reindex(labels=arrived_order, axis='columns')
                    .assign(total = lambda x: x.sum(axis='columns'))
                    .rename_axis(None, axis='columns')
        )

    if month is None:
        return employed
    else:
        idx = employed.index.month == month
        return employed[idx]


