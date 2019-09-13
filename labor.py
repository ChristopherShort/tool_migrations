from pathlib import Path
import pandas as pd

import chris_utilities as cu


# TODO automatically download the datacubes and convert

DATA_FOLDER = Path.home() / "Documents/Analysis/Australian economy/Data/ABS/"


def convert_lm7_excel_to_parquet(data_folder):
    '''
    Read LM7.xlsx, clean and convert to parquet file

    Parameters
    ----------
    data_folder: Path objectdd
        expect f'{Path.home()}/Documents/Analysis/Australian economy/Data/ABS'

    Returns
    -------

    TODO: automatically download latest LM7 file
    '''

    col_names = {
        'Month': 'date',
        'Sex': 'sex',
        'Main English-speaking countries': 'MESC',
        'Elapsed years since arrival': 'elapsed_years_since_arrival',
        'State and territory (STT): ASGS (2011)': 'state',
        "Employed full-time ('000)": 'employed_full_time',
        "Employed part-time ('000)": 'employed_part_time',
        "Unemployed looked for full-time work ('000)": 'unemployed_looked_full_time',
        "Unemployed looked for only part-time work ('000)": 'unemployed_looked_part_time_only',
        "Not in the labour force (NILF) ('000)": 'nilf',
    }

    osb = {'Main English-speaking countries': 'overseas',
           'Other than main English-speaking countries': 'overseas',
           'Australia (includes External Territories)': 'Australia',
           'Not Stated / Inadequately Described / Born at sea': 'unknown'
           }

    idx_labor_force = ['employed_full_time', 'employed_part_time', 'unemployed_looked_full_time',
                       'unemployed_looked_part_time_only']

    df = (pd
          .read_excel(data_folder / 'LM7.xlsx',
                      usecols='A:J',
                      sheet_name='Data 1',
                      skiprows=3,
                      parse_dates=[0], infer_datetime_format=True,
                      )
          .rename(columns=col_names)
          .assign(date=lambda x: x.date + pd.offsets.MonthEnd(0))
          .assign(COB=lambda x: x.MESC.map(osb))
          .assign(labor_force=lambda x: x[idx_labor_force].sum(axis=1))
          .assign(employed_total=lambda x: x.employed_full_time + x.employed_part_time)
          .assign(population=lambda x: x.nilf + x.labor_force)
          .set_index('date')
          )

    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].astype('category')

    df.to_parquet(data_folder / 'LM7.parquet')

    return df


def convert_lm5_excel_to_parquet(data_folder):
    '''
    Read in LM5.xlsx, clean and convert to parquet file

    Parameters
    ----------
    data_folder: Path objectdd
        expect f'{Path.home()}/Documents/Analysis/Australian economy/Data/ABS'

    Returns
    -------

    TODO: automatically download latest LM5 file
    '''

    col_names = {
        'Month': 'date',
        'Sex': 'sex',
        'Age': 'age_group',
        'Major country group (subcontinent) of birth: SACC (2011)': 'cob',
        "Employed full-time ('000)": 'employed_full_time',
        "Employed part-time ('000)": 'employed_part_time',
        "Unemployed looked for full-time work ('000)": 'unemployed_looked_full_time',
        "Unemployed looked for only part-time work ('000)": 'unemployed_looked_part_time_only',
        "Not in the labour force (NILF) ('000)": 'nilf',
    }

    idx_labor_force = ['employed_full_time', 'employed_part_time', 'unemployed_looked_full_time',
                       'unemployed_looked_part_time_only']

    df = (pd
          .read_excel(data_folder / 'LM5.xlsx',
                      usecols='A:I',
                      sheet_name='Data 1',
                      skiprows=3,
                      parse_dates=[0], infer_datetime_format=True,
                      )

          .rename(columns=col_names)
          .assign(date=lambda x: x.date + pd.offsets.MonthEnd(0))
          .assign(employed_total=lambda x: x.employed_full_time + x.employed_part_time)
          .assign(labor_force=lambda x: x[idx_labor_force].sum(axis=1))
          .assign(population=lambda x: x.nilf + x.labor_force)
          .assign(participation_rate=lambda x: x.labor_force.divide(x.population) * 100)
          .set_index('date')
          )

    # for col in df.select_dtypes(include='object').columns:
    #     df[col] = df[col].astype('category')

    df.to_parquet(data_folder / 'LM5.parquet')

    return df


def read_lm5(data_folder=DATA_FOLDER, delete_unknown_COB=True):
    """[summary]
    
    Parameters
    ----------
    data_folder : [type], optional
        [description], by default DATA_FOLDER
    remove_unknown_COB : bool, optional
        [description], by default True
    
    Returns
    -------
    [type]
        [description]
    """
    df = pd.read_parquet(data_folder / 'LM5.parquet')

    if delete_unknown_COB:
        return remove_unknown_COB(df)
    else:
        return df


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
    idx = (df.COB == 'Inadequately Described / Born at Sea') | (df.COB == 'Not stated')
    df = df.loc[~idx]

    #rename - Australia and OS born
    idx = df.COB == 'Australia (includes External Territories)'

    df.loc[idx,'COB'] = 'Australian-born'
    df.loc[~idx, 'COB'] = 'Overseas-born'

    return df


def male_female_population(df=None, delete_unknown_COB=True):
    """[summary]
    """

    if df is None:
        df = read_lm5()
        if delete_unknown_COB:
            df = remove_unknown_COB(df)
    
    return (df
        .groupby(["date", 'sex'])
        ['labour_force', 'population'].sum()
        .unstack()
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
        age_mapping = {'15-19 years': '15-19 years',
                '20-24 years': '20-24 years',
                '25-29 years': '25-34 years',
                '30-34 years': '25-34 years',
                '35-39 years': '35-44 years',
                '40-44 years': '35-44 years',
                '45-49 years': '45-54 years',
                '50-54 years': '45-54 years',
                '55-59 years': '55-64 years',
                '60-64 years': '55-64 years',
                '65 years and over': '65 years and over'
                }

    df.age = df.age.map(age_mapping)

    # remove data not required for this analysis
    return df.drop(columns=[
        'employed_full_time', 'employed_part_time',
        'unemployed_looked_full_time', 'unemployed_looked_part_time_only', 
        'nilf',
    ]
    )


def lf_hierarchical(df=None):
    """

    
    Parameters
    ----------
    df : [type], optional
        [description], by default None
    """

    if not df:
        df = read_lm5()
        df = set_age_groups(df)
    
    return (df
        .groupby(["date", "sex", "age", "COB"])
        ["labour_force", "population"]
        .sum()
        .unstack(["COB", "sex", "age"])
    )


