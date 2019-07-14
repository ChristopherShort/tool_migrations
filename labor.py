from pathlib import Path
import pandas as pd


# TODO automatically download the datacubes and convert

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
