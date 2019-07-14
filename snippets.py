from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib as mpl
from IPython.core.display import HTML, display_html, display

# code to create date from financial year, month columns (eg visa data)

def extract_date(df):
    '''
    create a date column by combine information from the financial year (financial_year_of_visa_grant) and month

    use pd.to_datetime() to convert created string of type yyyy-m-d to a datetime

    Parameters - df contains two key columns:
        financial_year_of_visa_grant: eg '2005-06'
        month: a string on a financial year basis: '01 JUL', '02 AUG' etc

    '''

    # Create a dictionary to map months to numbers
    monthDict = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}

    # rverse the dictioary - and lowercase it
    monthDict = dict([[v.lower(), str(k)] for k, v in monthDict.items()])

    # create dateframe with two columns:  first and second year of the financial year basis
    year = (df
            .financial_year_of_visa_grant.str[:7].str.split('-', expand=True)
            .rename(columns={0: 'fin_year_first', 1: 'fin_year_second'})
            .assign(fin_year_second=lambda x: '20' + x.fin_year_second)
            )

    display(year.iloc[:20])

    # dummy date to check all dates converted
    df = df.assign(date=pd.datetime(1900, 1, 1))

    # index of first and second parts of the finacial year
    idx = df.month.str[3:].str.strip().isin(['JUL',
                                             'AUG',
                                             'SEP',
                                             'OCT',
                                             'NOV',
                                             'DEC']
                                            )

    df.loc[idx, 'date'] = (pd
                           .to_datetime(year.loc[idx, 'fin_year_first'] + '-'
                                        + df[idx].month.str[3:].str.strip().map(monthDict) + '-'
                                        + '1'
                                        )
                           + pd.offsets.MonthEnd(0)
                           )

    df.loc[~idx, 'date'] = (pd
                            .to_datetime(year.loc[~idx, 'fin_year_second'] + '-'
                                         + df[~idx].month.str.strip().str.lower().str[-3:].map(monthDict) + '-'
                                         + '1'
                                         )

                            + pd.offsets.MonthEnd(0)
                            )

    if (df.date == pd.datetime(1900, 1, 1)).any() or (df.date.isna().any()):  # date
        raise ValueError('Chris: Not all dates converted properly')
    else:
        return df


# Make a hierarchical column heading - mapping subclasses to Visa Reporting grouping

# arrivals.columns = (pd
#                        .MultiIndex.
#                           from_tuples(
#                             list(
#                                 zip(
#                                     arrivals.columns.map(dict_visa_reporting),
#                                     arrivals.columns
#                                     )
#                                 )
#                            )
#                      )




# base_data_folder = (Path.home() /
#                         'Documents' /
#                         'Analysis' /
#                         'Australian economy' /,
#                         'Data'
#                     )

# individual_movements_folder = (base_data_folder /
#                     'NOM unit record data' /
#                     'NOM - individual movements'
#                 )

# dict_data_folder = (base_data_folder /
#                     'Dictionaries'
#                     )

# data_output = Path('data')  ## ie relative to the folder with this workbook

def chart_bits(ax):

    ax.spines['bottom'].set_visible(False)

    ax.tick_params(axis='both', labelcolor='black')
    ax.xaxis.set_tick_params(which='both', rotation=0)
    ax.tick_params(axis='x', length=0)

    ax.set_xlabel('')

    # thousands separator)
    ax.yaxis.set_major_formatter(
        mpl.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))

    fig = ax.get_figure()

    fig.show()

    ax.spines['bottom'].set_position(('data', -40_000))
    ax.spines['bottom'].set_position(('axes', 0))
    return


def abs_code_bits(df):
    # remove footnote marks eg '(a)' from elements
    # eg for column 'state'

    df.state = df.state.str.replace(r' (.\)', '').str.strip()

    # Create a spare column - ABS often stacks two labels in one column,
    # In the example below - Gender and State are in the same column
    # Create a separate gender column by approach of finding matching elements
    # Create new column, and fill forward (in this case, fill nan)

    idx = df.state.isin(['Males', 'Females', 'Persons'])

    df['gender'] = np.nan
    df.loc[idx, 'gender'] = df.loc[idx, 'state']

    df.gender = df.gender.ffill()

    df.set_index(['gender', 'state']).dropna(how='all')

    return

def gen_sample_distribution(sample, simulation_size=10_000):
    '''
    A generator to make a sample distribution from the elemnts of sample via bootstrapping

    Params
    ------
    sample: a list or series of values in the sample
    simulation_size: int

    Yields
    ------
    A single sample mean

    Usage
    -----

    pd.Series(gen_sample_distribution(sample, simulation_size=100_000)) 
    This will be a Pandas Series of length give n by simulation_size, 
    with each entry being the sample     mean from a single bootstrap sampling wihth replacemnt

    '''
    from numpy.random import randint

    for i in range(simulation_size):
        n = len(sample)
        sample_mean = sample[randint(n, size=n)].mean()
        yield sample_mean
