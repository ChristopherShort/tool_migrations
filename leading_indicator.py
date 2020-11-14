from pathlib import Path
import pandas as pd
import file_paths

import nom_forecast as nomf


def get_leading_indicator(filepath, sheet_name
    ):
    """Extract leading indicator data

    Parameters
    ----------
    file_name : filepath
        pointer to location in the ABS data folder
    sheet_name : sheetname
        

    """

## Dec 2019 order
    # 'australian_citizen',
    # 'new_zealand_citizen',
    # 'family',
    # "humanitarian",
    # 'skill_permanent',
    # 'other_permanent',
    # 'other_temporary',
    # 'skill_temporary',
    # 'student',
    # 'visitor',
    # #     'working_holiday',
    # 'temporary_work',
    # 'unclassified',

    abs_visas = [
    'australian_citizen',
    'new_zealand_citizen',
    'family',
    "humanitarian",
    'skill_permanent',
    'other_permanent',
    'skill_temporary',
    'student',
    'visitor',
    'working_holiday',
    'other_temporary',
    'unclassified',
    ]

    col_names = pd.MultiIndex.from_product([["arrivals", "departures"], abs_visas], names=["direction", "visa_group"])

    le = (pd
        .read_excel(filepath, 
            sheet_name=sheet_name,
            skiprows=9,
            skipfooter = 2,
            index_col=0, 
            header=None,
            usecols="A:Y",
            parse_dates=True
            )
        .dropna(axis='index', how='all')
        )

    le.columns = col_names
    le.index.name = 'date'

    ### Convert "June Quarter 2009", "September Quarter 2009"... to DateTime indexdddd
    le.index = pd.to_datetime(le.index.str.replace(" Quarter", "")) + pd.offsets.MonthEnd(0)

    return (nomf.add_nom(le.swaplevel(axis=1)))
