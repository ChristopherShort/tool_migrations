"""
A set of utilities to manipulate ABS Australian Demography tables (eg3101, 3102, 3101016)
"""
from pathlib import Path

import pandas as pd

# import numpy as np
# import matplotlib as mpl
# from IPython.core.display import HTML, display_html

from data import read_abs_data, read_abs_meta_data


DATA_ABS_PATH = Path.home() / "Documents/Analysis/Australian economy/Data/ABS"


def check_max(df):
    """
    Append to df columns containing the maximum, date of the maximum, rank of the current value
    """

    df_max = pd.concat(
        [
            df.iloc[-1].rename("this_period"),
            df.max().rename("largest_ever"),
            df.idxmax().rename("date_of_max"),
            df.rank(ascending=False).iloc[-1].astype(int).rename("current_rank"),
            (df.iloc[-1] >= df.max()).rename("is_maximum"),
        ],
        axis=1,
    ).rename_axis("components")

    df_max.is_maximum = df_max.is_maximum.astype(str).replace("False", "")

    # comparison_this_period = df.iloc[-1] >= df_max.maximum
    #                             .rename('is_maximum')
    #                           )

    # df_max_period = (pd
    #                  .concat([df_max, comparison_this_period], axis=1)
    #                  .rename(columns={0: 'is_maximum'})
    #                  )

    # if df_max_period.maximum.any():
    #     idx = df_max_period.Maximum
    #     df_max_period['this_period'] = np.nan
    #     df_max_period.loc[~idx, 'this_period'] = df.iloc[-1].loc[~idx]

    # column_order = ['this_period', 'current_rank',
    #                 'maximum', 'max date',	'is_maximum',
    #                 ]

    # df_max_period = df_max_period[column_order]

    return df_max


def make_dependency_df(data_df, bins=[0, 14, 64, 101]):

    # Use 20 as the age dependency break
    binned = pd.Series(
        data=pd.cut(
            data_df.index,
            bins=bins,
            labels=["Youth", "Working_age", "Older_age"],
            include_lowest=True,
        ),
        index=data_df.index,
    )

    dependency = data_df.groupby(binned).sum().T

    dependency.columns = dependency.columns.add_categories("aged_dependency")
    dependency.columns = dependency.columns.add_categories("working_age_share")
    dependency["aged_dependency"] = dependency.Working_age / dependency.Older_age

    dependency["working_age_share"] = (
        dependency.Working_age / dependency[["Youth", "Working_age", "Older_age"]]
    )
    return dependency


def seq_idx(df):
    """
    Provide boolean indexer for South East Queensland

    Parameters:
    -------
    df : dataframe
        a population dataframe

    Raises
    ------
    ValueError
        if the dataframe does not contain an asgs_name column

    Returns
    -------
    idx_seq : boolean indexer   
        an indexer to Tudge's definition of SEQ.
    """

    if "asgs_name" not in df.columns:
        raise ValueError("asgs_name column not in dataframe")

    idx_seq = (
        (df.asgs_name == "Greater Brisbane")
        | (df.asgs_name == "Sunshine Coast")
        | (df.asgs_name == "Gold Coast")
        | (df.asgs_name == "Toowoomba")
    )

    return idx_seq
