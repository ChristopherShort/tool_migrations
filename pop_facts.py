""" Generate population summaries at State, GCCSA and SA4 by region name:
  * Population 2018
  * 1 year growth (levels and %)
  * NOM (levels and share of growth)
  * NIM (levels and share of growth)
  * NI (levels and share of growth)
  * Average 5 year growth
 
 Use 32180ds0001_2017-18.parq as it has sub state values for first 5 items
 Use 3218.parquet for 5 year growth
"""

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib as mpl
import chris_utilities as cu

data_folder = Path(Path.home() / 'Documents/Analysis/Australian economy/Data/ABS')


def add_growth(df, region_name):
    """
    TODO: ensure region selected is correct: SA2 vs SA4 etc for given region_name
    """
    df = (df.to_frame().T
             .rename(columns = {"erp_delta_levels": "growth"}, index={0:region_name})
             .T
             .assign(share = 0)
             )
    df.loc["growth", "share"] = df.loc["growth", region_name] / df.loc["erp_2017", region_name] * 100

    for measure in ["natural", "nim", "nom"]:
        df.loc[measure, "share"] = df.loc[measure, region_name] / df.loc["growth", region_name] * 100

    df[region_name] = df[region_name].astype(int)

    return df.T.drop(columns="erp_2017")

def add_growth_by_group(region):
    """
    """
    for row_number in range(len(region)):
        yield(add_growth(region.iloc[row_number], region.iloc[row_number].name))


def get_growth_rate(df, year_start="2013", year_end = "2018"):
    """
    """
    erp = pd.read_parquet(data_folder / "3218.parquet")
    
    col_name = "growth_rate_5_years"
    
    df[col_name] = 0
    
    for region_name in df.index.array:
        if region_name == "share":
            continue
        idx =  (erp.asgs_name == region_name)
        df.loc[region_name, col_name] = cu.cagr(erp[idx]
                                                    .groupby('date')
                                                    .erp.
                                                    sum()
                                                    [year_start:year_end]
                                               )
    
    
    return df

def make_summary(state, state_name, year_start="2013", year_end = "2018"):
    """
    """
    state_summary = add_growth(state.sum(), state_name)

    gccsa = state.groupby("gccsa_name")["erp_2017", "erp_2018", "erp_delta_levels", "nom", "nim", "natural"].sum()
    gccsa_summary = pd.concat(add_growth_by_group(gccsa))

    sa_4 = state.groupby("sa4_name")["erp_2017", "erp_2018", "erp_delta_levels", "nom", "nim", "natural"].sum()
    sa_4_summary = pd.concat(add_growth_by_group(sa_4))

    summary = pd.concat([state_summary, gccsa_summary, sa_4_summary])

    summary = summary.assign(growth_rate_5_years = 0)
    
    summary = get_growth_rate(summary, year_start, year_end)
    
    summary.index = summary.index.str.replace("share", "")

    return summary