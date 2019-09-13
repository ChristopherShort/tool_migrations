""" Generate population summaries at State, GCCSA and SA4 by region name:
  * Population 2018
  * 1 year growth (levels and %)
  * NI (levels and share of growth)
  * NOM (levels and share of growth)
  * NIM (levels and share of growth)
  * Average  n-year growth rate
 
 Use 32180ds0001_2017-18.parq as it has sub state values for first 5 items
 Use 3218.parquet for n-year growth
"""

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib as mpl
import chris_utilities as cu
import nom

data_folder = Path(Path.home() / 'Documents/Analysis/Australian economy/Data/ABS')
data_folder_stock = Path(Path.home() / 'Documents/Analysis/Australian economy/Data/Stock')
data_folder_nom = Path(Path.home()/"Documents/Analysis/Australian economy/Data/NOM unit record data/Traveller Characteristics Parquet/")

capitals_names = {
    "Australian Capital Territory": "Canberra",
	"Greater Adelaide": "Adelaide",
    "Greater Brisbane": "Brisbane",
    "Greater Darwin": "Darwin",
    "Greater Hobart": "Hobart",
    "Greater Melbourne": "Melbourne",
    "Greater Perth": "Perth",
    "Greater Sydney": "Sydney"
}

capitals_order = [
    "Sydney",
    "Melbourne",
    "Brisbane",
    "Adelaide",
    "Perth",
    "Hobart",
    "Darwin",
    "Canberra"
]

components_order = ["natural", "nom", "nim"]

def make_summary(state, state_name, year_start="2013", year_end = "2018"):
    """
    """

    col_order = ["erp_2017", "erp_2018", "erp_delta_levels"] + components_order
    
    state_summary = add_growth(state.sum(), state_name)

    gccsa = state.groupby("gccsa_name")[col_order].sum()
    gccsa_summary = pd.concat(add_growth_by_group(gccsa), sort=False)

    sa_4 = state.groupby("sa4_name")[col_order].sum()
    sa_4_summary = pd.concat(add_growth_by_group(sa_4), sort=False)

    summary = pd.concat([state_summary, gccsa_summary, sa_4_summary], sort=False)
    
    summary = n_year_growth_rate(summary, year_start, year_end)

    summary = summary.rename(index = {"growth": "growth (%)", "share": "share (%)"})

    return summary


def add_growth(S, region_name):
    """
    Add a row containing the share of population change by component
    Add a row contianing the population growth rate and contributions to the growth rate by components
    TODO: ensure region selected is correct: SA2 vs SA4 etc for given region_name
    """

    #S is a series - convert to dataframe
    df = (S.to_frame().T
             .rename(columns = {"erp_delta_levels": "growth"}, index={0:region_name})
             .T
             .assign(share = 0)
             )
    df.loc["growth", "growth"] = df.loc["growth", region_name] / df.loc["erp_2017", region_name] * 100

    for measure in components_order:
        df.loc[measure, "share"] = df.loc[measure, region_name] / df.loc["growth", region_name] * 100
        df.loc[measure, "growth"] = df.loc[measure, "share"] /100 *  df.loc["growth", "growth"]

    # df[region_name] = df[region_name].astype(int)

    return df.T.drop(columns="erp_2017").loc[[region_name, "growth", "share"]]


def add_growth_by_group(region):
    """
    """
    for row_number in range(len(region)):
        yield(add_growth(region.iloc[row_number], region.iloc[row_number].name))


def n_year_growth_rate(df, year_start="2013", year_end = "2018"):
    """
    Loop over the summary dataframe and calculate growth rate only for regions, not share, etc
    """
    erp = pd.read_parquet(data_folder / "3218.parquet")
    
    col_name = f"{int(year_end) - int(year_start)}_year_growth_rate"
    
    df[col_name] = 0
    
    for region_name in df.index.array:
        if (region_name == "growth") | (region_name == "share"):
            continue
        idx =  (erp.asgs_name == region_name)
        df.loc[region_name, col_name] = cu.cagr(erp[idx]
                                                    .groupby('date')
                                                    .erp.
                                                    sum()
                                                    [year_start:year_end]
                                               )
    
    
    return df


def national(erp):
    idx = erp.regiontype == "AUS"
    return (erp[idx][["date", "erp"]]
            .set_index("date")
            .rename(columns={"erp": "Australia"})
    )


def capitals_levels(erp, totals=True):

    def add_all(df, totals):
        if totals:
            return df[capitals_order + ["All"]]
        else:
            return df[capitals_order]
    
    idx = (
            (erp.regiontype == "GCCSA") &
           (
               (erp.asgs_name.str.contains('Greater'))| (erp.asgs_name.str.contains('Australian Capital'))
           )
        )
    return (erp[idx]
            .pivot_table(index='date', columns='asgs_name', values='erp')
            .rename(columns=capitals_names)
            .assign(All = lambda x: x.sum(axis=1))
            .pipe(add_all, totals)
    )


def SUA(data_folder=data_folder, fname = '32180ds0003_2008-18.xls', keep_asgs_code=True):
    sua = (pd
            .read_excel(data_folder / fname,
                    sheet_name='Table 1',
                    skiprows=6,
                    )
            .rename(columns= {'Unnamed: 0': 'asgs_code',
                                'Unnamed: 1': 'sua'
                            })
            .drop(columns=['Unnamed: 13', '2017-2018', 'Unnamed: 15'])
            .dropna(thresh=8)
        )

    sua = sua.iloc[2:]

    ## Remove 'Not in any Significant' state areas
    # idx = sua.sua.str.contains('Not in any Significant')
    # sua = sua[~idx]

    sua = sua.set_index(["asgs_code", "sua"]).T
    sua.index = pd.to_datetime(sua.index.astype(str) + "-06-30")

    if keep_asgs_code:
        return sua
    else:
        return sua.droplevel(0, axis=1)


    return sua


def SEQ(data_folder=data_folder, fname="3129.parquet"):
    erp = pd.read_parquet(data_folder / "3218.parquet")

    idx_seq = (
        (erp.asgs_name == "Greater Brisbane") |
        ((erp.asgs_name == "Sunshine Coast") &  (erp.regiontype == "SA4"))| 
        ((erp.asgs_name == "Gold Coast") &  (erp.regiontype == "SA4")) | 
        ((erp.asgs_name == "Toowoomba") & (erp.regiontype == "SA4"))
   )

    return (erp[idx_seq]
            .pivot(index="date", columns="asgs_name", values="erp")
            .assign(SEQ=lambda x: x.sum(axis="columns"))
    )


def get_stock_data(fname="stock_today.parq",
    datafolder=data_folder_stock,
    monthly=None,
    resample="A",
    visitor=False):
    """Return stock dataframe as either monthly data or average year-ending data
    
    Parameters
    ----------
    fname : str, optional
        Home Affairs stock data file, by default "stock_today.parq"
    datafolder : str or Path, optional
        data folder, by default data_folder_stock
    monthly : Int or None, optional
        if None, return all monthly data, else return year-ending to month given by int, by default None
    resample : str, optional, by default "A"
        periodicity for resampling, A, Q etc
    visitor : bool, optional
        include visitor data , by default False
    
    Returns
    -------
    dataframe
        dataframe of monthly visa category stock,  average stock for year ending basis
    """
    df = pd.read_parquet(datafolder / fname)
    
    stock = (df.pivot_table(index="snapshot_date",
        columns="visa_holder_category",
        values="visa_holders_total",
        aggfunc='sum'
        )
        .pipe(cu.clean_column_names, other_text_to_remove=" visa holders")
    )
    if not visitor:
        stock = stock.drop(columns="visitor")
    
    stock = stock.assign(all=lambda x: x.sum(axis=1))

    if monthly is True:
        return stock
    elif isinstance(monthly, int):
        ### only implemented for annual, adjust to be if resample="A", when "Q" added etc
        return stock.resample(cu.time_delta_rule(monthly)).mean()
    else:
        ### return original data, monthly=None
        return df


def population_by_age(fname="310105x.feather"):
    return pd.read_feather(data_folder / fname)


def get_nom(data_folder=data_folder_nom):
    nom_fields = [
        'person_id',
        'duration_movement_date',
        'visa_subclass',
        'net_erp_effect',
        'age'
    ]


    # Define the groupby
    def gen_get_students(df_fields):
        for df in df_fields:
            yield df  #.query('visa_subclass=="444"')
            #TODO: how to pass a query, or nul and get all

    # establish the generators
    file_paths = nom.gen_nom_files(data_folder, abs_visagroup_exists=False)
    df_fields = nom.gen_nom_fields(file_paths, nom_fields)
    df_students = gen_get_students(df_fields)

    df = (pd
        .concat(df_students, axis=0, ignore_index=True, sort=False)
        .rename({'duration_movement_date': 'date'}, axis='columns')
    )

    return df
