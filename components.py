"""Geographical extracts of natural increase, nom and nim
"""

from pathlib import Path
import pandas as pd

import data

from data import read_abs_data, read_abs_meta_data

DATA_ABS_PATH = Path.home() / "Documents/Analysis/Australian economy/Data/ABS"


def read_3101():
    series_id = data.series_id_3101()
    return data.read_abs_data(series_id=series_id)


def nom(df=None):
    """Exract NOM data
    
    Parameters
    ----------
    df : [type], optional
        [description], by default None
    """

    if df is None:
        df = read_3101()

    return df.net_overseas_migration


def nom_year_ending(df_nom=None):
    """Return year ending nom
    
    Parameters
    ----------
    nom : [type], optional
        [description], by default None
    """

    if df_nom is None:
        df_nom = read_3101()

    return df_nom.net_overseas_migration.rolling(4).sum().dropna()
        

def nom_year_ending_annual(df_nom=None, quarter="A-Jun"):
    """Return year ending for a given quarter
    
    Parameters
    ----------
    df_nom : Pandas series, optional
        contains nom in sub-annual data
    """
    if df_nom is None:
        df_nom = nom()

    nom_annual = df_nom.resample(quarter).sum()

    # remove last year if not full year (ie nom last period == quarter parameter)
    if df_nom.index[-1].strftime("%b") != quarter[-3:]:
        nom_annual = nom_annual.iloc[:-1]

    return nom_annual


def component_shares_between_dates(df):
    """
    Calculate the nom and natural contribution to population growth over the period covered
    by the dataframe.

    Parameters
    ----------
    df: a dataframe of ABS 3101, with column names already cleaned
        (ie lower cased, and joined with "_")

    Returns:
    None but prints out a summary of population increase and component contributions
    """

    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Chris - the dataframe does not have a time series index")

    idx_erp_start = df.first_valid_index()

    # Sum of components must start from 2nd period - components in first period
    # contribute to the start ERP only
    idx_component_start = df.iloc[1:].first_valid_index()
    idx_erp_end = df.last_valid_index()

    pop_delta = (
        df.loc[idx_erp_end].estimated_resident_population
        - df.loc[idx_erp_start].estimated_resident_population
    )

    pop_deta_pct_increase = (
        pop_delta / df.loc[idx_erp_start].estimated_resident_population
    )

    nom = df.loc[idx_component_start:].net_overseas_migration.sum()
    natural_increase = df.loc[idx_component_start:].natural_increase.sum()

    components = nom + natural_increase
    nom_share = nom / components
    natural_increase_share = natural_increase / components

    print(f"Between {idx_erp_start:%Y-%m-%d} and {idx_erp_end:%Y-%m-%d}:\n")

    print(
        f"Population increased {pop_delta * 1000:,.0f} ({pop_deta_pct_increase:.1%}) people.\n"
    )

    print(
        f"{nom_share:.1%} from NOM, {natural_increase_share:.1%} from natural increase."
    )
    return


def annual_population_components(df=None, month=6):
    """
    TODO: read in 3101 rather than passing in as df
    
    Calculate annual nom and natural increase components over the period covered by a 3101 dataframe.

    Parameters
    ----------
    df: a dataframe of ABS 3101, with column names already cleaned
        (ie lower cased, and joined with "_")

    Returns:
    a dataframe
    """

    if df is None:
        df = read_3101()

    ERP = df[df.index.month == month].estimated_resident_population

    ERP_flow = ERP.diff()
    ERP_flow.name = "ERP_flow"

    NOM = df.net_overseas_migration.rolling(4).sum()
    NOM = NOM[NOM.index.month == month]

    natural = df.natural_increase.rolling(4).sum()
    natural = natural[natural.index.month == month]

    population = pd.concat([ERP, ERP_flow, natural, NOM], axis=1)

    ## Adjust nom for period 1996 through 2005
    # population.loc["1996":"2005", "net_overseas_migration"] = population.loc["1996":"2005", "net_overseas_migration"] * 1.25

    population = population.assign(
        NI_and_NOM=lambda x: x[["natural_increase", "net_overseas_migration"]].sum(
            axis=1
        )
    )

    # adjust NOM and natural increase to be correct levels of ERP - apportion intercensal equally
    nom_intercensal_NOM_share = (
        population.net_overseas_migration / population.NI_and_NOM
    )

    population = population.assign(
        nom_adj=lambda x: nom_intercensal_NOM_share * x.ERP_flow
    ).assign(
        natural_increase_adj=lambda x: (1 - nom_intercensal_NOM_share) * x.ERP_flow
    )

    return population





