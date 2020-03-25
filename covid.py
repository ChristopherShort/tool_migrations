"""
Functions for managing covid scenarios for given NOM output.

Designed so scnearios can be done sequentially - by piping through additional scenarios
"""

import pandas as pd
import matplotlib as mpl
from matplotlib import pyplot as plt

from chris_utilities import adjust_chart


def remove_nom_levels(df):
    """remove nom values at both level 0 (visa_group) and level 1(direction)
    
    Parameters
    ----------
    df : dataframe
        a NOM dataframe with multiindex columns ("abs_visa_group", "direction") by dates 
    
    """

    # TODO: check names of passed array - "abs_visa_group" & "direction"

    # remove the "nom total" group
    if "nom" in df.columns.get_level_values(level=0):
        df = df.drop(["nom"], axis=1, level=0)
    
    # remove nom for all visa groups
    if "nom"in df.columns.get_level_values(level=1):
        df = df.drop(["nom"], axis=1, level=1)
    
    return df


def make_scenario(df, start, stop, adjusted_visas, percentage_change=100):
    """
    Apply covid adjustment to current NOM (current may mean NOM as it is in a series of chained scenarios) 
    
    Parameters:
    -----------
    df: dataframe: current nom forecasts (by visa_group by direction)
    
    start: start date, str in YYYY or YYYY-MM format
    
    stop: end date, str in YYYY or YYYY-MM format
    
    adjusted_visas: a dictionary of direction and visas to adjust: {"arrivals", [visas], "departures", [visas]}
    
    ratio_change: the percentage change adjustment
    
    returns
    -------
    scenario: a dataframe of adjusted visas
    """

    if not 0 <= percentage_change <= 100:
        raise ValueError(
            f"percentage_change must range from 0-100%, you provided {percentage_change/100:.0%}"
        )

    current_nom = df.copy()

    # check nom not in df
    df = remove_nom_levels(df)

    percentage_change_inverse = (100 - percentage_change) / 100

    for direction, visas in adjusted_visas.items():
        # scenario = current_nom[visas]
        # scenario.loc[start:stop, (visas, direction)] = (percentage_change_inverse
        #     * scenario.loc[start:stop, (visas, direction)]
        # )
        df.loc[start:stop, (visas, direction)] = (df
            .loc[start:stop, (visas, direction)] * percentage_change_inverse
        )


    return df


def add_nom(df):
    """
    add nom to each visa group and append a total nom visagroup to the dataframe
    
    Parameters:
    -----------
    df : dataframe
    A nom dataframe with multiindex columns of visa_group by (arrivals, departures) - but no nom elements
    
    Returns:
    --------
    df : extended with nom for each visa_group plus total nom
    """
    # ensure no NOM elements
    df = remove_nom_levels(df)

    ## Create nom for each visa grouop
    nom_monthly = df.swaplevel(axis=1).arrivals - df.swaplevel(axis=1).departures
    nom_monthly.columns = pd.MultiIndex.from_product([nom_monthly.columns, ["nom"]])
    df = pd.concat([df, nom_monthly], axis=1).sort_index(axis=1)

    ## Create nom total
    nom_total_monthly = df.sum(axis=1, level=1)
    nom_total_monthly.columns = (pd
        .MultiIndex
        .from_product([["nom"], nom_total_monthly.columns])
    )

    return pd.concat([df, nom_total_monthly], axis=1)


def plot_scenario_comparison(df):
        """plot scenario against comparison
        
        Parameters
        ----------
        df : dataframe
            holding monthly values of original nom and scenario no
        
        Returns
        -------
        fig, ax objects of chart
        """
        # TODO: check df has two columns and monthly dates
        # TODO: improve chart layout 
        fig, ax = plt.subplots()

        if df.min().min() > 0:
            ylim = [0, None]
        else:
            ylim = [None, None]

        df.loc["2015":"2022", "nom_scenario"].plot(ax=ax, ylim=ylim, color="C0", alpha=0.75, ls=("dashed"))
        df.loc["2015" : "2022", "nom_original"].plot(ax=ax, ylim=ylim, color="C0", alpha=1, ls=("solid"))
        
        _ = adjust_chart(ax)

        return fig, ax


def get_comparison(forecast, scenario):
    """Creates a dataframe with nom end of year values from the dataframes and calculates difference
    
    Parameters
    ----------
    forecast : dataframe
        nom by date by (visa_group, direction)
    scenario : dataframe
        scenario by date by (visa_group, direction)
    """
    # create rolling end of year nom from each dataframe
    forecast_nom_eoy = forecast[("nom", "nom")].rolling(12).sum().dropna().rename("nom_original")
    scenario_nom_eoy = scenario[("nom", "nom")].rolling(12).sum().dropna().rename("nom_scenario")

    return (pd
        .concat([forecast_nom_eoy, scenario_nom_eoy], axis=1)
        .dropna()
        .assign(difference = lambda x:x.nom_original-x.nom_scenario)
                    )


def get_nom(nom_forecast_filepath, grouping=["date", "abs_visa_group", "direction"]):
    """Return current NOM forecast
    
    Parameters
    ----------
    nom_forecast_filepath : str/Path object
        filepath to file containing tidy version of nom forecasts
    grouping : list
        variables to group the nom data by (usually [])
    """

    return (pd
            .read_parquet("nom_new_covd_tidy.parquet")
            .set_index(grouping)
            .squeeze()
            .unstack(grouping[1:])
            .sort_index(axis=1)
     )