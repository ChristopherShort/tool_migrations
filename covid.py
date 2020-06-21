"""
Functions for managing covid scenarios for given NOM output.

Designed so scnearios can be done sequentially - by piping through additional scenarios
"""

import pandas as pd
import matplotlib as mpl
from matplotlib import pyplot as plt
import calendar

from nom_forecast import remove_nom_levels, add_nom, get_nom_forecast
from chris_utilities import adjust_chart


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


def plot_scenario_comparison(df, scenario_name, month="June"):
        """display comparison nom comparison, 
           place comparsion in clipboard
           plot scenario against comparison
        
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
        
        #
        
        # show NOM scenario comparison

        cal_month = dict((v, k) for k, v in enumerate(calendar.month_name))

        if month not in calendar.month_name:
            raise ValueError(f'{month} is not the name of a capitalized month, eg "June"')
    
        # for k, v in cal_month.items():
        #     print(k,v)
        #     cal_month = dict((v, k) for k, v in enumerate(calendar.month_name))

        # print(cal_month[month])

        idx = df.index.month == cal_month[month]

        print("Calendar year differences")
        display(df[idx]["2019":"2022"])
        df[idx]["2019":"2022"].assign(scenario = scenario_name).to_clipboard()
        
        fig, ax = plt.subplots()

        if df.min().min() >= -1: ### account for digital maths having very small negative numbers
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

