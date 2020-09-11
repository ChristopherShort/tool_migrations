"""
Functions for managing covid scenarios for given NOM output.

Designed so scnearios can be done sequentially - by piping through additional scenarios
"""

import IPython
import pandas as pd
import matplotlib as mpl
from matplotlib import pyplot as plt
import calendar
from IPython.display import display, HTML

from nom_forecast import remove_nom_levels, add_nom, add_nom_4d, get_nom_forecast
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
        # print(direction, visas)

        # df.loc[start:stop, (visas, direction)] = (df
        #         .loc[start:stop, (visas, direction)] * percentage_change_inverse
        #     )
        
        # display(df.loc[start:stop, (direction, visas)])

        df.loc[start:stop, (direction, visas)] = (df
                .loc[start:stop, (direction, visas)] * percentage_change_inverse
            )

    return df


def make_scenario_4d(df, start, stop, adjusted_visas, percentage_change=100):
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
    # df = remove_nom_levels(df)

    percentage_change_inverse = (100 - percentage_change) / 100

    for direction, visas in adjusted_visas.items():
        df.loc[start:stop, (direction, visas)] = (df
                .loc[start:stop, (direction, visas)] * percentage_change_inverse
            )

    return df


def plot_scenario_comparison(df, scenario_name, month="June", include_reference=True, title=None, scenario_label=None):
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
        display(df[idx]["2019":"2024"])
        df[idx]["2019":"2024"].assign(scenario = scenario_name).to_clipboard()
        
        fig, ax = plt.subplots()

        df.loc[:"2019-12", "original"].plot(ax=ax, label="History", )
        
        if include_reference:
            df.loc["2019-12" :, "original"].plot(ax=ax,  color="C0", alpha=1, ls=("dashed"), label="Reference forecast")
        
        df.loc["2019-12":, "scenario"].plot(ax=ax, color="C1", alpha=0.75, ls=("dashed"), label=scenario_label)
        

        if df.min().min() >= -1: ### account for digital maths having very small negative numbers
            ax.set_ylim([0, None])
        else:
            ax.set_ylim([None, None])
        
        
        ax, ax2 = adjust_chart(ax)

    

        if title:
            ax.set_title(title, fontsize=10)
        
        # ax.legend(loc="upper center", ncol=3, fontsize=9)
        ax.legend(bbox_to_anchor=(0., 0.905, 1., 0), loc='lower center',ncol=3)

        return fig, ax


def get_comparison(forecast, scenario, visa_group="nom", direction = "nom"):
    """Creates a dataframe with nom end of year values from the dataframes and calculates difference
    
    Parameters
    ----------
    forecast : dataframe
        nom by date by (visa_group, direction)
    scenario : dataframe
        scenario by date by (visa_group, direction)
    """

    # create rolling end of year nom from each dataframe
    forecast_nom_eoy = forecast[(visa_group, direction)].rolling(12).sum().dropna().rename(f"original")
    scenario_nom_eoy = scenario[(visa_group, direction)].rolling(12).sum().dropna().rename(f"scenario")

    return (pd
        .concat([forecast_nom_eoy, scenario_nom_eoy], axis=1)
        .dropna()
        .assign(difference = lambda x: x.original - x.scenario)
    )


def get_comparison_2(reference, scenario):
    """Creates a dataframe with nom end of year values from the dataframes and calculates difference
    
    Parameters
    ----------
    forecast : dataframe
        nom by date by (visa_group, direction)
    scenario : dataframe
        scenario by date by (visa_group, direction)
    """

    reference_nom_eoy = reference.rename("original").dropna()
    scenario_nom_eoy = scenario.nom.sum(axis=1).rolling(4).sum().dropna().rename("scenario")

    return (pd
        .concat([
            reference_nom_eoy, 
            scenario_nom_eoy
            ]
            , axis=1)
        # .dropna()
        .assign(difference = lambda x: x.original - x.scenario)
    )


def add_nom_4d(df):
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

    #TODO - don't assume direction is always second
    # TODO - generalise - find "direction" column header make it first 
    # ensure no NOM elements
    # df = remove_nom_levels(df)

    ## Create nom for each visa grouop
    # nom_monthly = df.swaplevel(axis=1).arrivals - df.swaplevel(axis=1).departures
    # nom_monthly.columns = pd.MultiIndex.from_product([nom_monthly.columns, ["nom"]])
    # df = pd.concat([df, nom_monthly], axis=1).sort_index(axis=1)
    nom_monthly = df.arrivals - df.departures
    nom_monthly.columns = (pd
        .MultiIndex
        .from_product(
            [["nom"], nom_monthly.columns.levels[0], nom_monthly.columns.levels[1]], 
            names = ["direction", "visa_group", "state"]
            )
    )
    df = pd.concat([df, nom_monthly], axis=1) # .sort_index(axis=1)

    ## Create nom total
    # nom_total_monthly = df.sum(axis=1, level=1)
    # nom_total_monthly.columns = (pd
    #     .MultiIndex
    #     .from_product([["nom"], nom_total_monthly.columns])
    # )
    # nom_total_monthly = df.sum(axis=1, level=1)
    # nom_total_monthly.columns = (pd
    #     .MultiIndex
    #     .from_product([["nom"], nom_total_monthly.columns])
    # )

    return df #pd.concat([df, nom_total_monthly], axis=1)


    def forecast_adjustment(df, date_, visa_, reduction):
        """[summary]
        Adjust NOM forecast by a fixed level (eg reduce Skilled by 10,000
        Adjust all sub levels by allocating on a share basis
        Parameters
        ----------
        df : [type]
            [description]
        date_ : [type]
            [description]
        visa_ : [type]
            [description]
        reduction : [type]
            [description]

        Returns
        -------
        [type]
            [description]
        """
        state_allocation_reduction = (df.loc[date_, visa_]
                            .divide(
                                (df.loc[date_, visa_].sum(axis=1)), axis="rows"
                            )
                            * reduction
                            )
        return df.loc[date_, visa_].subtract(state_allocation_reduction, axis="rows").values