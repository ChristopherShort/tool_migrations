"""
A set of utilities to manipulate ABS Australian Demography tables (eg3101, 3102, 3101016)
"""
# from pathlib import Path

import pandas as pd

# import numpy as np
# import matplotlib as mpl
# from IPython.core.display import HTML, display_html

from chris_utilities import read_abs_data, read_abs_meta_data


def series_id_3101():
    series_id = {
        "births": "A2133244X",
        "deaths": "A2133245A",
        "natural_increase": "A2133252X",
        "interstate_arrivals": "A2133246C",
        "interstate_departures": "A2133247F",
        "overseas_arrivals": "A2133248J",
        "overseas_departures": "A2133249K",
        "net_permanent_and_long_term_movement": "A2133253A",
        "migration_adjustment": "A2133250V",
        "net_overseas_migration": "A2133254C",
        "estimated_resident_population": "A2133251W",
    }
    return series_id


def component_shares_between_dates(df):
    """
    Calculate the nom and natural contribution to population growth over the period covered
    by the dataframe.

    Parameters
    ----------
    df: a dataframe of ABS 3101, with column names already cleaned
        (ie lower cased, and joined with '_')

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
        f"Population increased {pop_delta * 1000:,.0f} ({pop_deta_pct_increase:.0%}) people.\n"
    )

    print(
        f"{nom_share:.1%} from NOM, {natural_increase_share:.1%} from natural increase."
    )
    return


def annual_population_components(df, month=6):
    """
    Calculate annual nom and natural increase components over the period covered by a 3101 dataframe.

    Parameters
    ----------
    df: a dataframe of ABS 3101, with column names already cleaned
        (ie lower cased, and joined with '_')

    Returns:
    a dataframe
    """

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

    population = (
        population.assign(NI_and_NOM=lambda x: x[["natural_increase", "net_overseas_migration"]].sum(axis=1))
    )
    
    # adjust NOM and natural increase to be correct levels of ERP - apportion intercensal equally
    nom_intercensal_NOM_share = population.net_overseas_migration / population.NI_and_NOM
    
    population = (population
        .assign(nom_adj=lambda x: nom_intercensal_NOM_share * x.ERP_flow)
        .assign(natural_increase_adj = lambda x: (1-nom_intercensal_NOM_share) * x.ERP_flow)
    ) 

    return population


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


def tidy_components(data_folder, filenames):
    """
    Extract the components from 310102 (TABLE 2. Population Change, Components) and
    from 3101016a&b (TABLE 16A/B. Interstate Arrivals/Departures)
    Return a tidy dataframe
    """

    def gen_components(data_folder):
        for filename in filenames:
            meta = read_abs_meta_data(data_folder, filename)

            tidy_labels = (
                meta.loc["Description"]
                .str.split(";", expand=True)
                .rename(columns={0: "component", 1: "state"})
                .assign(component=lambda x: x.component.str.strip())
                .assign(state=lambda x: x.state.str.strip())
                .drop(columns=[2])
            )

            components = (
                read_abs_data(data_folder, filename)
                .rename_axis(columns="labels", index="date")
                .unstack()
                .rename("value")
                .reset_index()
                .set_index("labels", drop=True)
                .assign(state=lambda x: x.index.map(tidy_labels.state))
                .assign(component=lambda x: x.index.map(tidy_labels.component))
            )

            if filename == "310102.xls":
                # remove 'Change over previous quarter' rows
                idx = components.component == "Change Over Previous Quarter"
                components = components[~idx]

            yield components

    components = pd.concat(gen_components(data_folder), sort=False)
    components = components[["date", "state", "component", "value"]]

    return components


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
        raise ValueError ("asgs_name column not in dataframe")

    idx_seq = ( 
        (df.asgs_name == 'Greater Brisbane') |
        (df.asgs_name == 'Sunshine Coast') | 
        (df.asgs_name == 'Gold Coast') |
        (df.asgs_name == 'Toowoomba')
    )

    return idx_seq