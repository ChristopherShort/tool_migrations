"""
Functions for helping CPOP profiles.


"""
import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.ticker as ticker

from matplotlib import pyplot as plt
import statsmodels.api as sm

import components
import file_paths

import chris_utilities as cu


ABS_FOLDER = file_paths.abs_data_folder
DICT_FOLDER = file_paths.dict_data_folder

def get_kde(df, tile=True, year=None):
    """create a kernel density object of population ages

    Parameters
    ----------
    df : dataframe
        population ages: condtains two columns: age, value.  The 'value' column is count of ages
    year : string, optional
        a date of for yyyy-mm-dd (2019-06-30) to be coerced to a date object, by default None

    Returns
    -------
    kde
        a statsmodels kernel density object of ages
    """
    #tile age by the count value
    if tile:
        population_age = tile_age(df, year=year)
    else:
        population_age = df.age

    kde_population_age = sm.nonparametric.KDEUnivariate(population_age.values)
    kde_population_age.fit()

    return kde_population_age, population_age


def tile_age(df, year=None):
    """tile age by the count value in df

    Parameters
    ----------
    df : dataframe
        population ages: condtains two columns: age, value.  The 'value' column is count of ages
    year : [string, optional
        a date of for yyyy-mm-dd (2019-06-30) to be coerced to a date object, by default None

    Returns
    -------
    series
        values are ages tiled (repeated) - for use in a histogram or kde
    """

    if year:
        idx = df.date == pd.to_datetime(year)
        population_age = df[idx].age.repeat(df[idx].value).reset_index(drop=True)
    else:
        population_age = df.age.repeat(df.value).reset_index(drop=True)
    
    return population_age


def clean_ticks_spines(
    ax, 
    fixed_locator=[0, 0.02, 0.04, 0.06],
    xmax= 100,
    ):
    
    for spine in ax.spines:
        if spine == "bottom":
            continue
        ax.spines[spine].set_visible(False)
    
    ax.spines['bottom'].set_position(('data', 0.0))

    if fixed_locator:
        # ax.yaxis.set_major_locator(plt.MaxNLocator(3))
        ax.yaxis.set_major_locator(ticker.FixedLocator(fixed_locator))
    
    ax.grid(True, axis='y', alpha=0.2)
    
    ax.tick_params(axis='y', length=0)
    
    if xmax:
        ax.set_xlim(0,xmax)
    ax.set_ylim(0, None)
    
    return


def get_x_y(population_age, kde, scaled=True, slice=None):
    
    if scaled:
        population_count = len(population_age)
    else:
        population_count = 1

    if slice is None:
        slice = np.s_[:]

    x = kde.support[slice]
    y = (kde.density * population_count)[slice]
    
    return x, y


def get_concordance_dictionary():

    # file_path = DICT_FOLDER / "ABS - Visacode3412mapping.xlsx"

    # concordance = pd.read_excel(file_path, sheet_name="concordance")

    concordance = (pd
    .read_csv(file_paths.profiles_folder / "data/Final Concordance - w provisional.txt", sep="\t")
    .assign(vsc = lambda x:x.VISAP.str.strip())
    .drop(columns=["VISAP"])
    .convert_dtypes()
          )

    #work around to ensure vsc's are strings
    # concordance.vsc = [str(i) for i in concordance.vsc.array]
    # concordance = concordance.convert_dtypes()

    concordance_dict = {visa_group:list(group.vsc) 
        for visa_group, group in concordance.groupby("Hierarchy3") 
            if visa_group not in ["unknown", "australian",] #visitor, "bridging", "humanitarian"]
    }

    return concordance_dict


    def get_age_by_direction(df, direction = None):


        if direction == "arrivals":
            idx = nom.net_erp_effect > 0
        elif direction == "departures":
            idx = nom.net_erp_effect < 0
        else:
            raise ValueError(f"Chris: direction can only be 'arrivals' or 'departures', you passed: '{direction}'")
            
        gender = {1: "male", 2: "female"}
        
        grouper = ["date", "age", "sex"]


        return (df[idx]
            .groupby(grouper)
            .net_erp_effect
            .sum()
            .unstack(grouper[1:])
            .resample("A-Jun")
            .sum()
            .stack(grouper[1:])
            .round()
            .astype(int)
            .rename("value")
            .reset_index()
            .assign(direction = direction)
            .assign(gender = lambda x: x.sex.map(gender))
            .drop(columns=["sex"])
            .convert_dtypes()
    )
        
        # # for col in nom_by_age.select_dtypes("object"):
        # #     nom_by_age[col] = nom_by_age[col].astype("string")
        
        # return nom_by_age


def invert_concordance(groups): #=visa_group_dict.items()
    vsc_to_group_dict = {}
    for group, vsc_list in groups:
        for vsc in vsc_list:
            if vsc in vsc_to_group_dict.keys():
                raise ValueError("Chris - duplicate visa subclass")
            vsc_to_group_dict[vsc] = group
    
    return vsc_to_group_dict


def ha_visa_dict():
    visas = (pd.
    read_excel(
        io=file_paths.profiles_folder / "data/MPO for 2017-18 and 2016-17.xlsx", 
        sheet_name="MPO 2017-18 (2)", 
        skiprows=3,
        dtype={"vsc":"string"}
    )
      .ffill()
      .convert_dtypes()
      .pipe(cu.clean_column_names)
     )

    visa_dict = {}
    for stream, program in visas.groupby(["visa_program_stream", "visa_category"]):
        visa_dict[stream] = list(program.vsc.array)
    
    return visa_dict