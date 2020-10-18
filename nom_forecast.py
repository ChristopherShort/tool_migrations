"""
Utilities for examining ABS NOM unit record
"""

import pickle
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib as mpl
from matplotlib import pyplot as plt


from IPython.display import display_html, display

from matplotlib.patches import Patch

from chris_utilities import adjust_chart

import file_paths


# the data storage
base_data_folder = file_paths.base_data_folder
abs_data_folder = file_paths.abs_data_folder
unit_record_folder = file_paths.unit_record_folder
individual_movements_folder = file_paths.individual_movements_folder
abs_nom_propensity = file_paths.abs_nom_propensity
abs_traveller_characteristics_folder = file_paths.abs_traveller_characteristics
grant_data_folder = file_paths.grant_data_folder
dict_data_folder = file_paths.dict_data_folder
program_data_folder = file_paths.program_data_folder


# local to current forecasting period folder
forecasting_data_folder = Path("data/forecasting")
forecasting_input_folder = forecasting_data_folder / "input"


### Utilities to read in raw ABS data:


def process_original_ABS_data(abs_original_data_folder, analysis_folder):
    """Process the SAS data, include removing previous preliminary parquet 
       and replace with final parquet, and add new preliminary parquet for latest quarter

    Parameters
    ----------
    abs_original_data_folder : Path ojbect
        SAS data directory
    analysis_folder : Path object
        ABS Traveller characteristics folder pat

    Returns
    -------
    None

    Raises
    ------
    ValueError
        Check ABS NOM files must commence with p or f
        This differentiates between preliminary and final NOM
        Raise error to advice user that RTS file name convention not in place
    """
    # TODO: read from the zip file rather than unzipped data

    # variables to convert to ints or strings
    ints_preliminary = [
        "person_id",
        "sex",
        "country_of_birth",
        "country_of_citizenship",
        "country_of_stay",
        "initial_erp_flag",
        "final_erp_flag",
        "duration_movement_sort_key",
        "nom_direction",
        "duration_in_australia_category",
        "count_of_movements",
        "initial_category_of_travel",
        "age",
        "status_flag",
        "reason_for_journey",
        "odb_time_code",
    ]

    ## For preliminary leave as floats: 'rky_val'

    ints_final = [
        "person_id",
        "sex",
        "country_of_birth",
        "country_of_citizenship",
        "country_of_stay",
        "initial_erp_flag",
        "final_erp_flag",
        "duration_movement_sort_key",
        "nom_direction",
        "duration_in_australia_category",
        "count_of_movements",
        "initial_category_of_travel",
        "age",
        "status_flag",
        "reason_for_journey",
        "odb_time_code",
        "net_erp_effect",
        "nom_propensity",
    ]

    # string vars are the same across preliminary and final
    string_vars = [
        "visa_group",
        "visa_subclass",
        "visa_applicant_type",
        "visa_stream_code",
        "stream_code_out",
        "state",
        "direction",
    ]

    date_times = ["Duration_movement_date"]

    ### For unzipped sas data filess files
    ### Requires both options - older folders may not have the zipped version
    # for abs_filepath in sorted(abs_original_data_folder.glob("*.sas7bdat")):
        # print(abs_filepath.stem)

        # df = pd.read_sas(abs_filepath, encoding="latin-1", format="sas7bdat").rename(
        #     columns=str.lower
        # )


    for abs_filepath in sorted(abs_original_data_folder.glob("*.sas7bdat")):
        print(abs_filepath.stem)

        df = pd.read_sas(abs_filepath, encoding="latin-1", format="sas7bdat").rename(
            columns=str.lower
        )
    # for zip_filename in sorted(abs_original_data_folder.glob("*.zip")):
    #     zipped_file = zipfile.ZipFile(zip_filename, 'r')

    #     # There's only expected to be one file in each zip
    #     if len(zipped_file.namelist()) != 1:
    #         raise ValueError("Chris: zipped file has more than one file...recode!")
    #     sasfile = zipfile.open(zipped_file.namelist()[0])
        
    #     print(sasfile.stem)

    #     df = pd.read_sas(sasfile, encoding="latin-1", format="sas7bdat").rename(
    #         columns=str.lower
    #     )

        ### need to fix all abs_filepath below

        # adjust datatypes and write out:

        # string vars are the same across preliminary and final
        for col in string_vars:
            df[col] = df[col].astype("category")

        # integer variables differ across final and preliminary data
        if abs_filepath.stem[0] == "p":  # preliminary NOM
            for col in ints_preliminary:
                df[col] = df[col].astype(int)

        elif abs_filepath.stem[0] == "f":  # final NOM
            for col in ints_final:
                df[col] = df[col].astype(int)

        else:
            raise ValueError(
                "Chris - ABS NOM files must commence with p or f: {abs_filepath.stem} does not!"
            )

        write_outfile(df, abs_filepath, abs_original_data_folder, analysis_folder)

    return None


def write_outfile(df, abs_filepath, abs_original_data_folder, analysis_folder):
    """
    write out the processed ABS data to the ABS data folder and the analysis folder

    Parameters
    ----------
    df: pandas dataframe to write out
    abs_filepath: Path object of original ABS file
    abs_original_data_folder: Path object of path to ABS data folder
    analysis_folder: Path to folder containing all NOM unit record parquet files

    Returns
    -------
    None
    """

    # ABS NOM filenames are of the type xxxx2018q1.sas...
    # Want to extract the date compenent: 2018q1
    date_start = abs_filepath.stem.find("2")

    if date_start != -1:  # if a '2' is found
        filename_date = abs_filepath.stem[date_start:]

        ## append '_p' if it's a preliminary file
        if abs_filepath.stem[0] == "p":
            filename_date = filename_date + "_p"
    else:
        raise ValueError(
            f"Chris - filename {abs_filepath.stem} does not appear to have a 20XXqY date in it"
        )

    filename = "traveller_characteristics" + filename_date + ".parquet"

    # Write to original ABS folder:
    #    to keep as history for comparison with updated preliminary/final files
    df.to_parquet(abs_original_data_folder / filename)

    # Write to folder for analysis
    df.to_parquet(analysis_folder / filename)

    # if a final file replaces a preliminary file - delete it from the analysis file
    if abs_filepath.stem[0] == "f":
        preliminary_filename = (
            "traveller_characteristics" + filename_date + "_p" + ".parquet"
        )
        preliminary_path = analysis_folder / preliminary_filename
        if preliminary_path.exists():
            preliminary_path.unlink()

    return None


def get_visa_code_descriptions(vsc_list):
    """
    get visa code descriptions

    parameters
    ----------
    vsc_list: list
       visa suc codes as strings

    returns
    -------
    a dictionary matching visa subcode to description
    """

    with open(dict_data_folder / "dict_visa_code_descriptions.pickle", "rb") as pickle_file:
        dict_visa_code_descriptions = pickle.load(pickle_file)

    for vsc in vsc_list:
        print(dict_visa_code_descriptions[vsc])

    return dict_visa_code_descriptions


def get_monthly(
    df, net_erp_effect, group_by=("Duration_movement_date", "Visa_subclass")
    ):
    """
    Aggregate unit record NOM data to monthly by visa subclass
    """

    summary = (
        df[df.net_erp_effect == net_erp_effect]
        .groupby(group_by)
        .net_erp_effect.sum()
        .unstack()
    )

    return summary.resample("M").sum()


def read_single_NOM_file(data_folder, file_name, field_list=None):

    if field_list is None:
        df = pd.read_parquet(data_folder / file_name)
    else:
        df = pd.read_parquet(data_folder / file_name, columns=field_list)

    return df


def get_NOM_monthly_old(net_erp_effect, data_folder=Path("parquet")):
    """
    A generator for returning NOM data selected for arrivals or departures

    Parameters
    ----------
    net_erp_effect: contribution to NOM: 1 = arrivals, -1 = departure

    data_folder: a Path object to the folder containing ABS NOM unit record data

    Yields:
    -------
    NOM_effect: a dataframe selected on net_erp_effect
    """

    assert (net_erp_effect == 1) | (net_erp_effect == -1)

    for p in sorted(data_folder.glob("*.parq")):
        print(p.stem)

        df = pd.read_parquet(p)

        monthly_nom_outcomes = get_monthly(df, net_erp_effect)

        yield monthly_nom_outcomes


def get_visa_groups_old(visa_groups, df_nom):
    for group, idx in visa_groups.items():
        df = df_nom[idx]

        if group not in ["citizens", "student"]:  # don't aggregate if in list:
            if len(df.columns) > 1:
                df = df.sum(axis=1)

            df.name = group

        if group == "student":
            df.columns = [
                s.lower().replace(" ", "_") for s in df.columns.droplevel(level=0)
            ]
            # columns to breakout
            idx_break_out = ["572", "573", "570"]
            idx_break_outnames = ["higher_ed", "vet", "elicos", "student_other"]
            df = pd.concat(
                [df[idx_break_out], df.drop(columns=idx_break_out).sum(axis=1)], axis=1
            )
            df.columns = idx_break_outnames

        if group == "citizens":
            df.columns = [
                s.lower().replace(" ", "_") for s in df.columns.droplevel(level=1)
            ]

        yield df


def get_NOM(data_folder, abs_visa_group, nom_fields, abs_visagroup_exists=False):
    """
    A generator to return unit records in an ABS visa group

    Parameters:
    -----------
    data_folder: string, path object (pathlib.Path)
      assumes contains parquet files

    vsc: list
      list of visa sub groups

    nom_fields: list
      list of nom fields to be extracts from ABS unit record file
    """

    # abs_visa_group_current = ['AUST', 'NZLA', # Australian citizen, NZ citizen
    #                           'PSKL', 'PFAM', 'POTH', # skill, family, other
    #                           'TSKL', 'TSTD', 'TWRK', 'TOTH', 'TVIS' #still, student, WHM, other, visitor
    #                          ]

    # if not abs_visa_group in abs_visa_group_current:
    #     raise ValueError(f'Chris: {abs_visa_group} not legitimate ABS visa group.')

    if not isinstance(nom_fields, (list, tuple)):
        raise ValueError(
            "Chris: get_NOM expects {nom_fields} to be a list of fields to extract."
        )

    for p in sorted(data_folder.glob("*.parquet")):

        # Only loop over post 2011Q3 files
        if abs_visagroup_exists:
            if "ROADS" in p.stem:
                continue
        print(p.stem)
        df = pd.read_parquet(p, columns=nom_fields)
        yield df[(df.net_erp_effect != 0) & (df.visa_group == abs_visa_group)]


def append_nom_columns(df):
    """
    Append each visa with a NOM column

    Parameters
    ----------
    df: data frame
        the dataframe has hierarchical columns where:
        level[0] has [arrival, departure]
        level[1] has [visagroup, VSC, VSC etc]
    """

    # set visa subclasses to level 0 & arrival, departure at levet 1)
    df.columns = df.columns.swaplevel()
    df = df.sort_index(axis="columns")

    for col in df.columns.levels[0]:
        df[(col, "nom")] = df[(col, "arrival")] - df[(col, "departure")]

    df.columns = df.columns.swaplevel()
    df = df.sort_index(axis="columns")

    return df


def make_unique_movement_files(characteristcis_folder=abs_traveller_characteristics_folder, nom_final=True):
    nom_fields = [
        "person_id",
        "duration_movement_date",
        "visa_subclass",
        "net_erp_effect",
    ]


    # establish the generators
    get_file_paths = gen_nom_files(
        characteristcis_folder,
        abs_visagroup_exists=False,
        nom_final=nom_final)

    df_get_fields = gen_nom_fields(get_file_paths, nom_fields)
    df_visa_group = gen_get_visa_group(df_get_fields, vsc_list=None)


    # build the NOM dataframe
    df = (pd.concat(df_visa_group, axis="index", ignore_index=True, sort=False)
                    .rename({"duration_movement_date": "date"}, axis="columns")
                    .sort_values(["date", "person_id"])
                )
    
    if nom_final:
        file_name = "NOM unique movement - final.parquet"
    else:
        file_name = "NOM unique movement - preliminary.parquet"

    df.to_parquet(individual_movements_folder / file_name)

    return df




# Dictionary utilities
def get_vsc_reference(file_path=None):
    """
    Return a dataframe containing definitions and groupings for visa subclasses
    The reference definitions and groupings is the sql table 'REF_VISA_SUBCLASS'
    It is maintained by the visa stats team.

    Parameters:
    -----------
    file_path: Path or str object
        filepath to parquet file

    Returns:
    -------
    dataframe
    """

    if file_path == None:
        file_path = dict_data_folder / "REF_VISA_SUBCLASS.parquet"

    reference_visa_dict = (
        pd.read_parquet(file_path)
        .rename(columns=str.lower)
        .rename(columns=lambda x: x.replace(" ", "_"))
    )

    return reference_visa_dict


def get_ABS_visa_grouping(file_path=None):
    """
    Return a dataframe with ABS visa groupings (in cat no. 3412) by subclass
    See ABS Migration unit for updated copies of excel file

    Parameters:
    -----------
    file_path: None or Path object to 'ABS - Visacode3412mapping.xlsx'

    Returns:
    -------
    dataframe
    """

    if file_path is None:
        file_path = dict_data_folder / "ABS - Visacode3412mapping.xlsx"

    abs_3412 = (
        pd.read_excel(file_path)
        .rename(columns=str.lower)
        .rename(columns=lambda x: x.replace(" ", "_"))
        # make sure visa subclass code is a string
        .assign(visa_subclass_code=lambda x: x.visa_subclass_code.astype(str))
    )

    return abs_3412


def get_abs_3412_mapper(df_abs_3412=None):
    """
    Return a dictionary to map subclass strings to modified ABS groupings

    Parameters
    ----------
    df_abs_3412: dataframe, output from get_ABS_visa_grouping
        3 columns in the dataframe: visa_subclass_code,
                                    visa_subclass_label,
                                    migration_publication_category

    Returns:
    --------
    abs_3412_mapper
    """
    # TODO: add in test that dataframe contains the expected columns

    if df_abs_3412 is None:
        df_abs_3412 = get_ABS_visa_grouping()

    idx = ["visa_subclass_code", "migration_publication_category"]
    abs_3412_mapper = df_abs_3412[idx].set_index("visa_subclass_code").squeeze()

    # Thhe ABS migration_publication_category splits students out - put them back into one group
    student_mapper = {
        "Higher education sector": "Student",
        "Student  VET": "Student",
        "Student other": "Student",
    }

    abs_3412_mapper[abs_3412_mapper.isin(student_mapper.keys())] = "Student"

    ## break out a bridging category
    bridging = {
        "10": "Bridging",
        "010": "Bridging",
        "020": "Bridging",
        "20": "Bridging",
        "030": "Bridging",
        "30": "Bridging",
        "040": "Bridging",
        "40": "Bridging",
        "041": "Bridging",
        "41": "Bridging",
        "42": "Bridging",
        "042": "Bridging",
        "050": "Bridging",
        "50": "Bridging",
        "051": "Bridging",
        "51": "Bridging",
        "060": "Bridging",
        "60": "Bridging",
        "070": "Bridging",
    }

    idx = abs_3412_mapper.index.isin(bridging.keys())
    abs_3412_mapper[idx] = "Bridging"

    # as the mapper is used to generate variable names (in columns), make lowercase, no breaks
    abs_3412_mapper = abs_3412_mapper.str.lower().str.replace(" ", "_")

    return abs_3412_mapper


def get_ABS_3412_definitions(abs_3412_excel_path):
    """
    Get a

    Parameters:
    -----------
    abs_excel_path: Path object, or str
      absolute path to ABS 3412 visa groupings and subclasses

    Return:
    -------

    Dataframe
        with visa subclass as index,
        column 0 is home affairs visa reporting subclass defintions
        column 1 is ABS visa
    """

    abs_3412_def = (
        pd.read_excel(abs_3412_excel_path)
        .rename(str.lower, axis="columns")
        .rename(lambda x: x.replace(" ", "_"), axis="columns")
        # make sure visa subclass code are all strings
        .assign(visa_subclass_code=lambda x: x.visa_subclass_code.astype(str))
        .set_index("visa_subclass_code")
    )

    student_mapper = {
        "Higher education sector": "Student",
        "Student  VET": "Student",
        "Student other": "Student",
    }
    # using map writes NaNs for items not being mapped rather than ignoring
    # abs_3412_def = abs_3412_def.map(student_mapper)

    idx = abs_3412_def["migration_publication_category"].isin(student_mapper.keys())

    abs_3412_def.loc[idx, "migration_publication_category"] = "Student"

    return abs_3412_def


# Generic Chart Utilities - always check consistent with Chris_utiltiies
def adjust_chart(ax, ylim_min=None, do_thousands=False):
    """
    add second y axis, remove borders, set grid_lines on

    Parameters
    ----------
    ax: ax
      the left hand axis to be swapped
      # TODO: make it so that the side of the axis is endogenous, and the opposite side is created

    do_thousands: boolean
      if True, call thousands style

    Returns:
    -------
    ax, ax2
    """

    if ylim_min != None:
        ax.set_ylim(ylim_min, None)

    # remove second axes if it exists
    # this will occur when multiple calls to a figure are made - eg plotting forecasts on top of actuals
    fig = ax.get_figure()

    if len(fig.axes) == 2:
        fig.axes[1].remove()
    #     ax, ax2 = adjust_chart(ax, do_thousands=True)
    # else:
    #     ax2 = fig.axes[1]

    ax2 = ax.twinx()
    ax2.set_ylim(ax.get_ylim())

    ax.set_xlabel("")

    if ax.get_ylim()[0] < 0:
        ax.spines["bottom"].set_position(("data", 0))
        ax2.spines["bottom"].set_visible(False)

    for axe in ax.get_figure().axes:
        axe.tick_params(axis="y", length=0)

        for spine in ["top", "left", "right"]:
            axe.spines[spine].set_visible(False)

    ax.set_axisbelow(True)
    ax.grid(axis="y", alpha=0.5, lw=0.8)

    if do_thousands:
        thousands(ax, ax2)

    return ax, ax2


def commas(x, pos):
    # formatter function takes tick label and tick position - but position is
    # passed from FuncFormatter()
    # PEP 378 - format specifier for thousands separator
    return "{:,d}".format(int(x))


def thousands(*axes, y=True):
    comma_formatter = mpl.ticker.FuncFormatter(commas)

    if y:
        for ax in axes:
            ax.yaxis.set_major_formatter(comma_formatter)
    else:
        for ax in axes:
            ax.xaxis.set_major_formatter(comma_formatter)


def set_y_axis_min(vsc):
    """
    Determine whether y_axis_min should be zero or a negative value

    Parameters
    ----------
    vsc: Pandas Series

    Returns
    -------
    zero or 1.1 * negative minimum of the series
    """
    if vsc.min() > 0:
        y_axis_min = 0
    else:
        y_axis_min = 1.1 * vsc.min()

    return y_axis_min


######## Charting of ABS NOM output
def plot_visa_group_stacked(df, group, legend=False):

    # sort by last observation from lowest to highest
    df = df.sort_values(by=df.index[-1], axis=1)

    fig, ax = plt.subplots()

    ax.stackplot(df.index, *list(df.columns), data=df, labels=df.columns)

    ax, ax2 = adjust_chart(ax, df.min().min() * 1.1, do_thousands=True)

    # thousands(ax, ax2)

    ax.set_title(group)

    if legend:
        ax.legend(ncol=4)

    # label_vsc_stacked(df.iloc[-1], ax, None)
    label_vsc_stacked(df.iloc[-1:], ax, None)
    # label_vsc_stacked(df.head(1), ax, left=False)

    return fig, ax, ax2


def plot_visa_group_line_2(df, group):
    fig, ax = plt.subplots()

    for col in df.columns:
        ax.plot(df[col])

    # do total for group
    if len(df.columns) > 1:
        df_group_total = df.sum(axis=1).to_frame().rename(columns={0: "All"})

        df_group_total.plot(ax=ax, ls=":", color="black", legend=None)
        label_vsc(df_group_total.tail(1), ax, "black")

    ax, ax2 = adjust_chart(ax, do_thousands=True)

    ax2.spines["right"].set_position(("outward", 10))

    label_vsc(df.tail(1), ax, None)

    ax.set_title(group)

    ax.set_xlabel("")

    return fig, ax, ax2


def plot_visa_group_line_(df, group):
    ax = df.plot(legend=None)
    fig = ax.get_figure()

    #     ax, ax2 = adjust_chart(ax)
    #     thousands(ax, ax2)

    #     label_vsc(visa_groups_by_year[group].tail(1), ax)

    # do total for group
    if len(df.columns) > 1:
        df_group_total = df.sum(axis=1).to_frame().rename(columns={0: "All"})

        df_group_total.plot(ax=ax, ls=":", color="black", legend=None)
        label_vsc(df_group_total.tail(1), ax, "black")

    ax, ax2 = adjust_chart(ax)
    thousands(ax, ax2)

    label_vsc(df.tail(1), ax, None)

    ax.set_title(group)

    ax.set_xlabel("")

    return fig, ax, ax2


def plot_visa_group_line(df, group):
    ax = df[group].plot(legend=None)
    fig = ax.get_figure()

    #     ax, ax2 = adjust_chart(ax)
    #     thousands(ax, ax2)

    #     label_vsc(visa_groups_by_year[group].tail(1), ax)

    # do total for group
    if len(df[group].columns) > 1:
        df_group_total = df[group].sum(axis=1).to_frame().rename(columns={0: "All"})

        df_group_total.plot(ax=ax, ls=":", color="black", legend=None)
        label_vsc(df_group_total.tail(1), ax, "black")

    ax, ax2 = adjust_chart(ax)
    thousands(ax, ax2)

    label_vsc(df[group].tail(1), ax, None)

    ax.set_title(group)

    ax.set_xlabel("")

    return fig, ax, ax2


def label_vsc(label_positions, ax, color=None):
    """
    Plot the vsc label at the vertical position and date given by label_positions

    Parameters:
    -----------
    label_positions: dataframe
      Expected to be the last row of visa_groups_by_year dataframe for relevant visa_group

    ax: matplotlib axes

    Returns:
    --------
    None
    """

    # Make sure it's one row of data, if not, take the last one
    if len(label_positions) != 1:
        label_positions = label_positions.tail(1)

    for i, col in enumerate(label_positions.columns):

        # not sure why xaxis isn't dates - resorted to using tick location
        # x = mpl.dates.date2num(label_positions.index).values

        # x = ax.xaxis.get_ticklocs()[-1]
        x = (
            mpl.dates.date2num(label_positions.index[-1]) + 30
        )  # shift label right by 30 days
        y = label_positions.iat[0, i]

        if color == "black":
            y = y * 1.05

        if color == None:
            color_ = f"C{i % 10}"  # use to the modulus operator, and assumes default color scheme with 10 colors
            # this ensures that i%10 only returns a value in the range [0,9] regardless of
            # number of visa subclasses - ie if i > 9
        else:
            color_ = color

        ax.text(x, y, col, color=color_)  # fontsize=14,

    return None


def label_vsc_stacked(label_positions, ax, color=None, left=True):
    """
    Plot the vsc label at the vertical position and date given by label_positions

    Parameters:
    -----------
    label_positions: dataframe
      Expected to be the last row of visa_groups_by_year dataframe for relevant visa_group

    ax: matplotlib axes

    Returns:
    --------
    None
    """

    #     #Make sure it's one row of data, if not, take the last one
    #     if len(label_positions) != 1:
    #         label_positions = label_positions.tail(1)

    label_positions = label_positions.sort_values(
        by=label_positions.index[-1], axis=1
    ).cumsum(axis=1)

    if left:
        x = ax.get_xlim()[1] + 0
    else:
        x = ax.get_xlim()[1] - 0

    for i, col in enumerate(label_positions.columns):
        y = label_positions.iat[0, i]

        if y > 0:

            if color == None:
                color_ = f"C{i % 10}"  # use to the modulus operator, and assumes default color scheme with 10 colors
                # this ensures that i%10 only returns a value in the range [0,9] regardless of
                # number of visa subclasses - ie if i > 9
            else:
                color_ = color

            ax.text(x, y, col, color=color_)  # fontsize=14,

    return None


def label_vsc_stacked_(label_positions, ax, color=None, left=True):
    """
    Plot the vsc label at the vertical position and date given by label_positions

    Parameters:
    -----------
    label_positions: dataframe
      Expected to be the last row of visa_groups_by_year dataframe for relevant visa_group

    ax: matplotlib axes

    Returns:
    --------
    None
    """

    #     #Make sure it's one row of data, if not, take the last one
    #     if len(label_positions) != 1:
    #         label_positions = label_positions.tail(1)

    label_positions = label_positions.sort_values(
        by=label_positions.index[-1], axis=1
    ).cumsum(axis=1)

    for i, col in enumerate(label_positions.columns):
        #         not sure why xaxis isn't dates - resorted to using tick location
        #         x = mpl.dates.date2num(label_positions.index).values

        if left:
            x = ax.xaxis.get_ticklocs()[-1] + 365
        else:
            x = ax.xaxis.get_ticklocs()[0] - 400

        y = label_positions.iat[0, i]

        if y > 0:

            if color == None:
                color_ = f"C{i % 10}"  # use to the modulus operator, and assumes default color scheme with 10 colors
                # this ensures that i%10 only returns a value in the range [0,9] regardless of
                # number of visa subclasses - ie if i > 9
            else:
                color_ = color

            ax.text(x, y, col, color=color_)  # fontsize=14,

    return None


def plot_vsc_nom_charts(data, ax=None, ls="-", lw=1.75, colors=["C0", "C1", "C2"], legend=True):
    """
    Plot a 12 month rolling window chart of nom, arrivals and departures

    Parameters:
    -----------
    data: data frame with 3 columns lablled arrivals, departures & nom
    """
    if ax is None:
        ax = plt.gca()

    chart_data = data #.copy().rolling(12).sum().dropna()

    # work around for pandad datetime[ns] vs matplotlib datetime functionality
    # Meant to be resolved in Matplotlib 1.2.3 - but still fails for bar charts
    # chart_data.index = chart_data.index.date

    l1 = chart_data.arrivals.plot(ax=ax, linewidth=lw, linestyle=ls, color=colors[0])
    l2 = chart_data.departures.plot(ax=ax, linewidth=lw, linestyle=ls, color=colors[1])
    l3 = chart_data.nom.plot(ax=ax, linewidth=lw, linestyle=ls, color=colors[2])

    ax, ax2 = adjust_chart(ax, do_thousands=True)

    if legend:
        ax.legend(frameon=False, ncol=3)

    return ax, ax2, l1, l2, l3


def plot_visa_groups(df, visa_group, window=1, nom=False, vsc=None):
    """
    plot visa group and select visa subclasses

    Parameters
    ----------
    df: dataframe
     multiindex dataframe with arrivals & departures by visa subclasses and the visa group

    visa_group: str
      the name of the visa group, eg whm, students etc
      the name will be column in the second level in the hierarchy

    window: int, default=1
      rolling window length, 1(default) is no window, 12 = year ending etc

    nom: boolean, deault = False
        if True, plot NOM

    vsc: List or None, default = None
      list of visa subclasses to plot. If None(default), print all
    """
    if nom:
        # is df.copy() defensive driving here or is Chris confused
        # about whether the object passed to this function is a copy or a reference
        df = append_nom_columns(df.copy())

    df = df.rolling(window).sum().dropna()

    linewidth = 3
    A4_landscape = (11.69, 8.27)
    A4_portrait = (8.27, 11.69)

    fig, fig_axes = plt.subplots(
        figsize=A4_portrait,
        nrows=len(df.columns.levels[1]),
        sharex=True,
        constrained_layout=True,
    )

    for chart_column, direction in enumerate(df.columns.levels[0]):

        # plot visa_group first
        df[(direction, visa_group)].plot(ax=fig_axes[0], lw=linewidth)

        if direction == "departure":
            if nom:
                y_axis_min = set_y_axis_min(df[("nom", visa_group)])
            else:
                y_axis_min = set_y_axis_min(df[(direction, visa_group)])

            ax1, ax2 = adjust_chart(fig_axes[0], y_axis_min)
            thousands(ax1, ax2)

            fig_axes[0].set_title(visa_group, size=14)

        df = df.drop((direction, visa_group), axis="columns")

        for chart_row, col in enumerate(df[direction].columns):
            # Since chart_row is the iterator across VSC's, but fig_axes[0] already holds visa_group plot
            # need to add 1 to chart_row to plot visa sub group in subsequent rows

            df[(direction, col)].plot(ax=fig_axes[chart_row + 1], lw=linewidth)

            # do last otherwise grid line get removed
            if direction == "departure":
                if nom:
                    y_axis_min = set_y_axis_min(df[("nom", col)])
                else:
                    y_axis_min = set_y_axis_min(df[(direction, col)])

                ax1, ax2 = adjust_chart(fig_axes[chart_row + 1], y_axis_min)
                thousands(ax1, ax2)
                fig_axes[chart_row + 1].set_title(col, size=14)

    return fig, fig_axes


def plot_check_for_gaps(arrivals, departures, abs_grouping, label_top_10=None):
    """
    Plot the visa subgroups of a given abs group to assess aggregation requirements

    Rough code transfered by jupyter - could be tidied up
    Parameters
    ----------
    arrivals, tidy dataframed of with keys of date, abs_grouping,visa_label,visa_subclass,count
    departures: tidy dataframed of with keys of date, abs_grouping,visa_label,visa_subclass,count
    abs_grouping: the abs group
    """

    def plot_it(df, direction):
        ax = df.plot(legend=None)
        ax.legend(loc="upper center", bbox_to_anchor=(1.6, 0.9), frameon=False, ncol=1)
        ax.set_title(direction)
        adjust_chart(ax)
        return ax

    labels_arrivals = (
        arrivals[["abs_grouping", "visa_subclass", "visa_label"]]
        .drop_duplicates()
        .astype(str)
    )

    idx_a = labels_arrivals.abs_grouping == abs_grouping

    a = labels_arrivals[idx_a].set_index("visa_subclass").visa_label.rename("arrivals")

    labels_departures = (
        departures[["abs_grouping", "visa_subclass", "visa_label"]]
        .drop_duplicates()
        .astype(str)
    )

    idx_d = labels_departures.abs_grouping == abs_grouping

    d = (
        labels_departures[idx_d]
        .set_index("visa_subclass")
        .visa_label.rename("departures")
    )

    display(pd.concat([a, d], axis=1, sort=False).fillna("").rename_axis(index="vsc"))

    print()
    idx = arrivals.abs_grouping == abs_grouping
    df = (
        arrivals[idx]
        .groupby(["date", "visa_label"])["count"]
        .sum()
        .unstack("visa_label")
        .rolling(12)
        .sum()
    )

    ax_arrivals = plot_it(df, "Arrivals")

    idx = departures.abs_grouping == abs_grouping
    df = (
        departures[idx]
        .groupby(["date", "visa_label"])["count"]
        .sum()
        .unstack("visa_label")
        .rolling(12)
        .sum()
    )
    ax_departures = plot_it(df, "Departures")

    return ax_arrivals, ax_departures


######### Retriving NOM data for analysis
def get_NOM_final_preliminary(data_folder=individual_movements_folder, arrival=True):
    """
    Return dataframe of monthly data by visa subclass

    Parameters:
    ----------
    individual_movements_folder: Path or str object
        filepath to locations of: 'NOM unique movement - preliminary.parquet'
                                  'NOM unique movement - final.parquet')

    arrival: Boolean
        flag to get departure or arrival data

    Returns
    -------
    dataframe
    """
    # TODO: change arrival parameter from booleann to string: direction="arrival" as default, values to be "arrival", "departure", "nom"
    # TODO: generalise to return with multiindex of abs visa group by vsc (ie call nomf.make_vsc_multiIndex)
    ## TODO: think about returning both arrivals and departures as a tidy datasset

    final = pd.read_parquet(data_folder / "NOM unique movement - final.parquet")
    prelim = pd.read_parquet(data_folder / "NOM unique movement - preliminary.parquet")

    if arrival:
        ## generalise with positive (>0) to accomodate propensity values, final NOM has 1, -1
        idx_final = final.net_erp_effect > 0
        idx_prelim = prelim.net_erp_effect > 0
    else:
        ## generalise with negative (<0) to accomodate propensity values, final NOM has 1, -1
        idx_final = final.net_erp_effect < 0
        idx_prelim = prelim.net_erp_effect < 0

    nom = (
        pd.concat([final[idx_final], prelim[idx_prelim]], axis="rows")
        .groupby(["date", "visa_subclass"])
        .net_erp_effect.sum()
        .unstack("visa_subclass")
        .resample("M")
        .sum()
        .round()
        .abs()  # take absolute values to account for -negative departures
        .astype(int)
    )

    return nom


def make_vsc_multiIndex(df, mapper=None):
    """
    generate multiIndex by mapping vsc (column labels of df) via a dict

    Parameters:
    -----------
    df: dataframe whose column names are visa subclass codes

    mapper: a Series mapping subclass codes to labels, usually would be get_abs_3412_mapper

    Returns:
    --------
    dataframe: original dataframe with multiIndex columns
    """

    if mapper is None:
        mapper = get_abs_3412_mapper()

    # check no unmapped visa subclasses
    # ie test whether every vsc element df.columns is in the mapper index
    col_set = set(df.columns)
    map_set = set(mapper.index)

    if not map_set.issuperset(col_set):
        vsc_missing = col_set.difference(map_set)
        error_msg = f"Unmapped visa subclass for {vsc_missing}. \nAdjust file: ABS - Visacode3412mapping.xlsx"
        print(f"{error_msg}")
        raise ValueError(f"\nChris: {error_msg}")

    m_index = mapper.loc[df.columns]

    # check no unmapped visa subclasses
    idx = m_index.isna()

    if idx.any():
        print("Unmapped visa subclasses for:")
        print(f"{m_index.loc[idx]}")
        raise ValueError(f"Chris: Unmapped visa subclass for {m_index.loc[idx]}")

    df.columns = pd.MultiIndex.from_tuples(
        zip(m_index.values, m_index.index), names=["abs_visa_group", "vsc"]
    )
    return df.sort_index(axis=1)


def nom_year_ending(df):
    """Generate a dataframe with multinidex columns given by variables 
        abs_visa_group and direction, value is calculated as yearending
    
    Parameters
    ----------
    df : dataframe
        contains 4 columns; date, abs_visa_group, direction, value
        data is monthly

    Returns
    -------
    a dataframe
    """
    df_ye = (df
     .groupby(["date", "direction", "abs_visa_group"])
     .value
     .sum()
     .unstack(("direction", "abs_visa_group", ))
     .rolling(12)
     .sum()
     .dropna()
     .assign(departures = lambda x: x.departures * -1)
     .swaplevel(axis=1)
     .sort_index(axis=1)
    )

    #calculate total nom
    df_ye_nom = pd.concat([
        df_ye.swaplevel(axis=1)["arrivals"].sum(axis=1).rename("arrivals"),
        df_ye.swaplevel(axis=1)["departures"].sum(axis=1).rename("departures"),
        df_ye.swaplevel(axis=1)["nom"].sum(axis=1).rename("nom"),
        ],
        axis=1
    )
    
    df_ye_nom.columns = pd.MultiIndex.from_product([["nom"],["arrivals", "departures", "nom"]])

    #join df_nom to df_ye
    return pd.concat([df_ye, df_ye_nom], axis=1)


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

    #TODO - don't assume direction is always second
    # TODO - generalise - find "direction" column header make it first 
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


def get_nom_forecast(nom_forecast_filepath, grouping=["date", "abs_visa_group", "direction"]):
    """Return current NOM forecast
    
    Parameters
    ----------
    nom_forecast_filepath : str/Path object
        filepath to file containing tidy version of nom forecasts
    grouping : list
        variables to group the nom data by (usually [])
    """

    return (pd
            .read_parquet(nom_forecast_filepath)
            .set_index(grouping)
            .squeeze()
            .unstack(grouping[1:])
            .sort_index(axis=1)
     )


######### Preparing NOM monthly forecasting data: Generators #######
def gen_nom_files(data_folder, abs_visagroup_exists=False, nom_final=True):
    """
        A generator to the file path to nom unit record file

        Will yield filepaths for either final or preliminary only

        Parameters:
        -----------
        data_folder: string, path object (pathlib.Path)
            assumes contains parquet files

        abs_visagroup_exists: boolean
            True if only loop over post 2011Q2 files
            These files contains the visa_group olumn

        nom_final: boolean
            True if extracting unique NOM from final NOM file
            False if extracting unique propensity NOM from preliminary NOM file

        Yields
        -------
        file_path: path object to parquet file
    """

    for file_path in sorted(data_folder.glob("*.parquet")):
        if abs_visagroup_exists:
            # The field 'visagroup' exists only in post 2011Q3 files
            # Only loop over these files
            if "ROADS" in file_path.stem:
                continue

        if nom_final:
            if file_path.stem[-1] != "p":
                yield file_path
        else:
            # preliminary NOM files end with 'p'
            if file_path.stem[-1] == "p":
                yield file_path


def gen_nom_fields(file_paths, nom_fields, net_erp_effect=True):
    """
        A generator to return DataFrames where NOM event triggered
        for given fields in a unit record file

        Parameters:
        -----------
        file_paths: string, path object (pathlib.Path)
            assumes contains parquet files

        nom_fields: None or list of fields to select
          if None, return all NOM regardless of subclass

        net_erp_effect: boolean, default=True
          if True, only return if net_erp = 1 or -1
          if False, return all net_erp values

        Yields
        -------
        df: DataFrame containing selected fields
    """
    for file_path in file_paths:
        print(file_path.stem)

        df = pd.read_parquet(file_path, columns=nom_fields)
        if net_erp_effect:
            yield df.query("net_erp_effect != 0")
        else:
            yield df


def gen_get_visa_group(df_fields, vsc_list=None):
    """'
    A generator to select the visa subclasses in vsc_list

    Parameters
    ----------
    df_fields: dataframe generator
      a NOM dataframe

    vsc_list: a list or None
      A list of visa sub class numbers to be selected, or, if None, select all
    """

    for df in df_fields:
        if vsc_list is None:
            yield df
        else:
            yield df.query("visa_subclass == @vsc_list")


def get_NOM_query():
    """[summary]
    """
    nom_fields = [
        'person_id',
        'duration_movement_date',
        'visa_subclass',
        'net_erp_effect',
        "country_of_citizenship",
        "country_of_stay",
    ]

    # query_visa = "(visa_subclass=='573') | (visa_subclass=='572')"
    # query_citizenship = "(country_of_citizenship==6101) | (country_of_citizenship==7103) | (country_of_citizenship==7105)"
    # query_citizenship = "(country_of_citizenship==6203)"
    # query = f"({query_visa}) and ({query_citizenship}) and net_erp_effect > 0" #or country_of_birth==6101


    # Define the groupby
    def gen_get_query(df_fields):
        for df in df_fields:
            yield df #.query(query_citizenship)  #query

    # establish the generators
    file_paths = nom.gen_nom_files(data_folder, abs_visagroup_exists=False)
    df_fields = nom.gen_nom_fields(file_paths, nom_fields)
    df_query = gen_get_query(df_fields)


    df = (pd.concat(df_query, axis=0, ignore_index=True, sort=False)
            .rename({'duration_movement_date': 'date'}, axis='columns')
            .assign(abs_visa_group = lambda x: x.visa_subclass.map(abs_mapper))
            .assign(country_of_citizenship = lambda x: x.country_of_citizenship.map(sacc_nom))
            .assign(country_of_stay = lambda x: x.country_of_stay.map(sacc_nom))
        )



    return df


def get_nom_file_fields(data_folder, nom_fields, abs_visagroup_exists=False):
    """
        A generator to return unit records for given fields in nom unit records

        Parameters:
        -----------
        data_folder: string, path object (pathlib.Path)
        assumes folder contains parquet files

        nom_fields: list of fields to select

        abs_visagroup_exists: boolean
        True if only loop over post 2011Q2 files
        These files contains the visa_group list

        Yields
        -------
        dataframe: of NOM grouped by ABS visa group, year and visa subclass
    """

    for file_path in sorted(data_folder.glob("*.parquet")):

        # Only loop over post 2011Q3 files
        if abs_visagroup_exists:
            if "ROADS" in file_path.stem:
                continue
        print(file_path.stem)

        df = pd.read_parquet(file_path, columns=nom_fields)

        yield (
            df.query("net_erp_effect != 0")
            .groupby(
                ["visa_group", df.duration_movement_date.dt.year, "visa_subclass"]
            )["net_erp_effect"]
            .sum()
        )


def get_visa_groups(
    ABS_nom_group,
    vsc_list,
    nom_fields,
    abs_nom_data_folder,
    individual_movements_folder,
    net_erp_effect=True,
    abs_visagroup_exists=False,
):
    """
    Return a dataframe containing each unique NOM movement for a given visa group

    Parameters
    ----------
    ABS_nom_group: str
      The ABS visa group being extracted from the unit record data

    nom_fields: None or list of fields to select
      list of fields should be consistent with ABS_nom_group definition
      if None, return all NOM regardless of subclass

     vsc_list: a list
      contains visa sub class numbers to be selected

       abs_nom_data_folder: path object
      directory location of ABS unit record NOM files, assumes contains parquet files

    individual_movements_folder: path object
      directory location for storing dataframes for each visa group

    net_erp_effect: boolean, default=True
          if True, only return if net_erp = 1 or -1
          if False, return all net_erp values

    abs_visagroup_exists: boolean
            True if only loop over post 2011Q2 files
            These files contains the visa_group olumn

    Returns
    -------
    a dataframe
    """

    # establish the generators
    file_paths = gen_nom_files(abs_nom_data_folder, abs_visagroup_exists=False)
    df_with_fields = gen_nom_fields(file_paths, nom_fields)

    df_visa_group = gen_get_visa_group(df_with_fields, vsc_list)

    # concatenate over the generators
    df = pd.concat(df_visa_group, axis=0, ignore_index=True, sort=False).rename(
        {"duration_movement_date": "date"}, axis="columns"
    )

    df.to_parquet(
        individual_movements_folder / f"{ABS_nom_group} unique movement.parquet"
    )

    return df


def get_NOM_monthly(
    ABS_nom_group, individual_movements_folder, monthly_data_folder, df=None
):
    """
    Convert individual daily data to monthly arrivals & departures by vsc and create data for the ABS visa grouping

    Parameters
    ----------
    ABS_nom_group: str
      The ABS visa group being extracted from the unit record data

    individual_movements_folder: path object
      directory location of individual momements parquet files

    monthly_data_folder: path object
      directory location of monthly data used for forecasting

    df: dataframe or None
      if None, read in dataframe

    returns
    -------
    monthly: dataframe
    """

    if df is None:
        df = pd.read_parquet(
            individual_movements_folder / f"{ABS_nom_group} unique movement.parquet"
        )

    monthly = (
        df.assign(direction=df.net_erp_effect.map({-1: "departure", 1: "arrival"}))
        .groupby(["date", "visa_subclass", "direction"])
        .net_erp_effect.sum()
        .unstack(["visa_subclass", "direction"])
        .sort_index(axis="columns")
        .resample("M")
        .sum()
        .astype(int)
    )

    # Make visa_group summation, sum across arrive/departure - which is 2nd level of multiindex
    visa_group = monthly.sum(level=1, axis=1)

    # make multiidex
    m_index = pd.MultiIndex.from_product(
        [
            # all lower case, no spaces
            [ABS_nom_group.lower().replace(" ", "_")],
            # arrive, departure - this approach makes no assumption on order
            monthly.columns.levels[1],
        ]
    )
    visa_group.columns = m_index

    monthly = pd.concat([monthly, visa_group], axis=1)

    if ABS_nom_group == "New Zealand citizen":
        # single visa: remove equivalent VSC '444'
        monthly = monthly.drop(["444"], axis=1)

    # Write out monthly data as a tidy data set
    (
        monthly.unstack()
        .rename("net_erp_effect")
        .reset_index()
        .to_parquet(monthly_data_folder / f"{ABS_nom_group} - monthly.parquet")
    )

    return monthly


def gen_abs_group_totals(df):
    """
    A generator to add totals for abs groups to a tidy dataframe of arrivals or departures
    by visa subclass

    Parameters
    ----------
    df: a tidy dataframe of abs arrivals or departures
        has columns: date	abs_grouping	visa_label	visa_subclass	count

    Yields
    -------
    a tidy dataframe that:
        contains
            i)  each abs group concatenated with the totals for that group
            ii) the total of arrivals or deparutres

    This groups are collected in a 'pd.concat() group that calls this function
    """

    def add_labels(df, abs_group, label):
        return (
            df.assign(abs_grouping=abs_group)
            .assign(visa_label=label)
            .assign(visa_subclass=label)
        )

    # return conccated abs visa group together with monthly totals for that group
    for abs_group in sorted(df.abs_grouping.unique()):
        idx = df.abs_grouping == abs_group

        abs_group_total = (
            df[idx]
            .groupby(["date", "abs_grouping"])["count"]
            .sum()
            .unstack("abs_grouping")
            .rename(columns={abs_group: "count"})
            .pipe(add_labels, abs_group, "total_" + abs_group)
            .reset_index()
        )

        yield pd.concat([df[idx], abs_group_total], sort=False, ignore_index=True)

    # return totals by month (eg monthly totals for arrivals or departures)
    yield (
        df.groupby("date")["count"]
        .sum()
        .to_frame()
        .pipe(add_labels, "nom", "total")
        .reset_index()
    )


def tidy__by_visa_subclass(df):
    """Convert the vsc by date returned from get_NOM_final_preliminary() to tidy data

    Arguments:
        df {dataframe} -- with columns vsc and rows date
    Returns:
        a tidy dataframe of nom by month, abs_group, visa_label, visa_subclass, nom
    """

    # TODO should df be passed in a call to
    #   get_NOM_final_preliminary with a direction parameter?

    reference_visa_dict = get_vsc_reference()
    abs_3412_mapper = get_abs_3412_mapper()

    col_order = ["date", "abs_grouping", "visa_label", "visa_subclass", "nom"]
    tidy_df = (
        df.unstack()
        .rename("nom")
        .reset_index()
        .assign(
            visa_label=lambda x: x.visa_subclass.map(
                reference_visa_dict.visa_subclass_ds
            )
        )
        .assign(abs_grouping=lambda x: x.visa_subclass.map(abs_3412_mapper))
    )

    ### remove zero entries so contiguous data can be identified later,
    idx = tidy_df["nom"] == 0

    # TODO what sort order of the data should be returned?
    return tidy_df[~idx].reset_index(drop=True)[col_order]


### NOM (3101) analysis
def plot_nom_delta(year_start, year_end, df, ascending=True, legend_display=True):
    """
    TODO: return horizontal bar chart showing changes in groups from year_start to year_end
    TODO: add axes title: NOM delta between year_start and year_end
    TODO: colors: 4 colors: NOM, permanent visa, temporary visa, humanitarian

    Assumes df has a date index - should check

    Parameters:
    ----------

    ascending boolean: True if nom is rising, False if falling (should put in code)
    """

    nom_visa_group_dict = {
        "nom": "Total NOM",
        "special_eligibility_and_humanitarian": "Humanitarian",
        "australian_citizen": "Australian citizen",
        "temporary_work_skilled": "Temporary",
        "visitor": "Temporary",
        "bridging": "Temporary",
        "new_zealand_citizen": "New Zealand citizen",
        "student": "Temporary",
        "other_temporary": "Temporary",
        "working_holiday": "Temporary",
        "family": "Permanent",
        "skill": "Permanent",
        "other": "unknown",
        "other_permanent": "Permanent",
    }

    nom_color_dict = {
        "Total NOM": "C2",
        "Permanent": "#024DA1",
        "Temporary": "#007AC3",
        "Humanitarian": "C4",
        "Australian citizen": "#5B666D",
        "New Zealand citizen": "#BEBFC7",
        "unknown": "C5",
    }

    nom_group_label_dict = {
        "nom": "Total NOM",
        "special_eligibility_and_humanitarian": "Humanitarian",
        "australian_citizen": "Australian",
        "temporary_work_skilled": "Skilled: temp",
        "visitor": "Visitor",
        "bridging": "Bridging",
        "new_zealand_citizen": "New Zealander",
        "student": "Student",
        "other_temporary": "Other: temp",
        "working_holiday": "Working holiday",
        "family": "Family",
        "skill": "Skilled: perm",
        "other": "Unknown",
        "other_permanent": "Other: perm",
    }

    color_dict = dict(zip(nom_group_label_dict.values(), nom_visa_group_dict.values()))

    color_order = pd.DataFrame.from_dict(
        color_dict, orient="index", columns=["visa_type"]
    ).assign(color=lambda x: x.visa_type.map(nom_color_dict))

    nom_delta = (
        df[year_end]
        .T.astype(int)
        .squeeze()
        .subtract(df[year_start].T.astype(int).squeeze())
        .rename("delta")
        .to_frame()
    )

    nom_delta_share = (
        nom_delta.iloc[:-1]
        .divide(nom_delta.iloc[:-1].sum())
        .multiply(100)
        .rename(columns={"delta": "delta_share"})
        .assign(cumalative_share=lambda x: x.delta_share.cumsum())
    )

    nom_delta = (
        pd.concat([nom_delta, nom_delta_share], axis=1, sort=False)
        .assign(visa_groups=nom_delta.index.map(nom_group_label_dict))
        .set_index("visa_groups")
        .sort_values(by="delta_share", ascending=False)
        .sort_values(by="delta", ascending=ascending)
        .assign(cumalative_share=lambda x: x.delta_share.cumsum())
    )

    fig, ax = plt.subplots(constrained_layout=True)

    # plot the 'delta' value
    (
        nom_delta.delta.plot.barh(
            ax=ax, color=color_order.loc[nom_delta.index].color.values, width=0.7,
        )
    )

    ax.xaxis.tick_top()

    for axe in fig.get_axes():
        for spine in ["top", "bottom", "left", "right"]:
            axe.spines[spine].set_visible(False)
        axe.tick_params(length=0)

    ax.axvline(x=0, lw=0.8, color="k", alpha=0.5)

    ax.set_axisbelow(True)
    ax.grid(axis="x", alpha=0.5, lw=0.8)

    thousands(ax, y=False)

    if legend_display:
        legend_elements = [
            Patch(facecolor=colour, label=visa_type)
            for visa_type, colour in nom_color_dict.items()
        ]
        ax.legend(
            handles=legend_elements, frameon=False, facecolor="white", edgecolor="white"
        )

    ax.set_ylabel("")

    return nom_delta, fig, ax


def check_max(df):
    """
    Find max values time series passed in, compare to current period (the
    last observsation is the current period)

    #TODO: add column showing date of last period max if this period is max
           add two columns showing change from last maximum in level and % terms

    Parameters
    ----------
    df: a pandas dataframe
        the index is a timeseries index

    Returns
    -------
    df_max_period: a pandas data frame
        the series names are in the row index with 4 columns:
        Date: date of the last maximum
        Value: what that maximum was
        Maximum: boolean: is the current period the maximum

    """

    df_max = (
        pd.concat([df.idxmax(), df.max()], axis=1)
        .rename(columns={0: "Date", 1: "Value"})
        .rename_axis("NOM variable", axis="rows")
    )

    comparison_this_period = df.iloc[-1] >= df_max.loc[df.iloc[-1].index, "Value"]

    # df_max_period = (pd
    #                        .concat([df_max, comparison_this_period], axis=1)
    #                        .rename(columns={0: 'Maximum'})
    #                   )

    df_max_period = (
        pd.concat([df_max, comparison_this_period], axis=1)
        .rename(columns={0: "Maximum"})
        .assign(this_period=df.iloc[-1])
        .assign(diff_level=lambda x: x.this_period - x.Value)
        .assign(diff_pct=lambda x: (x.diff_level / x.Value) * 100)
        .round()
        .assign(diff_pct=lambda x: x.diff_pct.astype(int))
    )
    ### add in current period
    # df_max_period['this_period'] = df.iloc[-1]

    # idx = df_max_period.Maximum
    # df_max_period['this_period']=np.nan
    # df_max_period.loc[~idx, 'this_period'] = df.iloc[-1].loc[~idx]

    return df_max_period


def check_min(df):
    """
     Find minimum values for each time series in the dataframe, compare to current period (the
    last observsation is the current period)

    #TODO: add column showing date of last period max if this period is max
           add two columns showing change from last maximum in level and % terms

    Parameters
    ----------
    df: a pandas dataframe
        the index is a timeseries index

    Returns
    -------
    df_min_period: a pandas data frame
        the series names are in the row index with 4 columns:
        Date: date of the last maximum
        Value: what that maximum was
        Minimum: boolean: is the current period the maximum
        this_period: value of current period (last observation)
    """

    df_min = (
        pd.concat([df.idxmin(), df.min()], axis=1)
        .rename(columns={0: "Date", 1: "Value"})
        .rename_axis("NOM variable", axis="rows")
    )

    comparison_this_period = df.iloc[-1] <= df_min.loc[df.iloc[-1].index, "Value"]

    # df_min_period = (pd
    #                        .concat([df_min, comparison_this_period], axis=1)
    #                        .rename(columns={0: 'Minimum'})
    #                   )

    df_min_period = (
        pd.concat([df_min, comparison_this_period], axis=1)
        .rename(columns={0: "Minimum"})
        .assign(this_period=df.iloc[-1])
        .assign(diff_level=lambda x: x.this_period - x.Value)
        .assign(diff_pct=lambda x: (x.diff_level / x.Value) * 100)
        .round()
        .assign(diff_pct=lambda x: x.diff_pct.astype(int))
    )

    # idx = df_min_period.Minimum
    # df_min_period['this_period']=np.nan
    # df_min_period.loc[~idx, 'this_period'] = df.iloc[-1].loc[~idx]

    return df_min_period


def display_side_by_side(*args):
    html_str = ""
    for df in args:
        html_str += df.to_html()
    display_html(html_str.replace("table", 'table style="display:inline"'), raw=True)


########################### Frorecast accuracy measures ##########################
def gen_mase(history, forecast, forecast_start_period, seasonal=1):
    """
    A generator that yields the mean absolute scale error (or seasonal mase) for every 
    visa type in the history dataframe

    Parameters:
    -----------
    history: dataframe, may include data from the forecast period
    forecast: dataframe of same variables
    forecast_start_period: datetime forecast start period
    ### T0D0: consider if start period should be extract
    seasonal: int: 1 if it's a relative to a naive forecast, else set seasonality, eg 3, 12
    """
    if "abs_visa_group" not in history.columns:
        raise ValueError(
            "Chris - there is no 'abs_visa_group' in history columns{history.columns}"
        )

    for visa_group in history.abs_visa_group.unique():

        idx_history = history.abs_visa_group == visa_group
        training_set = (
            history[idx_history].groupby(["date", "direction"]).value.sum().unstack()
        )

        idx_predictions = forecast.abs_visa_group == visa_group
        predictions = (
            forecast[idx_predictions]
            .groupby(["date", "direction"])
            .value.sum()
            .unstack()
        )

        n_periods = len(training_set)
        # history ends at the month prior to forecast_start_period
        history_end_period = pd.to_datetime(
            forecast_start_period
        ) + pd.offsets.MonthEnd(-1)

        # Compute the Mean Absolute Error from the in-sample (seasonal) naïve randomw walk forecast

        denominator = training_set[:history_end_period].diff(seasonal).abs().sum() / (
            n_periods - seasonal
        )
        # denominator = (training_set[:history_end_period].diff().abs().sum() / (n_periods - 1))

        # Compute absolute prediction error
        numerator = (
            predictions[forecast_start_period:] - training_set[forecast_start_period:]
        ).abs()

        # Return mean
        mase = (numerator / denominator).mean().rename(visa_group)

        yield mase.to_frame().T


def make_vsc_first_character_lists():
    """
    Generate: ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'k', 'r', 'v', 'y'].
    This is the list of the first characters of  visa subclass codes that appear in the
    in the NOM data
    
    Generate: []
    This is the list of first characters not currently expected in visa subclass codes
    This list should be checked against each new NOM file to identify if the above list should be adjusted
    
    Returns
    -------
    vsc_description_first_char: the list of visa subclass codes first characters that DO appear in visa codes
    vsc_description__not_first_char: the list of first visa sublcass code characters that DO NOT appear in the reference dict.
    """
    #
    vsc_description_first_char = [str(i) for i in range(10)] + [
        "k",
        "r",
        "v",
        "y",
    ]  # 'y']

    # Generate the converse - all letters not expected as the first character in a visa subclass code
    vsc_description__not_first_char = [chr(i) for i in range(ord("a"), ord("z") + 1)]

    for letter in vsc_description_first_char:
        # preferred pattern to drop from list: data[:] = [elem for elem in data if elem != target]
        vsc_description__not_first_char = [
            elem for elem in vsc_description__not_first_char if elem != letter
        ]

        # TODO delete commented code below after testing above preferred pattern works
        # try:
        #     vsc_description__not_first_char.remove(letter)
        # except:
        #     pass

    return vsc_description_first_char, vsc_description__not_first_char


########################### Utilities for aggregating csv forecast files ##########################
def add_nom(df):
    '''
    add nom for each visa group and a total nom (with arrivals, departures and non)
    
    Parameters:
    -----------
    df: a dataframe with multiindex columns of visa_group by (arrivals, departures)
    
    Returns
    -------:
    df: extended with nom for each visa_group plus total nom
    '''


    ## Create nom for each visa grouop
    nom_monthly = df.swaplevel(axis=1).arrivals - df.swaplevel(axis=1).departures
    nom_monthly.columns = pd.MultiIndex.from_product([nom_monthly.columns, ["nom"]])
    df = pd.concat([df, nom_monthly], axis=1).sort_index(axis=1)
    

    ## Create nom total
    nom_total_monthly = df.sum(axis=1, level=1)
    nom_total_monthly.columns = pd.MultiIndex.from_product([["nom"], nom_total_monthly.columns])

    return pd.concat([df, nom_total_monthly], axis=1)

########################### Utilities to check data as expected ####################
def make_vsc_first_character_lists():
    """
    Generate: ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'k', 'r', 'v', 'y'].
    This is the list of the first characters of  visa subclass codes that appear in the
    in the NOM data
    
    Generate: []
    This is the list of first characters not currently expected in visa subclass codes
    This list should be checked against each new NOM file to identify if the above list should be adjusted
    
    Returns
    -------
    vsc_description_first_char: the list of visa subclass codes first characters that DO appear in visa codes
    vsc_description__not_first_char: the list of first visa sublcass code characters that DO NOT appear in the reference dict.
    """
    #
    vsc_description_first_char = [str(i) for i in range(10)] + [
        "k",
        "r",
        "v",
        "y",
    ]  # 'y']

    # Generate the converse - all letters not expected as the first character in a visa subclass code
    vsc_description__not_first_char = [chr(i) for i in range(ord("a"), ord("z") + 1)]
    for letter in vsc_description_first_char:
        try:
            vsc_description__not_first_char.remove(letter)
        except:
            pass

    return vsc_description_first_char, vsc_description__not_first_char


def check_nom_vsc_in_mappers(df, mapper):
    # check no unmapped visa subclasses
    # ie test whether every vsc element df.columns is in the mapper index
    col_set = set(df.columns)
    map_set = set(mapper.index)

    if not map_set.issuperset(col_set):
        vsc_missing = col_set.difference(map_set)
        error_msg = (
            f"Unmapped visa subclass for {vsc_missing}.",
            "Adjust file: ABS - Visacode3412mapping.xlsx or REF_VISA_SUBCLASS.txt",
        )
        print(f"{error_msg}")
        raise ValueError(f"\nChris: {error_msg}")
    return True

### COVID scenarios

def MPO_change(df, date_, visa_, reduction):
    """Adjust a visatype, year by a fixed amount - distributing across visa types on basis of share

    change_array = (
        ("2020-09" ,( "family"), 2_500),
        ("2020-12" , "family", 2_500),
        ("2020-09" , "skill_permanent", 5_000),
        ("2020-12" , "skill_permanent", 5_000),
    )

    for changes in change_array:
        date_, visa_, reduction = changes
        visa_ = ("arrivals", visa_)
        scenario_2021_Q2.loc[date_, visa_] = MPO_change(scenario_2021_Q2, date_=date_, visa_=("arrivals", visa_), reduction=reduction)

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


    def MPO_level_change(df, date_, visa_, level_change, operation="subtraction"):
    """Add or Subtract from a State or a visa group.
    The change is uniformly spread across next column index down (acreoss visa groups if spreading change frome State or across states 
    if spreading change to a visa group)
    
    Add in some error checks or messages to confirm whether it's applied to visa or state and relevant state/visa is in top level group
    """
    state_allocation_increase = (df.loc[date_, visa_]
                         .divide(
                             (df.loc[date_, visa_].sum(axis=1)), axis="rows"
                         )
                         * level_change
                        )
    
    if operation == "subtraction":
        return df.loc[date_, visa_].subtract(state_allocation_increase, axis="rows").values
    elif operation == "addition":
        return df.loc[date_, visa_].add(state_allocation_increase, axis="rows").values
    else:
        raise ValueError(f"Chris: only 'addtional' or 'subtraction'. You tried {operation}.")

