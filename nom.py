
'''
Utilities for examining ABS NOM unit record
'''

import pickle
from pathlib import Path
import pandas as pd
import matplotlib as mpl
from matplotlib import pyplot as plt


def get_visa_code_descriptions(vsc_list):
    '''
    get visa code descriptions

    parameters
    ----------
    vsc_list: list
       visa suc codes as strings

    returns
    -------
    a dictionary matching visa subcode to description
    '''

    dict_folder = Path(
        '/Users/christopher/Documents/Analysis/Australian economy/Data/Visa/Dictionaries/')

    with open(dict_folder / 'dict_visa_code_descriptions.pickle', 'rb') as pickle_file:
        dict_visa_code_descriptions = pickle.load(pickle_file)

    for vsc in vsc_list:
        print(dict_visa_code_descriptions[vsc])

    return dict_visa_code_descriptions


def get_monthly(df, net_erp_effect):
    '''
    Aggregate unit record NOM data to monthly by visa subclass
    '''

    summary = (df[df.net_erp_effect == net_erp_effect]
               .groupby(['Duration_movement_date', 'Visa_subclass'])
               .net_erp_effect
               .sum()
               .unstack()
               )

    return summary.resample('M').sum()


def read_single_NOM_file(data_folder, file_name, field_list=None):

    if field_list is None:
        df = pd.read_parquet(data_folder / file_name)
    else:
        df = pd.read_parquet(data_folder / file_name,
                             columns=field_list
                             )

    return df


def get_NOM_monthly(net_erp_effect, data_folder=Path('parquet')):
    '''
    A generator for returning NOM data selected for arrivals or departures

    Parameters
    ----------
    net_erp_effect: contribution to NOM: 1 = arrivals, -1 = departure

    data_folder: a Path object to the folder containing ABS NOM unit record data

    Yields:
    -------
    NOM_effect: a dataframe selected on net_erp_effect
    '''

    assert (net_erp_effect == 1) | (net_erp_effect == -1)

    for p in sorted(data_folder.glob('*.parq')):
        print(p.stem)

        df = pd.read_parquet(p)

        monthly_nom_outcomes = get_monthly(df, net_erp_effect)

        yield monthly_nom_outcomes


def get_visa_groups(visa_groups, df_nom):
    for group, idx in visa_groups.items():
        df = df_nom[idx]

        if group not in ['citizens', 'student']:  # don't aggregate if in list:
            if len(df.columns) > 1:
                df = df.sum(axis=1)

            df.name = group

        if group == 'student':
            df.columns = [s.lower().replace(' ', '_')
                          for s in df.columns.droplevel(level=0)]
            # columns to breakout
            idx_break_out = ['572', '573', '570']
            idx_break_outnames = ['higher_ed',
                                  'vet', 'elicos', 'student_other']
            df = pd.concat([df[idx_break_out],
                            df.drop(columns=idx_break_out).sum(axis=1)
                            ],
                           axis=1
                           )
            df.columns = idx_break_outnames

        if group == 'citizens':
            df.columns = [s.lower().replace(' ', '_')
                          for s in df.columns.droplevel(level=1)]

        yield df


def get_NOM(data_folder, abs_visa_group, nom_fields, abs_visagroup_exists=False):
    '''
    A generator to return unit records in an ABS visa group

    Parameters:
    -----------
    data_folder: string, path object (pathlib.Path)
      assumes contains parquet files

    vsc: list
      list of visa sub groups

    nom_fields: list
      list of nom fields to be extracts from ABS unit record file
    '''

    # abs_visa_group_current = ['AUST', 'NZLA', # Australian citizen, NZ citizen
    #                           'PSKL', 'PFAM', 'POTH', # skill, family, other
    #                           'TSKL', 'TSTD', 'TWRK', 'TOTH', 'TVIS' #still, student, WHM, other, visitor
    #                          ]

    # if not abs_visa_group in abs_visa_group_current:
    #     raise ValueError(f'Chris: {abs_visa_group} not legitimate ABS visa group.')

    if not isinstance(nom_fields, (list, tuple)):
        raise ValueError(
            'Chris: get_NOM expects {nom_fields} to be a list of fields to extract.')

    for p in sorted(data_folder.glob('*.parquet')):

        # Only loop over post 2011Q3 files
        if abs_visagroup_exists:
            if 'ROADS' in p.stem:
                continue
        print(p.stem)
        df = pd.read_parquet(p,
                             columns=nom_fields
                             )
        yield df[(df.net_erp_effect != 0) & (df.visa_group == abs_visa_group)]

# Make a hierarchical column heading - mapping subclasses to Visa Reporting grouping

# arrivals.columns = (pd
#                        .MultiIndex.
#                           from_tuples(
#                             list(
#                                 zip(
#                                     arrivals.columns.map(dict_visa_reporting),
#                                     arrivals.columns
#                                     )
#                                 )
#                            )
#                      )


def gen_nom_files(data_folder, abs_visagroup_exists=False):
    '''
        A generator to return unit records for given fields in nom unit records

        Parameters:
        -----------
        data_folder: string, path object (pathlib.Path)
            assumes contains parquet files

        abs_visagroup_exists: boolean
            True if only loop over post 2011Q2 files
            These files contains the visa_group list

        Yields
        -------
        file_path: path object to parquet file
    '''
    for file_path in sorted(data_folder.glob('*.parquet')):
        if abs_visagroup_exists:
            # Only loop over post 2011Q3 files
            if 'ROADS' in file_path.stem:
                continue

        yield file_path


def gen_nom_fields(file_paths, nom_fields, net_erp_effect=True):
    '''
        A generator to return DataFrames where NOM event triggered
        for given fields in a unit record file

        Parameters:
        -----------
        file_paths: string, path object (pathlib.Path)
            assumes contains parquet files

        nom_fields: None or list of fields to select
          if None, return all NOM regardless of

        net_erp_effect: boolean, default=True
          if True, only return if net_erp = 1 or -1
          if False, return all net_erp values

        Yields
        -------
        df: DataFrame containing selected fields
    '''
    for file_path in file_paths:
        print(file_path.stem)

        df = pd.read_parquet(file_path,
                             columns=nom_fields
                             )
        if net_erp_effect:
            yield df.query('net_erp_effect != 0')
        else:
            yield df


def get_nom_file_fields(data_folder, nom_fields, abs_visagroup_exists=False):
    '''
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
    '''

    for file_path in sorted(data_folder.glob('*.parquet')):

        # Only loop over post 2011Q3 files
        if abs_visagroup_exists:
            if 'ROADS' in file_path.stem:
                continue
        print(file_path.stem)

        df = pd.read_parquet(file_path,
                             columns=nom_fields
                             )

        yield (df
               .query('net_erp_effect != 0')
               .groupby(['visa_group', df.duration_movement_date.dt.year, 'visa_subclass'])['net_erp_effect'].sum()
               )


def plot_vsc_nom_charts(data, ax):
    '''
        data: data frame with 3 columns: arrivals, departures & nom
    '''
    chart_data = data.rolling(12).sum().copy()

    # work around for pandad datetime[ns] vs matplotlib datetime functionality
    # Meant to be resolved in Matplotlib 1.2.3 - but still fails for bar charts
    chart_data.index = chart_data.index.date

    lw = 3

    chart_data.arrival.plot(ax=ax, lw=lw)
    chart_data.departure.plot(ax=ax, lw=lw)
    chart_data.nom.plot(ax=ax, lw=lw)

    ax, ax2 = adjust_chart(ax)

    ax.legend(frameon=False, ncol=3)

    thousands(ax, ax2)

    return ax, ax2


def plot_visa_groups(df, visa_group, window=1, nom=False, vsc=None):
    '''
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
    '''
    if nom:
        # is df.copy() defensive driving here or is Chris confused
        # about whether the object passed to this function is a copy or a reference
        df = append_nom_columns(df.copy())

    df = (df
          .rolling(window)
          .sum()
          .dropna()
          )

    linewidth = 3
    A4_landscape = (11.69, 8.27)
    A4_portrait = (8.27, 11.69)

    fig, fig_axes = plt.subplots(figsize=A4_portrait,
                                 nrows=len(df.columns.levels[1]),
                                 sharex=True,
                                 constrained_layout=True)

    for chart_column, direction in enumerate(df.columns.levels[0]):

        # plot visa_group first
        df[(direction, visa_group)].plot(ax=fig_axes[0], lw=linewidth)

        if direction == 'departure':
            if nom:
                y_axis_min = set_y_axis_min(df[('nom', visa_group)])
            else:
                y_axis_min = set_y_axis_min(df[(direction, visa_group)])

            ax1, ax2 = adjust_chart(fig_axes[0], y_axis_min)
            thousands(ax1, ax2)

            fig_axes[0].set_title(visa_group, size=14)

        df = df.drop((direction, visa_group), axis='columns')

        for chart_row, col in enumerate(df[direction].columns):
            # Since chart_row is the iterator across VSC's, but fig_axes[0] already holds visa_group plot
            # need to add 1 to chart_row to plot visa sub group in subsequent rows

            df[(direction, col)].plot(ax=fig_axes[chart_row + 1], lw=linewidth)

            # do last otherwise grid line get removed
            if direction == 'departure':
                if nom:
                    y_axis_min = set_y_axis_min(df[('nom', col)])
                else:
                    y_axis_min = set_y_axis_min(df[(direction, col)])

                ax1, ax2 = adjust_chart(fig_axes[chart_row + 1], y_axis_min)
                thousands(ax1, ax2)
                fig_axes[chart_row + 1].set_title(col, size=14)

    return fig, fig_axes


def append_nom_columns(df):
    '''
    Append each visa with a NOM column

    Parameters
    ----------
    df: data frame 
        the dataframe has hierarchical columns where:
        level[0] has [arrival, departure]
        level[1] has [visagroup, VSC, VSC etc]
    '''

    # set visa subclasses to level 0 & arrival, departure at levet 1)
    df.columns = df.columns.swaplevel()
    df = df.sort_index(axis='columns')

    for col in df.columns.levels[0]:
        df[(col, 'nom')] = df[(col, 'arrival')] - df[(col, 'departure')]

    df.columns = df.columns.swaplevel()
    df = df.sort_index(axis='columns')

    return df


# Chart Utilities


def adjust_chart(ax, ylim_min=None):
    '''
    remove  add second y axis, borders, set grid_lines on

    Parameters
    ----------
    ax: ax
      the left hand axis to be swapped
      # TODO: make it so that the side of the axis is endogenous, and the opposite side is created

    Returns:
    -------
    ax, ax2
    '''
    if ylim_min != None:
        ax.set_ylim(ylim_min, None)

    ax2 = ax.twinx()
    ax2.set_ylim(ax.get_ylim())

    if ylim_min < 0:
        ax.spines['bottom'].set_position(('data', 0))

    for axe in ax.get_figure().axes:
        axe.tick_params(axis='y', length=0)

        for spine in ['top', 'left', 'right']:
            axe.spines[spine].set_visible(False)

    ax.set_axisbelow(True)
    ax.grid(axis='y', alpha=0.5, lw=0.8)

    return ax, ax2


def commas(x, pos):
    # formatter function takes tick label and tick position - but position is
    # passed from FuncFormatter()
    # PEP 378 - format specifier for thousands separator
    return '{:,d}'.format(int(x))


def thousands(*axes):
    y_formatter = mpl.ticker.FuncFormatter(commas)

    for ax in axes:
        ax.yaxis.set_major_formatter(y_formatter)


def set_y_axis_min(vsc):
    '''
    Determine whether y_axis_min should be zero or a negative value

    Parameters
    ----------
    vsc: Pandas Series

    Returns
    -------
    zero or 1.1 * negative minimum of the series
    '''
    if vsc.min() > 0:
        y_axis_min = 0
    else:
        y_axis_min = 1.1 * vsc.min()

    return y_axis_min
