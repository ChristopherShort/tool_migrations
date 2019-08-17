'''
    MODULE DOCSTRING TBD
'''
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib as mpl
from IPython.core.display import HTML, display_html


data_abs_path = Path(Path.home() / "Documents/Analysis/Australian economy/Data/ABS")


def clean_column_names(df, other_text_to_remove='sector'):
    '''
    clean dataframe names - replace spaces, hyphens and '_sector' for labour force data!
    '''
    if other_text_to_remove:
        df = df.rename(columns=lambda x: x.replace(other_text_to_remove, ''))

    df = (df
          .rename(columns=str.lower)
          .rename(columns=str.strip)
          .rename(columns=lambda x: x.replace(' ', '_'))
          .rename(columns=lambda x: x.replace('-', '_'))
          .rename(columns=lambda x: x.replace('(', '_'))
          .rename(columns=lambda x: x.replace(')', '_'))
          .rename(columns=lambda x: x.replace('__', '_'))
        # .rename(columns=lambda x: x.replace(, ''))
          )
    return df


def read_abs_data(folder_path=data_abs_path,
                  fname='310101.xls',
                  series_id=None,
                  sheet_name='Data1'):
    ''' Extract data from an ABS time series file
    fname='310101.xls',
    series_id=None,   eg{'births': 'A2133244X', 'deaths': 'A2133245A'...}
    sheet_name='Data1'

    Time series data stored in worksheets labeled 'Data1, Data2, etc or Table 9.1 etc
    First 9 rows contains meta data (description, unit, Series Type, Data type, Frequency,
    colleciton month, start date, end date, number of observations)
    Row 10 contains series ID
    First column contains the dates.
    ABS dates often are start of month - so adjust below

    Sometimes there is trailing data containing footnotes

    # Add in date of release

    '''

    fpath = folder_path / fname

    df = pd.read_excel(fpath,
                       sheet_name=sheet_name,
                       skiprows=9,
                       index_col=0,
                       na_values=['', '-', ' ']
                       )

    # Make dates end of month
    df.index = df.index + pd.offsets.MonthEnd()
    df.index.name = 'date'

    if series_id is None:
        # Return all data with ABS variable names
        # Consider returning meta data as well
        # How will user know requesting both meta and data?
        return df

    # else - keep selected series
    series_names_to_keep = list(series_id.values())
    df = df[series_names_to_keep]

    df.columns = list(series_id.keys())

    return df


def read_abs_notes(folder_path=data_abs_path,
                   fname='310101.xls',
                   sheet_name='Data1'):
    '''
    Read notes in a data table
    Start in column A
    Notes commence with an open parenthesis '('
    identify range of notes by commencing with '('
        This assumes that last note does not have a line continuation, or is not
        a line continuation itself
    '''
    fpath = folder_path / fname

    notes = pd.read_excel(fpath,
                          sheet_name=sheet_name,
                          use_cols='A',
                          names=['note']
                          )
    # Print release date:
    idx = notes.note.str.lower().str.contains('released').fillna(False)
    print(notes[idx].to_string(index=False, header=False))
    print()

    # identify index range containing notes
    notes_rows = notes[notes.note.str[0] == '('].index

    note_start = notes_rows[0]
    note_end = notes_rows[-1]

    for note in notes.loc[note_start:note_end].values:
        print(note[0])
        print()

    return


def read_abs_meta_data(folder_path=data_abs_path,
                       fname='310101.xls',
                       sheet_name='Data1'):
    """
    Return met data for all sereis from an ABS time series worksheet.

    # TODO: think about multiple data sheets, should this single sheet function be generalised
    
    Parameters
    ----------
    folder_path : Path object, optional
        path to folder containing ABS workbook 
    fname : str, optional
        abs timeseries workbook, by default '310101.xls'
    sheet_name : str, optional
        Data1, Data2, etc, by default 'Data1'description_labels=None
    
    Returns
    -------
    dataframe
        a dataframe containing ABS time series meta data, index is Series ID, columns are ABS meta data
    """

    # Meta data contained in the first 10 rows
    nrows = 10 

    fpath = Path(folder_path) / fname

    meta = pd.read_excel(fpath,
                         sheet_name=sheet_name,
                         header=None,
                         nrows=nrows
                         )

    # set column names to series_id (last row), and remove the last row
    meta.columns = meta.iloc[-1]
    meta = meta[:-1]

    if meta.iloc[:, 0].isna()[0]:
        meta.iloc[0, 0] = 'Description'
    else:
        meta.iloc[0, 0] = met.iloc[0, 0].replace(' *> ', '', regex=True)  # wonder what ABS workbooks needed this?

    return meta.set_index('Series ID').rename_axis(columns=None).T


def meta_description_split(df, label_list):
    """Split ABS Description field into components.  Components are delimited with ';'
    
    Parameters
    ----------
    df : dataframe
        a 
    label_list : list
        contains labels for the delimited field in Description column

    Returns
    -------
    dataframe :
        with index of ABS Series ID and columns of the Description elements
    """

    def drop_last_column(df):
        """ABS Descriptions use ';' as separator and the last character is always a ';'
            Pandas doesn't allow a lambda function in the drop columns, so this pipe funciton
        
        Parameters
        ----------
        df : dataframe
        
        Returns
        -------
        dataframe :
            
        """
        cols = df.columns
        return df.drop(columns=cols[-1])
    
    if 'Description' not in df.columns:
        raise ValueError("'Description' not found in columns")
        

    df = (df
            .Description
            .str
            .split(pat=r" *; *", expand=True)
            .pipe(drop_last_column)
            .rename_axis(index="Series_ID", columns=None)
    )

    df.columns = label_list

    return df 


def remove_note_references(df):
    """Remove references such as "(a)" from label rows in ABS non-timeseries worksheets
    
    Parameters
    ----------
    df : dataframe
        a dataframe containing an ABS worksheet with row labels in first colum 
    """
    pat = r"(.+)(\([^\)]+\))"
    df.iloc[:,0] = df.iloc[:,0].str.replace(pat,r'\1')
    
    pat = r"\.$"
    df.iloc[:,0] = df.iloc[:,0].str.replace(pat,"")
    
    return df


def adjust_chart(ax, ylim_min=None, do_thousands=True):
    '''
    add second y axis, remove borders, set grid_lines on

    Parameters
    ----------
    ax: ax
      the left hand axis to be swapped
      # TODO: make it so that relevant axis is endogenous, and the opposite side is created

    do_thousands: boolean
      if True, call thousands style

    Returns:
    -------
    ax, ax2
    '''

    if ylim_min is not None:
        ax.set_ylim(ylim_min, None)
    else:
        # ax.set_ylim(0, None)
        pass

    # remove second axes if it exists
    # this will occur when multiple calls to a figure are made -
    # eg plotting forecasts on top of actuals

    fig = ax.get_figure()

    if len(fig.axes) == 2:
        fig.axes[1].remove()

    ax2 = ax.twinx()
    ax2.set_ylim(ax.get_ylim())

    ax.set_xlabel('')

    if ax.get_ylim()[0] < 0:
        ax.spines['bottom'].set_position(('data', 0))
        ax2.spines['bottom'].set_visible(False)

    for axe in ax.get_figure().axes:
        axe.tick_params(axis='y', length=0)

        for spine in ['top', 'left', 'right']:
            axe.spines[spine].set_visible(False)

    ax.set_axisbelow(True)
    ax.grid(axis='y', alpha=0.5, lw=0.8)

    if do_thousands:
        thousands(ax, ax2)

    return ax, ax2


def recession_spans():
    '''
    recession span dates from:
    THE AUSTRALIAN BUSINESS CYCLE: A COINCIDENT INDICATOR APPROACH
    RBA Discussion Paper 2005-07
    Christian Gillitzer, Jonathan Kearns and Anthony Richards
    http://www.rba.gov.au/publications/rdp/2005/pdf/rdp2005-07.pdf

    '''

    recessions = [[(1965, 6, 30), (1966, 3, 31)],
                  [(1971, 9, 30), (1972, 3, 31)],
                  [(1975, 6, 30), (1975, 12, 31)],
                  [(1977, 6, 30), (1977, 12, 31)],
                  [(1981, 9, 30), (1983, 3, 30)],
                  [(1990, 6, 30), (1991, 9, 30)],
                  ]

    recession_timestamps = []
    for date_start, date_end in recessions:
        recession_timestamps.append((pd.Timestamp(pd.datetime(*date_start)),
                                     pd.Timestamp(pd.datetime(*date_end))
                                     )
                                    )

    return recession_timestamps


def add_recession_bars(ax=None):
    '''
    Plots recession bars in a time series chart

    To do:
    *   check ax is not None
    *   check x_axis is a timeseries axis
    *   adjust recessions spans to be only within x_xais timeseries range
    '''

    rec_spans = recession_spans()

    for span in rec_spans:
        ax.fill_between(span, *ax.get_ylim(), color='k', alpha=.25)
    return ax


def ABS_read_non_time_series(rel_path, fname):
    '''
    Docstring
    '''

    sheet_name = 'Table 1.1'

    fpath = rel_path / fname

    df = pd.read_excel(fpath,
                       sheetname=sheet_name,
                       skiprows=4,  # years are in row 5
                       skip_footer=14,
                       index_col=0,
                       )

    return df


def make_categories(df):
    '''
    A convenience function to set object types to category variables

    Parameters:
    -----------
    df: pandas dataframe

    Returns:
    -------
    df: with object types set as category variables

    '''
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].astype('category')
    return df


def commas(x, pos):
    # formatter function takes tick label and tick position - but position is
    # passed from FuncFormatter()
    # PEP 378 - format specifier for thousands separator
    return '{:,d}'.format(int(x))


def thousands(*axes, y=True):
    comma_formatter = mpl.ticker.FuncFormatter(commas)

    if y:
        for ax in axes:
            ax.yaxis.set_major_formatter(comma_formatter)
    else:
        for ax in axes:
            ax.xaxis.set_major_formatter(comma_formatter)
        # ax.yaxis.set_major_formatter(y_formatter)


def write_y_axis_label(ax,
    text='missing',
    x_offset=0,
    y_offset=0.02,
    color="#808080",
    fontsize=8,
    ):

    x_lhs = -x_offset
    x_rhs = 1 + x_offset
    y_lhs = 1 + y_offset
    y_rhs = 1 + y_offset

    ax.text(x_lhs, y_lhs, text,
            horizontalalignment='right',
            verticalalignment='bottom',
            transform=ax.transAxes,
            color=color,
            fontsize=fontsize
            )

    ax.text(x_rhs, y_rhs, text,
            horizontalalignment='left',
            verticalalignment='bottom',
            transform=ax.transAxes,
            color=color,
            fontsize=fontsize
            )
    return None


def set_fin_year_axis(ax, rotation=0, ha='center', end_of_fin_year=True):
    '''
    Parameters:
    -----------

    end_of_fin_year: Boolean, if True, convert 1996 to 1995-96, else 1996-97
    '''

    # Draw the canvas to force labels to be written out
    fig = ax.get_figure()
    fig.canvas.draw()

    labels_old = [tick.get_text() for tick in ax.xaxis.get_ticklabels()]

    if end_of_fin_year:
        # labels_new = ['{0}-{1}'.format(i, int(i[2:])+1)
        #               if i != '' else '' for i in labels_old]

        labels_new = [f'{int(t[:4]) - 1}-{t[2:4]}' if i % 2 else ''
                      for i, t in enumerate(labels_old)]
    else:
        labels_new = [f"{t[:4]}-{int(t[2:4]) + 1}" for t in labels_old]

    ax.xaxis.set_ticklabels(labels_new, rotation, ha)

    return labels_new


def multi_table(table_list):
    '''
    Acceps a list of IpyTable objects and returns a table which contains
    each IpyTable in a cell
    '''
    return HTML(
        '<table><tr style="background-color:white;">' +
        ''.join(['<td>' + table._repr_html_() + '</td>' for table in table_list]) +
        '</tr></table>'
    )


def display_side_by_side(*args):
    html_str = ''
    for df in args:
        if isinstance(df, pd.DataFrame):
            html_str += df.to_html()
        elif isinstance(df, pd.Series):
            html_str += df.to_frame().to_html()
    display_html(html_str.replace(
        'table', 'table style="display:inline"'), raw=True)
    return None


def cagr(df):
    '''
    Return the compound annual growth rate for a given dataframe.
    Assumes cagr to be calculated from first to last observation.

    Converts data to annual - with annual period based on month of last date

    Parameters:
    -----------
    df: pandas dataframe, indexed by annual timeseries

    returns:
    --------
    cagr: a series or single value depending on number of columns in datframe
    '''

    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError(
            "Chris - the dataframe must have a time series index, this one doesn't")

    # use the dates at the start and end of the period to determine the number of years.
    # Former way - indended to be a bit more flexible - but wasn't and wrong!   

    number_of_years = df.index.year[-1] - df.index.year[0]

    value_initial = df.iloc[0]
    value_end = df.iloc[-1]

    # Use / operator as assumes series all same length
    # TODO check assumption - eg no NAN etc

    cagr = ((value_end / value_initial) ** (1/number_of_years) - 1) * 100

    return cagr


def group_sum_unstack(df, group_var, sum_var, unstack_var):
    """Demongraphy group by date and asgs, but unstack by asgs
    
    Parameters
    ----------
    df : dataframe
        usually a demography dataframe
    group_var : list or str
        variables for groupby
    sum_var : list or str
        what variable is being aggregated
    unstack_var : str
        what variable to ungroupby
    """

    if isinstance(unstack_var, str):
        unstack_var = [unstack_var]

    return (df
       .groupby(group_var)
       [sum_var] # should this be in [] if sum_var is a list?
       .sum()
       .unstack(unstack_var)
      )

def get_oecd_members():
    oecd_members = {
        'Australia': '7-Jun-1971',
        'Austria': '29-Sep-1961',
        'Belgium': '13-Sep-1961',
        'Canada': '10-Apr-1961',
        'Chile': '7-May-2010',
        'Czech Republic': '21-Dec-1995',
        'Denmark': '30-May-1961',
        'Estonia': '9-Dec-2010',
        'Finland': '28-Jan-1969',
        'France': '7-Aug-1961',
        'Germany': '27-Sep-1961',
        'Greece': '27-Sep-1961',
        'Hungary': '7-May-1996',
        'Iceland': '5-Jun-1961',
        'Ireland': '17-Aug-1961',
        'Israel': '7-Sep-2010',
        'Italy': '29-Mar-1962',
        'Japan': '28-Apr-1964',
        'Korea, Rep.': '12-Dec-1996',
        'Latvia': '1-Jul-2016',
        'Luxembourg': '7-Dec-1961',
        'Mexico': '18-May-1994',
        'Netherlands': '13-Nov-1961',
        'New Zealand': '29-May-1973',
        'Norway': '4-Jul-1961',
        'Poland': '22-Nov-1996',
        'Portugal': '4-Aug-1961',
        'Slovak Republic': '14-Dec-2000',
        'Slovenia': '21-Jul-2010',
        'Spain': '3-Aug-1961',
        'Sweden': '28-Sep-1961',
        'Switzerland': '28-Sep-1961',
        'Turkey': '2-Aug-1961',
        'United Kingdom': '2-May-1961',
        'United States': '12-Apr-1961'
        }

    return pd.Series(oecd_members, dtype="datetime64[ns]").sort_values()


############ Debugging utilies

def csnap(df, fn=lambda x: x.shape, msg=None):
    """ Custom Help function to print things in method chaining via pipe.
        Returns back the df to further use in chaining.
        For example (df.pipe(df_shape) as part of a method chain to track changing size
        See https://towardsdatascience.com/the-unreasonable-effectiveness-of-method-chaining-in-pandas-15c2109e3c69
        Note you can pass other lambda functions: eg  .pipe(csnap, lambda x: x.head(), msg="After")
    """
    if msg:
        print(msg)
    display(fn(df))
    return df

import pandas.util.testing as tm
def makeTimeSeriesDataFrame():
    """
    Return a dataframe with monthly timeseries, defaults are 30 rows, 4 columns
    """
    # There are around 30 of these
    #[i for i in dir(tm) if i.startswith('make')]
    
    return tm.makeTimeDataFrame(freq='M')

