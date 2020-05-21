"""
    MODULE DOCSTRING TBD
"""
from pathlib import Path

import pandas as pd
import pandas.util.testing as tm
import numpy as np

import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.gridspec as gridspec

from IPython.core.display import HTML, display_html

# import nom_forecast as nomf




DATA_ABS_PATH = Path(Path.home() / "Documents/Analysis/Australian economy/Data/ABS")


def clean_column_names(df, other_text_to_remove="sector"):
    """
    clean dataframe names - replace spaces, hyphens and '_sector' for labour force data!
    """
    if other_text_to_remove:
        df = df.rename(columns=lambda x: x.replace(other_text_to_remove, ""))

    df = (
        df.rename(columns=str.lower)
        .rename(columns=str.strip)
        .rename(columns=lambda x: x.replace(" ", "_"))
        .rename(columns=lambda x: x.replace("/", "_"))
        .rename(columns=lambda x: x.replace("-", "_"))
        .rename(columns=lambda x: x.replace("(", "_"))
        .rename(columns=lambda x: x.replace(")", "_"))
        .rename(columns=lambda x: x.replace("__", "_"))
        # .rename(columns=lambda x: x.replace(, ""))
    )
    return df


def time_delta_rule(t=12, period="A"):
    """Return a Rule for resampling, eg t=12 returns "A-Dec
    
    Parameters
    ----------
    t : int, optional
        The month for year-ending rule in time sereis period, by default 12
    period: str, optional
        Whether Annual or Quarter, optional, by default A
    """
    annual_dict = {
        1: "A-Jan",
        2: "A-Feb",
        3: "A-Mar",
        4: "A-Apr",
        5: "A-May",
        6: "A-Jun",
        7: "A-Jul",
        8: "A-Aug",
        9: "A-Sept",
        10: "A-Oct",
        11: "A-Nov",
        12: "A-Dec",
    }

    if period == "A":
        return annual_dict[t]
    else:
        ### place holder for quarterly etc
        return None


def adjust_chart(ax, ylim_min=None, do_thousands=True):
    """
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
    """

    if ylim_min is not None:
        ax.set_ylim(ylim_min, None)
    else:
        # ax.set_ylim(0, None)
        pass

    # remove second axes if it exists
    # this will occur when multiple calls to a figure are made -
    # eg plotting forecasts on top of actuals

    fig = ax.get_figure()

    # if len(fig.axes) == 2:
    #     fig.axes[1].remove()

    ax_ = ax.twinx()
    ax_.set_ylim(ax.get_ylim())

    ax.set_xlabel("")

    if ax.get_ylim()[0] < 0:
        ax.spines["bottom"].set_position(("data", 0))
        ax_.spines["bottom"].set_visible(False)

    for axe in ax.get_figure().axes:
        axe.tick_params(axis="y", length=0)

        for spine in ["top", "left", "right"]:
            axe.spines[spine].set_visible(False)

    ax.set_axisbelow(True)
    ax.grid(axis="y", alpha=0.5, lw=0.8)

    if do_thousands:
        thousands(ax, ax_)

    return ax, ax_


def recession_spans():
    """
    recession span dates from:
    THE AUSTRALIAN BUSINESS CYCLE: A COINCIDENT INDICATOR APPROACH
    RBA Discussion Paper 2005-07
    Christian Gillitzer, Jonathan Kearns and Anthony Richards
    http://www.rba.gov.au/publications/rdp/2005/pdf/rdp2005-07.pdf

    """

    recessions = [
        [(1965, 6, 30), (1966, 3, 31)],
        [(1971, 9, 30), (1972, 3, 31)],
        [(1975, 6, 30), (1975, 12, 31)],
        [(1977, 6, 30), (1977, 12, 31)],
        [(1981, 9, 30), (1983, 3, 30)],
        [(1990, 6, 30), (1991, 9, 30)],
    ]

    recession_timestamps = []
    for date_start, date_end in recessions:
        recession_timestamps.append(
            (
                pd.Timestamp(pd.datetime(*date_start)),
                pd.Timestamp(pd.datetime(*date_end)),
            )
        )

    return recession_timestamps


def add_recession_bars(ax=None):
    """
    Plots recession bars in a time series chart

    To do:
    *   check ax is not None
    *   check x_axis is a timeseries axis
    *   adjust recessions spans to be only within x_xais timeseries range
    """

    rec_spans = recession_spans()

    for span in rec_spans:
        ax.fill_between(span, *ax.get_ylim(), color="k", alpha=0.25)
    return ax


def ABS_read_non_time_series(rel_path, fname):
    """
    Docstring
    """

    sheet_name = "Table 1.1"

    fpath = rel_path / fname

    df = pd.read_excel(
        fpath,
        sheetname=sheet_name,
        skiprows=4,  # years are in row 5
        skip_footer=14,
        index_col=0,
    )

    return df


def make_categories(df):
    """
    A convenience function to set object types to category variables

    Parameters:
    -----------
    df: pandas dataframe

    Returns:
    -------
    df: with object types set as category variables

    """
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype("category")
    return df


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
        # ax.yaxis.set_major_formatter(y_formatter)


def write_y_axis_label(
    ax, text="missing", x_offset=0, y_offset=0.02, color="#808080", fontsize=8
    ):

    x_lhs = -x_offset
    x_rhs = 1 + x_offset
    y_lhs = 1 + y_offset
    y_rhs = 1 + y_offset

    # left y axis
    ax.text(
        x_lhs,
        y_lhs,
        text,
        horizontalalignment="left",
        verticalalignment="bottom",
        transform=ax.transAxes,
        color=color,
        fontsize=fontsize,
    )

    # right y axis
    ax.text(
        x_rhs,
        y_rhs,
        text,
        horizontalalignment="right",
        verticalalignment="bottom",
        transform=ax.transAxes,
        color=color,
        fontsize=fontsize,
    )
    return None


def set_fin_year_axis(
    ax, rotation=0, ha="center", end_of_fin_year=True, every_year=True
    ):
    """
    Parameters:
    -----------

    end_of_fin_year: Boolean, if True, convert 1996 to 1995-96, else 1996-97
    """

    # Draw the canvas to force labels to be written out
    fig = ax.get_figure()
    fig.canvas.draw()

    labels_old = [tick.get_text() for tick in ax.xaxis.get_ticklabels()]

    if end_of_fin_year:
        # labels_new = ['{0}-{1}'.format(i, int(i[2:])+1)
        #               if i != '' else '' for i in labels_old]

        labels_new = [f"{int(t[:4]) - 1}-{t[2:4]}" for t in labels_old]
    else:
        labels_new = [f"{t[:4]}-{int(t[2:4]) + 1}" for t in labels_old]

    ax.xaxis.set_ticklabels(labels_new, rotation, ha)

    if not every_year:
        # remove every second lear
        labels_old = [tick.get_text() for tick in ax.xaxis.get_ticklabels()]
        labels_new = [t.get_text() if i % 2 else "" for i, t in enumerate(labels_old)]

    return labels_new


def multi_table(table_list):
    """
    Acceps a list of IpyTable objects and returns a table which contains
    each IpyTable in a cell
    """
    return HTML(
        '<table><tr style="background-color:white;">'
        + "".join(["<td>" + table._repr_html_() + "</td>" for table in table_list])
        + "</tr></table>"
    )


def display_side_by_side(*args):
    html_str = ""
    for df in args:
        if isinstance(df, pd.DataFrame):
            html_str += df.to_html()
        elif isinstance(df, pd.Series):
            html_str += df.to_frame().to_html()
    display_html(html_str.replace("table", 'table style="display:inline"'), raw=True)
    return None


def cagr(df):
    """
    Return the compound annual growth rate for a given dataframe.
    Assumes cagr to be calculated from first to last observation.

    Converts data to annual - with annual period based on month of last date

    Parameters:
    -----------
    df: pandas dataframe, indexed by annual timeseries

    returns:
    --------
    cagr: a series or single value depending on number of columns in datframe
    """

    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError(
            "Chris - the dataframe must have a time series index, this one doesn't"
        )

    # use the dates at the start and end of the period to determine the number of years.
    # Former way - indended to be a bit more flexible - but wasn't and wrong!

    number_of_years = df.index.year[-1] - df.index.year[0]

    value_initial = df.iloc[0]
    value_end = df.iloc[-1]

    # Use / operator as assumes series all same length
    # TODO check assumption - eg no NAN etc

    cagr = ((value_end / value_initial) ** (1 / number_of_years) - 1) * 100

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

    return (
        df.groupby(group_var)[sum_var]  # should this be in [] if sum_var is a list?
        .sum()
        .unstack(unstack_var)
    )


def get_oecd_members():
    oecd_members = {
        "Australia": "7-Jun-1971",
        "Austria": "29-Sep-1961",
        "Belgium": "13-Sep-1961",
        "Canada": "10-Apr-1961",
        "Chile": "7-May-2010",
        "Czech Republic": "21-Dec-1995",
        "Denmark": "30-May-1961",
        "Estonia": "9-Dec-2010",
        "Finland": "28-Jan-1969",
        "France": "7-Aug-1961",
        "Germany": "27-Sep-1961",
        "Greece": "27-Sep-1961",
        "Hungary": "7-May-1996",
        "Iceland": "5-Jun-1961",
        "Ireland": "17-Aug-1961",
        "Israel": "7-Sep-2010",
        "Italy": "29-Mar-1962",
        "Japan": "28-Apr-1964",
        "Korea, Rep.": "12-Dec-1996",
        "Latvia": "1-Jul-2016",
        "Luxembourg": "7-Dec-1961",
        "Mexico": "18-May-1994",
        "Netherlands": "13-Nov-1961",
        "New Zealand": "29-May-1973",
        "Norway": "4-Jul-1961",
        "Poland": "22-Nov-1996",
        "Portugal": "4-Aug-1961",
        "Slovak Republic": "14-Dec-2000",
        "Slovenia": "21-Jul-2010",
        "Spain": "3-Aug-1961",
        "Sweden": "28-Sep-1961",
        "Switzerland": "28-Sep-1961",
        "Turkey": "2-Aug-1961",
        "United Kingdom": "2-May-1961",
        "United States": "12-Apr-1961",
    }

    return pd.Series(oecd_members, dtype="datetime64[ns]").sort_values()


def get_program_outcomes(program_data_folder):
    """Return Migration Program Outcomes and Humanitarian programs
    """

    pgm_outcomes = (pd
         .read_excel(program_data_folder / "Permanent arrivals by year - APH.xlsx", 
                     sheet_name="Home Affair data", 
                     usecols="A:I", 
                     index_col=0, 
                     header=0, 
                     parse_dates=True,
                    ) 
        .rename(columns={"All": "programs_total"})
    )

    return pgm_outcomes



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


def makeTimeSeriesDataFrame():
    """
    Return a dataframe with monthly timeseries, defaults are 30 rows, 4 columns
    """
    # There are around 30 of these
    # [i for i in dir(tm) if i.startswith('make')]

    return tm.makeTimeDataFrame(freq="M")


############ Matplotlib testing #########
# See https://matplotlib.org/3.1.0/gallery/style_sheets/style_sheets_reference.html


# Plot current style colors for white and dark backgrounds

def plot_current_style_lines():
    """Display the colors from the current prop_cycle, which is obtained from the rc parameters.
    
    Returns: 
       fig
    """
    prop_cycle = plt.rcParams['axes.prop_cycle']
    colors = prop_cycle.by_key()['color']

    lwbase = plt.rcParams['lines.linewidth']
    thin = lwbase / 2
    thick = lwbase * 3

    fig, axs = plt.subplots(nrows=2, ncols=2, sharex=True, sharey=True)
    for icol in range(2):
        if icol == 0:
            lwx, lwy = thin, lwbase
        else:
            lwx, lwy = lwbase, thick
        for irow in range(2):
            for i, color in enumerate(colors):
                axs[irow, icol].axhline(i, color=color, lw=lwx)
                axs[irow, icol].axvline(i, color=color, lw=lwy)

        axs[1, icol].set_facecolor('k')
        axs[1, icol].xaxis.set_ticks(np.arange(0, 10, 2))
        axs[0, icol].set_title('line widths (pts): %g, %g' % (lwx, lwy),
                            fontsize='medium')

    for irow in range(2):
        axs[irow, 0].yaxis.set_ticks(np.arange(0, 10, 2))

    fig.suptitle('Colors in the current prop_cycle', fontsize='large')

    return #fig




# See https://matplotlib.org/3.1.0/users/dflt_style_changes.html

# Fixing random state for reproducibility
np.random.seed(19680801)


def plot_scatter(ax, prng, nb_samples=100):
    """Scatter plot.
    """
    for mu, sigma, marker in [(-0.5, 0.75, "o"), (0.75, 1.0, "s")]:
        x, y = prng.normal(loc=mu, scale=sigma, size=(2, nb_samples))
        ax.plot(x, y, ls="none", marker=marker, markersize=3)
    ax.set_xlabel("X-label")
    return ax


def plot_colored_sinusoidal_lines(ax):
    """Plot sinusoidal lines with colors following the style color cycle.
    """
    L = 2 * np.pi
    x = np.linspace(0, L)
    nb_colors = len(plt.rcParams["axes.prop_cycle"])
    shift = np.linspace(0, L, nb_colors, endpoint=False)
    for s in shift:
        ax.plot(x, np.sin(x + s), "-")
    ax.set_xlim([x[0], x[-1]])
    return ax


def plot_bar_graphs(ax, prng, min_value=5, max_value=25, nb_samples=5):
    """Plot two bar graphs side by side, with letters as x-tick labels.
    """
    x = np.arange(nb_samples)
    ya, yb = prng.randint(min_value, max_value, size=(2, nb_samples))
    width = 0.25
    ax.bar(x, ya, width)
    ax.bar(x + width, yb, width, color="C2")
    ax.set_xticks(x + width)
    ax.set_xticklabels(["a", "b", "c", "d", "e"])
    return ax


def plot_colored_circles(ax, prng, nb_samples=15):
    """Plot circle patches.

    NB: draws a fixed amount of samples, rather than using the length of
    the color cycle, because different styles may have different numbers
    of colors.
    """
    for sty_dict, j in zip(plt.rcParams["axes.prop_cycle"], range(nb_samples)):
        ax.add_patch(
            plt.Circle(
                prng.normal(scale=3, size=2), radius=1.0, color=sty_dict["color"]
            )
        )
    # Force the limits to be the same across the styles (because different
    # styles may have different numbers of available colors).
    ax.set_xlim([-4, 8])
    ax.set_ylim([-5, 6])
    ax.set_aspect("equal", adjustable="box")  # to plot circles as circles
    return ax


def plot_image_and_patch(ax, prng, size=(20, 20)):
    """Plot an image with random values and superimpose a circular patch.
    """
    values = prng.random_sample(size=size)
    ax.imshow(values, interpolation="none")
    c = plt.Circle((5, 5), radius=5, label="patch")
    ax.add_patch(c)
    # Remove ticks
    ax.set_xticks([])
    ax.set_yticks([])

    return ax


def plot_histograms(ax, prng, nb_samples=10000):
    """Plot 4 histograms and a text annotation.
    """
    params = ((10, 10), (4, 12), (50, 12), (6, 55))
    for a, b in params:
        values = prng.beta(a, b, size=nb_samples)
        ax.hist(values, histtype="stepfilled", bins=30, alpha=0.8, density=True)
    # Add a small annotation.
    ax.annotate(
        "Annotation",
        xy=(0.25, 4.25),
        xytext=(0.9, 0.9),
        textcoords=ax.transAxes,
        va="top",
        ha="right",
        bbox=dict(boxstyle="round", alpha=0.2),
        arrowprops=dict(
            arrowstyle="->", connectionstyle="angle,angleA=-95,angleB=35,rad=10"
        ),
    )
    return ax


def plot_figure(style_label=""):
    """Setup and plot the demonstration figure with a given style.
    """
    # Use a dedicated RandomState instance to draw the same "random" values
    # across the different figures.
    prng = np.random.RandomState(96917002)

    # Tweak the figure size to be better suited for a row of numerous plots:
    # double the width and halve the height. NB: use relative changes because
    # some styles may have a figure size different from the default one.
    (fig_width, fig_height) = plt.rcParams["figure.figsize"]
    fig_size = [fig_width * 2, fig_height / 2]

    fig, axes = plt.subplots(
        ncols=6, nrows=1, num=style_label, figsize=fig_size, squeeze=True, constrained_layout=True
        )
    
    axes[0].set_ylabel(style_label)

    plot_scatter(axes[0], prng)
    plot_image_and_patch(axes[1], prng)
    plot_bar_graphs(axes[2], prng)
    plot_colored_circles(axes[3], prng)
    plot_colored_sinusoidal_lines(axes[4])
    plot_histograms(axes[5], prng)

    return fig


def plot_sample_style(add_defaults=True, style_list=["default", "classic"]):

    if add_defaults:
        style_list = ["default", "classic"] + sorted(style_list)
    else:
        style_list = sorted(style_list)
    #     style for style in plt.style.available if style != 'classic')
    print(style_list)
    # Plot a demonstration figure for every available style sheet.
    for style_label in style_list:
        with plt.style.context(style_label):
            fig = plot_figure(style_label=style_label)

    plt.show()

    return fig


def plot_color_bar():
    # Also a constrained layout example
    # https://matplotlib.org/3.1.1/tutorials/intermediate/constrainedlayout_guide.html
    # For the pcolormesh kwargs (pc_kwargs) we use a dictionary.
    # Below we will assign one colorbar to a number of axes each containing a ScalarMappable;
    # specifying the norm and colormap ensures the colorbar is accurate for all the axes.

    arr = np.arange(100).reshape((10, 10))
    norm = mcolors.Normalize(vmin=0., vmax=100.)
    # see note above: this makes all pcolormesh calls consistent:
    pc_kwargs = {'rasterized': True, 'cmap': 'viridis', 'norm': norm}
    fig, ax = plt.subplots(figsize=(4, 4), constrained_layout=True)
    im = ax.pcolormesh(arr, **pc_kwargs)
    fig.colorbar(im, ax=ax, shrink=0.6)