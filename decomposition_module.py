def make_compositional_change(start, end, pop_share_it, lfpr_it):
    # The change in the age composition is the change in the population share (delta_w)
    # weighted by starting participation rate


    # calculate change in the population share
    pop_delta = ((pop_share_it.loc[end] 
                - pop_share_it.loc[start])
               / 100  # divide by 100 to take percentages in the table to be between 0 & 1
              )


    comp_share = pop_delta * lfpr_it.loc[start] # weight delta_w by starting participation rate

    # turn into table,
    comp_share = (comp_share
                    .unstack(['COB', 'sex'])
                    .sort_index(axis=1)
                  )
    
    return add_totals_to_table(comp_share)

def make_propensity_change(start, end, pop_share_it, lfpr_it):
    delta_lf = lfpr_it.loc[end] - lfpr_it.loc[start]

    propensity_change = delta_lf * pop_share_it.loc[end]/100 # divide by 100 to make share range [0,1]

    propensity_change = (propensity_change.unstack(['COB', 'sex'])
                         .sort_index(axis=1)
                        )

    return add_totals_to_table(propensity_change)

def make_LFPR_table(table):
    '''
    Adds a total column
    '''
    table['Total'] = table.sum(axis=1)

    table_row_total = pd.DataFrame(table.sum()).T
    table_row_total.index.name = 'age'
    table = pd.concat([table, table_row_total])

    index_new = list(table.index[:-1].values)
    index_new.append('Total')
    table.index = index_new
    
    return table

def add_totals_to_table(df):
    '''
    Takes raw table with hierarchical coloumn index
    
    Add column total for each item in level 0
    
    Add row total to table

    '''
    
    for cob in df.columns.levels[0]:
        df[(cob, 'Total')] = df[cob].sum(axis=1)

    df = df.sort_index(axis=1)

    row_total = pd.DataFrame(df.sum(axis=0), columns=['Total']).T

    df = pd.concat([df,
                    row_total
                   ]
                  )

    return df

def add_Total_all(composition, propensity):
    '''
    Create a Total row for all changes in participation rates
    composition and propensity must have total rows already
    '''
    total_all = pd.DataFrame(composition.loc['Total'] + propensity.loc['Total'])
    
    total_all.columns = ['Total change']
    total_all = total_all.T
    
    return pd.concat([propensity,total_all])

def join_comp_propen(comp_change, propen_change):
    '''
    Create grand total of two tables
    Return concatenanted elements
    '''
    total = propen_change.loc['Total'] + comp_change.loc['Total']
    total.name = 'Total change'
    
    decompostion_table = pd.concat([comp_change,
                                    propen_change,
                                    pd.DataFrame(total).T
                                    ])
    
    decompostion_table['Total All'] = decompostion_table.loc(axis=1)[:,'Total'].sum(axis=1)

    return decompostion_table

def make_decomposition_table(start, end, lfpr_it, pop_share_it):
    composition_change = make_compositional_change (start, end, pop_share_it, lfpr_it)
    propensity_change = make_propensity_change (start, end, pop_share_it, lfpr_it)
    
    return join_comp_propen(composition_change, propensity_change)





