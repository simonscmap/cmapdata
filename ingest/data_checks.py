"""
Author:  Diana Haring <dharing@uw.edu>
Date: 12-30-2021

cmapdata - data_checks - functions to check data for ingest.
"""

import sys
import os
import pandas as pd
import numpy as np

import common as cmn
import DB


def check_df_on_trajectory(df, cruise_name, loc_round, server, db_name='Opedia'):
    """Data checks on a dataframe before import, relies on Beast database. Checks lat, lon, and date against known trajectory."""
    ## Temp table import to sandbox server
    conn_str = DB.pyodbc_connection_string('Beast')
    quoted_conn_str = DB.urllib_pyodbc_format(conn_str)
    engine = DB.sqlalchemy_engine_string(quoted_conn_str)

    df_distinct = df[['time','lat','lon']].drop_duplicates()
    df_distinct.to_sql('test_dataset', con=engine, if_exists='replace', index=False)

    cruise_id = cmn.getCruiseID_Cruise_Name(cruise_name, server)
    df_traj = DB.dbRead(f"SELECT * FROM {db_name}.dbo.tblCruise_Trajectory WHERE Cruise_ID = {cruise_id}",server)
    df_traj[['time','lat','lon']].to_sql('test_trajectory', con=engine, if_exists='replace', index=False)

    qry_time = """ SELECT d.[time], d.[lat], d.[lon], t.[time], t.[lat], t.[lon], d.lat-t.lat as lat_diff, d.lon-t.lon as lon_diff
        FROM [dbo].[test_dataset] d
        left join [dbo].[test_trajectory] t
        on CAST(d.[time] as smalldatetime)= CAST(t.[time] as smalldatetime)
        where t.time is null or abs(d.lon-t.lon) >0.5 or abs(d.lat-t.lat) >0.5"""
    df_time_check = DB.dbRead(qry_time,'Beast')

    qry_loc = f""" SELECT d.[time], d.[lat], d.[lon], t.[time], t.[lat], t.[lon], ABS(DATEDIFF(hour, d.[time], t.[time])) as time_diff_hr
        FROM [dbo].[test_dataset] d
        left join [dbo].[test_trajectory] t
        on round(d.lat, {loc_round})= round(t.lat, {loc_round}) and round(d.lon, {loc_round})= round(t.lon, {loc_round})
        where t.lat is null or ABS(DATEDIFF(hour,  d.[time], t.[time])) >1 """
    df_loc_check = DB.dbRead(qry_loc,'Beast') 
    print(f"Trajectory date range: {df_traj['time'].min()} - {df_traj['time'].max()}")
    print(f"Dataset date range: {df_distinct['time'].min()} - {df_distinct['time'].max()}")
    return df_time_check, df_loc_check


def check_df_values(df):
    """Data checks on a dataframe before import
    Checks lat, lon, and depth
    Checks all numeric variables for min or max < or > 5 times standard deviation of dataset
    Returns 0 if all checks pass
    Returns 1 or more for each test with values out of expected range
    """
    i = 0
    ## Check data values
    for d in df.columns:
        if d not in ['lat','lon','depth'] and df[d].dtype != 'O' and df[d].dtype != 'bool' and 'datetime' not in df[d].dtype.name:
            std = df.describe()[d]['std']
            mn = df.describe()[d]['min']
            mx = df.describe()[d]['max']
            if 1 > std >=0:
                continue
            if (mn <= std *-5 or mx >= std *5):
                print(f'#############Check data values for {d}. Min: {mn}, Max: {mx}, Stdv: {std}')
                i +=1
    if 'lon' in df.columns.tolist():
        if sum(df['lon']>180) > 0 or sum(df['lon']<-180) > 0:
            print('#############Run dc.mapTo180180(df)')
            i +=1
    if 'lat' in df.columns.tolist():
        if sum(df['lat']>90) > 0 or sum(df['lat']<-90) > 0:
            print('#############Lat out of range') 
            i +=1
    if 'depth'in df.columns.tolist():
        if sum(df['depth']<0):
            print('###########Depth values below zero')
            i +=1
    return i

def check_df_nulls(df, table_name, server):
    """Check dataframe against SQL table for SQL not null columns
    Returns 0 if all checks pass
    Returns 1 or more for each df variable with nulls where SQL column is not null
    """
    i = 0
    query_not_null = f'''
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dbo'
        AND TABLE_NAME = '{table_name}'
        AND IS_NULLABLE = 'NO'
    '''
    df_not_null = DB.dbRead(query_not_null, server)
    for v in df_not_null['COLUMN_NAME'].tolist():
        if df[v].isna().sum() > 0:
            print('##########Null in ' + v)
            i +=1
    return i

def check_df_constraint(df, table_name, server):
    """Check dataframe against SQL table for SQL unique indicies
    Returns 0 if all checks pass
    Returns 1 if duplicates in df where should be unique
    """
    i = 0
    query_unique_indices = f'''
        SELECT TableName = t.name,
            IndexId = ind.index_id,
            ic.key_ordinal,
            ColumnName = col.name,
            col.is_nullable
        FROM sys.indexes ind 
            INNER JOIN sys.index_columns ic ON ind.object_id = ic.object_id and ind.index_id = ic.index_id 
            INNER JOIN sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id 
            INNER JOIN sys.tables t ON ind.object_id = t.object_id 
        WHERE ind.is_primary_key = 0 
            AND ic.is_included_column = 0
            AND ind.is_unique = 1 
            AND t.name = '{table_name}'
        ORDER BY t.name, ind.name, ind.index_id, ic.key_ordinal
    '''
    df_unique_indicies = DB.dbRead(query_unique_indices, server)
    if len(df_unique_indicies) > 0:
        len_check = len(df.groupby(df_unique_indicies['ColumnName'].tolist()).count())
        if len(df) != len_check:
            print(f'#############Check unique constraints. Unique len: {len_check}. DF len: {len(df)}') 
            i +=1
    return i

def check_df_dtypes(df, table_name, server):
    """Check dataframe data types against SQL table
    Returns 0 if all checks pass
    Returns 1 or more for each column with different data types
    """
    i=0
    query = f'''
    SELECT COLUMN_NAME [Columns], DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'
    '''
    df_sql = DB.dbRead(query, server)

    if 'xml' in df_sql.DATA_TYPE.values:
        df_sql.drop(df_sql[df_sql.DATA_TYPE == 'xml'].index, inplace=True)
        df_sql.reset_index(inplace=True)
    if 'ID' in df_sql.columns.tolist():
        df_sql.drop(columns='ID', inplace=True)
        df_sql.reset_index(inplace=True)        
    df_check = df.dtypes.to_frame('dtypes').reset_index()

    for i, row in df_sql.iterrows():
        if row.DATA_TYPE in df_check.iloc[i,1].name or ('varchar' in row.DATA_TYPE and df_check.iloc[i,1].name == 'object'):
            continue
        else:
            print('##########SQL dtype: ' + row.DATA_TYPE + ' var: ' + row.Columns +'. DF dtype: ' + df_check.iloc[i,1].name + ' var: ' + df_check.iloc[i,0])
            i +=1
    return i
  
def check_df_ingest(df, table_name, server):
    """Runs checks on a pandas df for ingest"""
    n = check_df_nulls(df, table_name, server)
    d = check_df_dtypes(df, table_name, server)
    c = check_df_constraint(df, table_name, server)

    if n + d + c == 0:
        print('All checks passed')
    return n + d + c



def check_metadata_for_organism(df, server):
    """Checks variable_metadata_df for variable names or units that could be associated with organisms

    Args:
        df (Pandas DataFrame): Input Pandas DataFrame (ie variable_metadata).
        server (str): Valid CMAP server name. ex Rainier
    Returns:
        org_var_check_passed (bool): If all variable checks passed

    """   
    unit_checks = ['cell','number','#']
    var_checks = ['abundance', 'ecotype', 'enumeration', 'cell', 'heterotrophic', 'genus']
    qry = 'SELECT Name FROM tblOrganism'
    org_df = DB.dbRead(qry, server)
    org_list = org_df['Name'].tolist()
    i = 0
    org_var_check_passed = True
    var_short = df['var_short_name'].tolist()
    var_long = df['var_long_name'].tolist()
    unit_list = df['var_unit'].tolist()
    match_short, match_long, match_unit, match_org = [],[],[],[]
    for v in var_checks:
        for l in var_long:
            if v in l.lower():
                match_long.append(l)        
        for s in var_short:
            if v in s:
                match_short.append(s)
    for u in unit_checks:
        for ul in unit_list:
            if u in ul.lower():
                match_unit.append(ul) 
    for o in org_list:
        for s in var_short:
            if o in s.lower():
                match_org.append(s)
        for l in var_long:
            if o in l.lower():
                match_org.append(l)
    if len(match_short) + len(match_long) > 0 or len(match_org) > 0:
        org_var_check_passed = False
        print(match_short)
        print(match_long)
        print(match_org)
    return org_var_check_passed
    


def validate_organism_ingest(df, server):
    """Checks for Organism ID, if null checks variable name and units to flag for review

    Args:
        df (Pandas DataFrame): Input Pandas DataFrame (ie variable_metadata).
        server (str): Valid CMAP server name. ex Rainier
    Returns:
        org_check_passed (bool): If all variable checks passed
    """
    unit_list = ['cell','number','#']
    var_list = ['abundance', 'ecotype', 'enumeration', 'cells', 'heterotrophic', 'genus']
    blank_check_list = ['', ' ', None, np.nan, 0]
    qry = 'SELECT Name FROM tblOrganism'
    org_df = DB.dbRead(qry, server)
    org_list = org_df['Name'].tolist()
    i = 0
    org_check_passed = True
    if 'org_id' in df.columns.tolist() and 'conversion_coefficient' in df.columns.tolist():
        print('Org columns present')
        for row in df.itertuples(index=True, name='Pandas'):
            ## If org and coeff are blank, check that they should be blank
            if row.org_id in blank_check_list and row.conversion_coefficient in blank_check_list:
                match_unit = [v for v in unit_list if v in row.var_unit]
                if len(match_unit)>0:
                    print(f'Index: {str(row.Index)}, unit {row.var_unit} matches {", ".join(match_unit)}')
                    i +=1
                match_short_var = [v for v in var_list if v in row.var_short_name]
                if len(match_short_var)>0:
                    print(f'Index: {str(row.Index)}, short name --{row.var_short_name}-- matches {", ".join(match_short_var)}')
                    i +=1
                match_long_var = [v for v in var_list if v in row.var_long_name]
                if len(match_long_var)>0:
                    print(f'Index: {str(row.Index)}, long name --{row.var_long_name}-- matches {", ".join(match_long_var)}')
                    i +=1
                match_short_org = [v for v in org_list if v in row.var_short_name]
                if len(match_short_org)>0:
                    print(f'Index: {str(row.Index)}, short name --{row.var_short_name}-- matches {", ".join(match_short_org)}')
                    i +=1
                match_long_org = [v for v in org_list if v in row.var_long_name]
                if len(match_long_org)>0:
                    print(f'Index: {str(row.Index)}, long name --{row.var_long_name}-- matches {", ".join(match_long_org)}')
                    i +=1
            if row.org_id not in blank_check_list and pd.isnull(row.conversion_coefficient):
                print(f'Index: {str(row.Index)}, Organism ID {row.org_id} missing coefficient')
                i +=1
            if pd.isnull(row.org_id) and row.conversion_coefficient not in blank_check_list:
                print(f'Index: {str(row.Index)}, Coefficient {row.conversion_coefficient} Missing organism ID')
                i +=1
            ## More logic can be added here
            if row.conversion_coefficient not in blank_check_list and isinstance(row.conversion_coefficient, (int, float)):
                if (row.conversion_coefficient > 1 and ('^' not in row.var_unit or 'micro' not in row.var_unit)) \
                or (row.conversion_coefficient <= 1 and '^' in row.var_unit):
                    print(f'Index: {str(row.Index)}, Check coefficient {row.conversion_coefficient} and unit {row.var_unit}')
                    i +=1
        if i == 0:
            print('All organism checks passed')
        else:
            org_check_passed = False
    else:
        print('No organism or coefficient columns present')

    return org_check_passed
