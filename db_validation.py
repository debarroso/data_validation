##########################################################################################
# File: table_validation.py
# File Created: Monday, 19th October 2020 8:41:04 pm
# Author: Oliver DeBarros (debarros.oliver@gmail.com)
# -----
# Last Modified: Monday, 7th December 2020 6:12:43 pm
# Modified By: Oliver DeBarros (debarros.oliver@gmail.com)
# -----
# Description:
#   This file compares an input table or data source against a target
##########################################################################################


###LIBRARIES###
import os, glob, time, json
import pandas as pd
import numpy as np
import snowflake.connector



"""GLOBAL VARIABLES"""

#table_dict stores all of the tables you want validated by source (in CAPS) {SOURCE: [TABLE LIST]} 'ALL' iterates over all files
table_dict = {'': ['']}

#used to determine whether to print out column level details
column_detail = True

#if true we'll run the designated queries in snowflake
run_queries = True

#debug mode on or off
debug = True


#date objects formatted for reporting
tm = time.localtime(time.time())
today = '{}{:02d}{:02d}'.format(tm.tm_year, tm.tm_mon, tm.tm_mday)
current_time = '{:02d}{:02d}{:02d}'.format(tm.tm_hour, tm.tm_min, tm.tm_sec)

if not os.path.exists(os.path.dirname(os.path.dirname(__file__)) + '/OUTPUT/REPORT_STATS/' + today):
    os.makedirs(os.path.dirname(os.path.dirname(__file__)) + '/OUTPUT/REPORT_STATS/' + today)

#used to open the report file in write mode the first time and append mode afterwards
first_run = True
open_mode = {True: 'w', False: 'a'}
pd.set_option('mode.chained_assignment', None)



"""FUNCTION DEFINITIONS"""

"""
Description:
    Call into the main function which drives the program flow.
    Only called if this file is run as a script
"""
def main():

    location = os.path.dirname(os.path.dirname(__file__))

    #execute the queries to the designated input folder
    if run_queries:
        execute_to_csv()
    
    #iterate over source[table] dict
    for source in table_dict:
        for table in table_dict[source]:
            
            #if no table skip
            if not table:
                continue

            if debug and table != 'ALL':
                print(table)

            #if 'ALL' then loop over all csv files in source directory
            if table == 'ALL':
                
                for input_file in glob.glob('{}/INPUT/{}/*.csv'.format(location, source)):
                    input_file = input_file.split('\\')[-1].split('.')[0].strip()
                    
                    if debug:
                        print(input_file)
                    
                    table_obj = DB_Table(source, input_file.upper())
                        
                    compare(table_obj)
                    del(table_obj)
            
            #else just look at the specified table
            else:
                table_obj = DB_Table(source, table.upper())
                compare(table_obj)
                del(table_obj)


"""
Description:
    If called, will execute queries to csv in source input directory
"""
def execute_to_csv(location=os.path.dirname(os.path.dirname(__file__))):
    
    #dict of queries to excute, keys are formatted as 'SOURCE|TABLE'
    queries = parse_input_sql()

    con = open_snowflake_connection()
    cur = con.cursor()

    #execute each query and save in input location
    for index, query in enumerate(queries, start=1):

        if debug:
            print('\n   Running query {} of {}'.format(index, len(queries)))
        
        source = query.split('|')[0]
        table = query.split('|')[1]
        results = cur.execute(queries[query]).fetchall()
        cols = [i[0] for i in cur.description]
        
        if debug:
            print("   Storing result set to df...")
        df = pd.DataFrame(results, columns=cols)
        
        if debug:
            print("   Saving to file...")
        df.to_csv('{}/INPUT/{}/{}.csv'.format(location, source, table), index=False)        
        
    if debug:
        print("\nClosing snowflake connection...\n")

    cur.close()
    con.close()


"""
Description:
    Open a connection to snowflake
Returns:
    Connection object to snowflake dev warehouse
"""
def open_snowflake_connection(location=os.path.dirname(os.path.dirname(__file__))):

    if debug:
        print('Establishing snowflake connection')

    creds_file = '{}\\INPUT\\CONFIG\\creds.json'.format(location)
    with open(creds_file, 'r') as fp:
        creds = json.load(fp)

    con = snowflake.connector.connect(
        account=creds['ACCOUNT'],
        user=creds['USER'],
        password=creds['PWD'],
        warehouse='DEV_WH'
    )

    return con


"""
Description:
    This method will parse the input sql query for the statements to run in snowflake
Returns:
    Dict object with key formatted as SOURCE|TABLE and query
"""
def parse_input_sql(location=os.path.dirname(os.path.dirname(__file__))):

    if debug:
        print('Parsing input SQL file...')
    
    queries = {}
    key = ''
    query = ''
    
    with open('{}/INPUT/INPUT_SQL_QUERIES.sql'.format(location)) as fp:
        for line in fp:
    
            if line[0:2] == '--':
                key = line[2:].strip('\n')
                continue
    
            if key != '':
                query = query + line
    
                if ';' in line:
                    queries[key] = query.strip('\n')
                    key = ''
                    query = ''

                else:
                    query.replace('\n', ' ')
    
    return queries


"""
Description:
    This function performs a database style join on the two dataframes belonging to this table object.
    It then calls the report method to output differences and a general printed report.
"""
def compare(table):
    
    begin_time = time.time()

    if debug:
        print('Comparing records...')

    #dict object that stores mteric values
    counts = {
        'tot_source': len(table.source_df.index),
        'tot_snow': len(table.snow_df.index)
    }

    #write unique records (based on pk) to file and add counts to dict
    merge = table.source_df.merge(table.snow_df, how='outer', on=table.primary_key, indicator=True)
    (counts['unique_source'], counts['unique_snow']) = unique_records(merge, table)
    
    #write duplicate records (based on pk) to file and add counts to dict
    source_dupes = table.source_df[table.source_df.duplicated(table.primary_key)]
    snow_dupes = table.snow_df[table.snow_df.duplicated(table.primary_key)]
    (counts['source_duplicates'], counts['snow_duplicates']) = duplicate_records(table, source_dupes, snow_dupes)

    #get the identical record count
    identical = merge[merge['_merge'] == 'both']
    counts['identical_records'] = len(identical.index)
    identical.to_csv('C:\\\\Users\\debar\\Desktop\\identical.csv', index=False)

    #column overlap for common columns
    if column_detail:
        counts['column_overlap'] = column_overlap(identical, table)
    else:
        counts['column_overlap'] = 'Column details not selected'

    #calculate runtime for this comparison
    counts['runtime'] = round(time.time() - begin_time, 2)
    
    #print report for this table
    report(table, counts)


"""
Description:
    Writes unique records to file
Parameters:
    merged_df = dataframe produced from merging both source and snowflake tables
    table = table object for making sure output saved in right location
Returns:
    tuple with number of how many unique values found (source, snowflake)
"""
def unique_records(merged_df, table, location=os.path.dirname(os.path.dirname(__file__))):
    if debug:
        print('Finding unique...')

    #new dataframes created for records that did not match
    source = merged_df[merged_df['_merge'] == 'left_only']
    snow = merged_df[merged_df['_merge'] == 'right_only']
    
    source.to_csv('{}/OUTPUT/UNIQUE/{}/{}.csv'.format(location, table.source, table.name), index=False)
    snow.to_csv('{}/OUTPUT/UNIQUE/{}/{}.csv'.format(location, 'SNOWFLAKE', table.name), index=False)
    
    return (len(source.index), len(snow.index))


"""
Description:
    Writes duplicate records to file
Parameters:
    table = table object for making sure output saved in right location
    source_dupes = dataframe with source duplicates
    snow_dupes = dataframe with snowflake duplicates
Returns:
    tuple with number of how many duplicate values found (source, snowflake)
"""
def duplicate_records(table, source_dupes, snow_dupes, location=os.path.dirname(os.path.dirname(__file__))):
    if debug:
        print('Getting duplicates...')

    source_len = len(source_dupes.index)
    snow_len = len(snow_dupes.index)

    source_dupes.to_csv('{}/OUTPUT/DUPLICATES/{}/{}.csv'.format(location, table.source, table.name), index=False)
    snow_dupes.to_csv('{}/OUTPUT/DUPLICATES/{}/{}.csv'.format(location, 'SNOWFLAKE', table.name), index=False)

    return (source_len, snow_len)


"""
Description:
    Iterates over each column between two dataframes and returns dict object
    that shows the percent match between common columns
Returns:
    Formatted string of dict pairing
"""
def column_overlap(df, table):

    overlap_string = ''
    column_match_dict = {}

    if debug:
        print('Determining overlap for each column...')

    for column in table.common_columns:
        if column in table.primary_key:
            continue

        overlap_df = df[df['{}_x'.format(column)] == df['{}_y'.format(column)]]
        column_match_dict[column] = (round(len(overlap_df.index) / len(df.index), 2) * 100.00)
    
    for k, v in sorted(column_match_dict.items(), key=lambda item: item[1]):
        overlap_string = overlap_string + 'Column {} = {}%\n'.format(k, v)

    return overlap_string


"""
Description:
    Prints out a general report for each table that we are checking.
"""
def report(table, metric_dict, location=os.path.dirname(os.path.dirname(__file__))):

    if debug:
        print('Generating table report...')

    #if this is the first run, open file in write mode, otherwise in append mode
    global first_run
    out_file = open('{}/OUTPUT/REPORT_STATS/{}/report{}.txt'.format(location, today, current_time), mode=open_mode[first_run])
    first_run = False

    out_file.write(
        'Table Name: ' + table.name + '\n'
        'Table Source: ' + table.source + '\n'
        'Columns Used as Primary Key: ' + str(table.primary_key) + '\n'
        'Comparison Runtime: ' + str(metric_dict['runtime']) + ' seconds\n'
        '\n###COUNTS###\n'
        'Source Record Count: ' + str(metric_dict['tot_source']) + '\n'
        'Snowflake Record Count: ' + str(metric_dict['tot_snow']) + '\n'
        'Difference Count: ' + str(metric_dict['tot_source'] - metric_dict['tot_snow']) + '\n'
        'Percent Variance: ' + '{0:.5f}'.format(abs((metric_dict['tot_source']) - metric_dict['tot_snow'])/((metric_dict['tot_source'] + metric_dict['tot_snow'])/2) * 100) + '%\n'
        'Number of Identical Matches: ' + str(metric_dict['identical_records']) + '\n'
        'Source Duplicate Count: ' + str(metric_dict['source_duplicates']) + '\n'
        'Snowflake Duplicate Count: ' + str(metric_dict['snow_duplicates']) + '\n'
        'Only in Source Count: ' + str(metric_dict['unique_source']) + '\n'
        'Only in Snowflake Count: ' + str(metric_dict['unique_snow']) + '\n'
        '\n---Column Overlap in matching records---\n\n' + metric_dict['column_overlap'] + '\n'
        '\n==========\n\n'
    )

    out_file.close()


"""TRANSFORMATION RULE FUNCTIONS"""
def append_rule(x, value):
    return str(x) + str(value)

def prepend_rule(x, value):
    return str(value) + str(x)

def strip_rule(x, value):
    x = str(x).replace(str(value), '')
    if x == '':
        return np.nan
    else:
        return x

def trunc_date_rule(x):
    return str(x).split(' ')[0]

def capitalize_rule(x):
    if x == np.nan:
        return x
    return str(x).upper()

def cast_int(x):
    if x == np.nan:
        x = '0'
    
    x = x.lstrip('0')
    
    if x == '':
        x = '0'

    if x.isnumeric():
        return int(x)
    else:
        return 0

def round_rule(x, value):
    if x == np.nan or x == 'None':
        x = 0

    return round(float(x), value)

def none_rule(x):
    if 'None' in str(x):
        return np.nan
    elif str(x) == '':
        return np.nan
    elif str(x) == 'NA':
        return np.nan
    elif str(x) == 'Softbank':
        return np.nan
    else:
        return x



"""CLASS DEFINITIONS"""

"""
Description:
    This class represents one database table that we are validating.

Attributes:
    name - the name of this table
    source - the database source for this table
    primary_key - the identified primary key for this table
    source_df - pandas dataframe created from source csv file
    snow_df - pandas dataframe created from snowflake csv file
    column_mapping - how to rename source columns to match SF for comparison
    column_rules - transformations to apply to columns for better comparisons
    common_columns - columns that exist in both source and snowflake tables
"""
class DB_Table():
    def __init__(self, source, name, location=os.path.dirname(os.path.dirname(__file__))):
        self.name = name
        self.source = source
        self.column_mapping = {}
        self.column_rules = []
        self.primary_key = []
        self.source_df = pd.read_csv('{}/INPUT/{}/{}.csv'.format(location, self.source, self.name), dtype=object, parse_dates=True)
        self.snow_df = pd.read_csv('{}/INPUT/{}/{}.csv'.format(location, 'SNOWFLAKE', self.name), dtype=object, parse_dates=True)
        self.read_mapping_file()
        self.rename_source_columns()
        self.common_columns = list(set(self.source_df.columns) & set(self.snow_df.columns))

        #this column will never match so remove it from common columns
        if 'TSTAMP' in self.common_columns:
            self.common_columns.remove('TSTAMP')

        if debug:
            print("Applying rules...")

        #apply rules to dataframes
        for rule in self.column_rules:
            self.apply_rule(rule)

        #dv has null as 'None', change that for comparison
        if self.source == 'DATAVISION':
            self.dv_none_rule()
    

    """
    Description:
        Reads mapping file in input folder and stores mapping to DBTable object
    """
    def read_mapping_file(self, location=os.path.dirname(os.path.dirname(__file__))):
        with open('{}/INPUT/CONFIG/column_mappings.json'.format(location), 'r') as fp:
            json_dict = json.load(fp)

        for table in json_dict:
            if table in self.name:
                self.column_mapping = json_dict[table]['Columns']
                self.column_rules = json_dict[table]['Rules']
                self.primary_key = json_dict[table]['PK']


    """
    Description:
        Applies the transformation rule in the mapping file to the specified column and dataframe
    Parameters:
        rule = one rule in self.columns_rules (created from mapping file)
    """
    def apply_rule(self, rule):

        #used to determine df to use
        target_switch = {"source": self.source_df, "snow": self.snow_df}
        df = target_switch[rule['target']]

        if rule['column'] not in df.columns:
            if debug:
                print('Column {} not found in {} dataframe, skipping rule...'.format(rule['column'], rule['target']))
            
            return

        if rule['operation'] == 'append':
            df[rule['column']] = df[rule['column']].apply(append_rule, value=rule['value'])

        elif rule['operation'] == 'prepend':
            df[rule['column']] = df[rule['column']].apply(prepend_rule, value=rule['value'])

        elif rule['operation'] == 'strip':
            df[rule['column']] = df[rule['column']].apply(strip_rule, value=rule['value'])

        elif rule['operation'] == 'capitalize':
            df[rule['column']] = df[rule['column']].apply(capitalize_rule)

        elif rule['operation'] == 'trunc_date':
            df[rule['column']] = df[rule['column']].apply(trunc_date_rule)

        elif rule['operation'] == 'cast_int':
            df[rule['column']] = df[rule['column']].apply(cast_int)

        elif rule['operation'] == 'round':
            df[rule['column']] = df[rule['column']].apply(round_rule, value=rule['value'])

        elif rule['operation'] == 'none':
            df[rule['column']] = df[rule['column']].apply(none_rule)
        
        else:
            print('Rule not found in Source: {} Table: {}, check syntax and spelling'.format(self.source, self.name))


    def dv_none_rule(self):
        for column in self.source_df.columns:
            self.source_df[column] = self.source_df[column].apply(none_rule)

        for column in self.snow_df.columns:
            self.snow_df[column] = self.snow_df[column].apply(none_rule)


    """
    Description:
        Uses column mapping to rename columns for comparison
    """
    def rename_source_columns(self):
        self.source_df.rename(columns=self.column_mapping, inplace=True)



""""Only run main if we're running this script"""
if __name__ == "__main__":
    main()