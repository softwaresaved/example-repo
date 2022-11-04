#!/usr/bin/env python
# encoding: utf-8

import pandas as pd
from pandas import ExcelWriter
import numpy as np
import csv
import matplotlib.pyplot as plt
from urllib.parse import urlparse
from collections import Counter
import math
import requests
import httplib2
import logging


def import_xls_to_df(filename, name_of_sheet):
    # Set up logging
    logger = logging.getLogger(__name__)
    logger.info('Importing data...')

    return pd.read_excel(filename,sheetname=name_of_sheet)


def add_column5(dataframe,newcol):
    """
    Adds a new column of NaNs called newcol
    :params: a dataframe and column name
    :return: a dataframe with a new column
    """
    
    # Set up logging

    logger = logging.getLogger(__name__)

    logger.info('Adding a column...')



    dataframe[newcol] = np.nan

    return dataframe


#def add_column2(dataframe,newcol):
#    """
#    Adds a new column of NaNs called newcol
#    :params: a dataframe and column name
#    :return: a dataframe with a new column
#    """
#    # Set up logging
#    logger = logging.getLogger(__name__)
#    logger.info('Adding a column...')
#    dataframe[newcol] = np.nan
#    return dataframe


def clean_data2(dataframe,colname):
    logger = logging.getLogger(__name__)
    logger.info('Cleaning data...')
    
    l = len(dataframe)

    dataframe = dataframe[(dataframe['Type of Tech Product'] == 'Software') | (dataframe['Type of Tech Product'] == 'Grid Application') | (dataframe['Type of Tech Product'] == 'e-Business Platform') | (dataframe['Type of Tech Product'] == 'Webtool/Application')]

    t = len(dataframe)
    
    dataframe.drop_duplicates(subset = ['Tech Product'], keep = 'first', inplace = True)

    length_dupes = len(dataframe)
    
    lost_years = ['2006', '2007', '2008', '2009', '2010', '2011']
    for year in lost_years:
        dataframe.drop(dataframe[dataframe[colname] == year].index, inplace = True)

    y = len(dataframe)
    
    for i, row in dataframe.iterrows():
        try:
            int(dataframe[colname][i])
        except:
            dataframe[colname][i] = np.nan
    
    dataframe.dropna(subset=[colname], inplace=True)
    
    length_final = len(dataframe)

    logger.info("Records dropped during tech product cleaning: " + repr(l - t))
    logger.info("Records dropped during duplicate cleaning: " + repr(t - length_dupes))
    logger.info("Records dropped when cleaning years outside 2012-2016: " + repr(length_dupes - y))       
    logger.info("Records dropped during non-valid-year cleaning: " + repr(y - length_final))
    logger.info("Records left in cleaned data set: " + repr(length_final))
    
    return dataframe
    

def produce_count(dataframe, colname):
    # Set up logging
    logger = logging.getLogger(__name__)
    logger.info('Producing a count...')    
    
    dataframe = pd.DataFrame(dataframe[colname].value_counts())
    
    # Add a column for percentages
    dataframe['percentage'] = dataframe[colname]/dataframe[colname].sum()
    
    return dataframe

    
def produce_count_and_na(dataframe, colname):
    # Set up logging
    logger = logging.getLogger(__name__)
    logger.info('Producing a count and including na...')

    # Employ special measures as discussed above for 'Open Source?' field
    if colname == 'Open Source?':
        temp_dataframe = dataframe[dataframe['Type of Tech Product'] == 'Software']
        dataframe = pd.DataFrame(temp_dataframe[colname].value_counts(dropna = False))
    # pd.DataFrame(dataframe[(dataframe['Tech Product'] == 'Software') & (dataframe[colname])].value_counts(dropna = False))
    else:
        dataframe = pd.DataFrame(dataframe[colname].value_counts(dropna = False))

    # Add a column for percentages
    dataframe['percentage'] = dataframe[colname]/dataframe[colname].sum()

    return dataframe


def plot_bar_charts(dataframe,filename,title,xaxis,yaxis,truncate):
    """
    Takes a two-column dataframe and plots it
    :params: a dataframe with two columns (one labels, the other a count), a filename for the resulting chart, a title, and titles for the
    two axes (if title is None, then nothing is plotted), and a truncate variable which cuts down the number of
    rows plotted (unless it's 0 at which point all rows are plotted)
    :return: Nothing, just prints a chart
    """
    # Set up logging
    logger = logging.getLogger(__name__)
    logger.info('Plotting charts...')
    
    if truncate > 0:
        # This cuts the dataframe down to the number of rows given in truncate
        dataframe = dataframe.ix[:truncate]

    dataframe.plot(kind='bar', legend=None)
    plt.title(title)
    if xaxis != None:
        plt.xlabel(xaxis)
    if yaxis != None:
        plt.ylabel(yaxis)
    # This provides more space around the chart to make it prettier        
    plt.tight_layout(True)
    plt.savefig("./charts/" + filename + '.png', format = 'png', dpi = 150)
    plt.show()
    return


def impact_to_txt(dataframe,colname):
    # Set up logging
    logger = logging.getLogger(__name__)
    logger.info('Creating impact text file...')
    
    # Don't want any of the NaNs, so drop them
    impact_dataframe = dataframe.dropna(subset=[colname])
    # Open file for writing
    file_for_impacts = open("./data/impact.txt", 'w')
    # Go through dataframe row by row and write the text from the colname column to as a separate line to the text file
    for i, row in impact_dataframe.iterrows():
        file_for_impacts.write("%s\n" % impact_dataframe[colname][i])
    return


def main():
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    handler = logging.FileHandler("./log/ResearchFishLog.log")
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info('Starting...')
    
    pd.options.mode.chained_assignment = None 

    
    df = import_xls_to_df("./data/Software&TechnicalProducts - ResearchFish.xlsx", "Software_TechnicalProducts")

    print(len(df))

    logger.info('Raw dataframe length before any processing: ' + repr(len(df)))

    add_column5(df,'URL status')

    df47 = clean_data2(df,'Year First Provided')
    
    # Here?
    
    open_source_licence = produce_count_and_na(df,'Open Source?')
    open_source_licence.index = open_source_licence.index.fillna('No response')
    universities = produce_count_and_na(df,'RO')
    unique_rootdomains = produce_count_and_na(rootdomainsdf,'rootdomains')
    year_of_return = produce_count(df,'Year First Provided')
    url_status = produce_count(df,'URL status')

    year_of_return.sort_index(inplace = True)

    impact_to_txt(df,'Impact')

    plot_bar_charts(open_source_licence,'opensource','Is the output under an open-source licence?',None,'No. of outputs',0)
    plot_bar_charts(universities,'universities','Top 30 universities that register the most outputs',None,'No. of outputs',30)
    plot_bar_charts(unique_rootdomains,'rootdomain','30 most popular domains for storing outputs',None,'No. of outputs',30)
    plot_bar_charts(year_of_return,'returnyear','When was output first registered?',None,'No. of outputs',0)


    writer = ExcelWriter("./data/researchfish_results.xlsx")
    open_source_licence.to_excel(writer,'opensource')
    universities.to_excel(writer,'universities')
    unique_rootdomains.to_excel(writer,'rootdomain')
    year_of_return.to_excel(writer,'returnyear')
    url_df.to_excel(writer,'urlstatus')
    url_status.to_excel(writer,'urlstatus_summ')
    df.to_excel(writer,'Resulting_df')
    writer.save()


if __name__ == '__main__':
    main()
