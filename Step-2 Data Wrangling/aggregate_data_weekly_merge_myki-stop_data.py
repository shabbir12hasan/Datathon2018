import os
import re as re
import pandas as pd
import timeit

# This method will clean the stop data file, which have StopId amd their description
def stop_data():

    # reading stop data
    stop_data_df = pd.read_csv("d:/studymaterial/datathonmelb_2018/stop_locations.txt", delimiter="|", header=None)
    stop_data_df.columns = ["StopID", "StopNameShort", "StopNameLong", "StopType", "SuburbName", "PostCode", "RegionName",
                         "LocalGovernmentArea", "StatDivision", "GPSLat", "GPSLong"]

    # Removing unwanted columns
    remove_columns = ["StopNameShort", "StopNameLong", "PostCode", "LocalGovernmentArea", "StatDivision"]
    stop_data_df = stop_data_df.drop(remove_columns, 1)

    # Removing kerbside spelling mistake error
    stop_data_df['StopType'][stop_data_df['StopType'] == "kerbside"] = "Kerbside"

    return stop_data_df

# This function return week number
def get_week_number(week):
    r = re.compile("([a-zA-Z]+)([0-9]+)")
    m = r.match(week)
    week_number = str(m.group(2))
    # appending 0 if length is less than 2
    if len(week_number) < 2:
        week_number = "0" + week_number
    return week_number


# This method will retrieve data from all weeks in a year, in all  folder- ie, from samp 0-9 and scan on-off
# get data for week 26, take 18 files of week 26 from 18 folders and create one data frame
# combine that data frame with location data
# merge and aggregate and save into one data file
# do this process for all weeks in all input years
# generate one main csv file
def get_aggregate_myki_data_per_location(year_list):

    # Catching time
    start = timeit.default_timer()

    # assigning stop data into df
    stop_data_df = stop_data()
    final_df = stop_data_df.copy()

    # iterate for all years
    for year in year_list:

        # getting list of weeks in a single year folder
        week_list = os.listdir("d:/studymaterial/datathonmelb_2018/samp_1/scanontransaction/" + str(year))

        # iterating for each week folder
        for week in week_list:

            df = pd.DataFrame() #empty df for each week
            for i in range(10):

                # Getting text file from Samp_ i ScanOnTransaction
                files_inside_week = os.listdir("d:/studymaterial/datathonmelb_2018/Samp_" + str(i) +"/ScanOnTransaction/" + str(year) + "/" + str(week))
                for txt_file in files_inside_week:
                    fileName_Samp_0_On = "d:/studymaterial/datathonmelb_2018/Samp_" + str(i) +"/ScanOnTransaction/" + str(year) + "/" + str(week) + "/" + txt_file
                    df_on = pd.read_csv(fileName_Samp_0_On, delimiter="|", header=None)
                    df = df.append(df_on)

                # Getting text file from Samp_i- ScanOffTransaction
                files_inside_week = os.listdir("d:/studymaterial/datathonmelb_2018/Samp_" + str(i) +"/ScanOffTransaction/" + str(year) + "/" + str(week))
                for txt_file in files_inside_week:
                    fileName_Samp_0_Off = "d:/studymaterial/datathonmelb_2018/Samp_" + str(i) +"/ScanOffTransaction/" + str(year) + "/" + str(week) + "/" + txt_file
                    df_Off = pd.read_csv(fileName_Samp_0_Off, delimiter="|", header=None)
                    df = df.append(df_Off)

            print("Total transaction for",year , week , df.shape[0])

            # Attaching column name to df
            df.columns = ['Mode', 'BusinessDate', 'DateTime', 'CardID', 'CardType', 'VehicleID', 'ParentRoute', 'RouteID', 'StopID']

            # Getting records for only "BUS"
            df = df[df["Mode"] == 1]

            # Merging week data with stop location data and creating a new df
            myki_stop_df = pd.merge(df, stop_data_df, how='inner', on=["StopID"])

            # grouping records for same stop
            # myki_stop_df.groupby(["StopType", "StopID"]).size()
            week_data = myki_stop_df.groupby(["StopID"]).size()
            week_data = pd.DataFrame(week_data) # converting to dataframe
            week_data.reset_index(inplace=True) # resetting index
            weekName = "week_" + str(year) + get_week_number(week) # creating new column name
            week_data.columns = ['StopID', weekName]

            final_df = pd.merge(final_df, week_data, how='left', on=["StopID"])
            print("Total aggregate transaction for stops in ", year, week, final_df.shape[0])

            # break # break for one week
        # break # break for one year
    final_df.to_csv("aggregated_stop_data_weekname.csv", sep=',', index=False)

    # calculating time
    stop = timeit.default_timer()
    print('Time taken: ', stop - start)


# years which we would like to get data
year_list = [2015, 2016, 2017, 2018]

# running the program
get_aggregate_myki_data_per_location(year_list)