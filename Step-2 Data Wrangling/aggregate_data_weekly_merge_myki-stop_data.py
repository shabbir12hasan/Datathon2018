import os
import re as re
import pandas as pd

# Declaring folders for Sample 0 and Sample 1 - Scan On and Off
sample_files_Off_0 = os.listdir("d:/studymaterial/datathonmelb_2018/samp_0/ScanOffTransaction")
sample_files_On_0 = os.listdir("d:/studymaterial/datathonmelb_2018/samp_0/ScanOnTransaction")
sample_files_Off_1 = os.listdir("d:/studymaterial/datathonmelb_2018/samp_1/ScanOffTransaction")
sample_files_On_1 = os.listdir("d:/studymaterial/datathonmelb_2018/samp_1/ScanOnTransaction")

# this method will clean the stop data file, which have StopId amd their description
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


# This method will retrieve data from all weeks in a year, in all 4 folder- ie, from samp 0-1 and scan on-off
# get data for week 26, take 4 files of week 26 from 4 folders and create one data frame
# combine that data frame with location data
# merge and aggregate and save into one data file
# do this process for all weeks in all input years
# generate one main csv file
def get_aggregate_myki_data_per_location(year_list):

    # assigning stop data into df
    stop_data_df = stop_data()
    final_df = stop_data_df.copy()

    # iterate for all years
    for year in year_list:

        # getting list of weeks in a single year folder
        week_list = os.listdir("d:/studymaterial/datathonmelb_2018/samp_1/scanontransaction/" + str(year))

        # iterating for each week folder
        for week in week_list:

            # Getting text file from Samp_0- ScanOnTransaction
            files_inside_week = os.listdir("d:/studymaterial/datathonmelb_2018/Samp_0/ScanOnTransaction/" + str(year) +"/" + str(week))
            for txt_file in files_inside_week:
                fileName_Samp_0_On = "d:/studymaterial/datathonmelb_2018/Samp_0/ScanOnTransaction/" + str(year) +"/" +str(week) + "/" + txt_file
                df_0_On = pd.read_csv(fileName_Samp_0_On, delimiter="|", header=None)
                # print("fileName_Samp_0_On:    ", fileName_Samp_0_On)

            # Getting text file from Samp_0- ScanOffTransaction
            files_inside_week = os.listdir("d:/studymaterial/datathonmelb_2018/Samp_0/ScanOffTransaction/" + str(year) + "/" + str(week))
            for txt_file in files_inside_week:
                fileName_Samp_0_Off = "d:/studymaterial/datathonmelb_2018/Samp_0/ScanOffTransaction/" + str(year) + "/" + str(week) + "/" + txt_file
                df_0_Off = pd.read_csv(fileName_Samp_0_Off, delimiter="|", header=None)
                # print("fileName_Samp_0_Off:    ", fileName_Samp_0_Off)

            # Getting text file from Samp_1- ScanOnTransaction
            files_inside_week = os.listdir("d:/studymaterial/datathonmelb_2018/samp_1/ScanOnTransaction/" + str(year) +"/" + str(week))
            for txt_file in files_inside_week:
                fileName_Samp_1_On = "d:/studymaterial/datathonmelb_2018/samp_1/ScanOnTransaction/" + str(year) +"/" +str(week) + "/" + txt_file
                df_1_On = pd.read_csv(fileName_Samp_1_On, delimiter="|", header=None)
                # print("fileName_Samp_1_On:    ", fileName_Samp_1_On)

            # Getting text file from Samp_1- ScanOffTransaction
            files_inside_week = os.listdir("d:/studymaterial/datathonmelb_2018/samp_1/ScanOffTransaction/" + str(year) + "/" + str(week))
            for txt_file in files_inside_week:
                fileName_Samp_1_Off = "d:/studymaterial/datathonmelb_2018/samp_1/ScanOffTransaction/" + str(year) + "/" + str(week) + "/" + txt_file
                df_1_Off = pd.read_csv(fileName_Samp_1_Off, delimiter="|", header=None)
                # print("fileName_Samp_1_Off:    ", fileName_Samp_1_Off)

            # Appeding all 4 files into 1 df
            # Will hold data for tap on and tap off for sample 1 and sample 0 for a particular week
            df = df_0_On.append(df_0_Off)
            df = df.append(df_1_On)
            df = df.append(df_1_Off)

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
            print(weekName)
            week_data.columns = ['StopID', weekName]

            final_df = pd.merge(final_df, week_data, how='left', on=["StopID"])

            # break
        # break
    final_df.to_csv("aggregated_stop_data_new_weekname.csv", sep=',', index=False)


# years which we would like to get data
year_list = [2015, 2016, 2017, 2018]

# running the program
get_aggregate_myki_data_per_location(year_list)