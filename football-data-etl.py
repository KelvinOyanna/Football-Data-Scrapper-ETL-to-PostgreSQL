#import Libraries
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from sqlalchemy import create_engine
import mysql.connector

#######################################################
###   EXTRACT - SCRAPE DATA FROM FOOTBALL WEBSITE   ###
#######################################################

def scrape_data():
    # Use the request library to scrape data from the specified link
    web_data = requests.get('https://www.football-data.co.uk/englandm.php')

    #Create a BeautifulSoup object to clean & extract our target data
    soup = BeautifulSoup(web_data.content, 'html.parser')
    links = soup.find_all('a')
    
    """
    Identify links containing the CSV data and save in a list
    Only football data in the CSV formats below are considered:
    - https://www.football-data.co.uk/mmz4281/1920/E0.csv
    - https://www.football-data.co.uk/mmz4281/1920/E2.csv
    - https://www.football-data.co.uk/mmz4281/0203/E1.csv

    """
    # A list to aggregate the matched/desired csv links
    csv_links = []
    for link in links:
        if re.search(r'mmz\d+\/\d+\/(E0|E1|E2)\.csv', str(link)):
            csv_link = re.search(r'mmz\d+\/\d+\/(E0|E1|E2)\.csv', str(link)).group()
            csv_link = 'https://www.football-data.co.uk/'+ csv_link
            csv_links.append(csv_link)
        else:
            continue
    return csv_links

##############################################################################
###       STRUCTURE AND TRANSFORM SCRAPPED DATA TO REQUIRED FORMAT         ###
##############################################################################

#Read data from the csv links and merge into one data file
def extract_data():
    scrapped_links = scrape_data() # Create an object to recieve scrapped data
    datafiles = []
    data_columns = ['Div','Date','HomeTeam','AwayTeam','FTHG','FTAG'] #This is a list of the specific columns of data required
    # Iterate through scrapped csv links, genetate dataframes and combine into a unified dataframe
    for link in scrapped_links:
        csv_data = pd.read_csv(link,usecols = data_columns,sep = ',', engine = 'python')
        datafiles.append(csv_data)
    combined_data = pd.concat(datafiles, axis=0, ignore_index=True) # Merge all data from each csv file into a single dataframe
    # Export extracted data to a stagging CSV file in append mode
    combined_data.to_csv('Football_data.csv', mode = 'a', header= combined_data.tell() == 0)


##############################################################
###   DATA TRANSFORMATION ON THE SCRAPPED/EXTRACTED DATA   ###
##############################################################

def transform_data():
    football_data = pd.read_csv('Football_data.csv')
    football_data['Date'].to_datetime.dt.date # Transform the Date column from string to a Date object

#########################################
###   LOAD DATA TO MYSQL DATABASE     ###
#########################################




def load_data_to_mysql():
    #Create a Mysql connection to use a database
    connection = mysql.connector.connect(host = 'localhost', user = 'your_username', password = 'your_password', database = 'your_database_name')
    #Create a cursor object for executing sql queries
    cursor = connection.cursor()

    #Create a mysql database
    cursor.execute('CREATE DATABASE IF NOT EXISTS sport_data')

    #Connect to the newly created database
    connection = mysql.connector.connect(host = 'localhost', user = 'your_username', password = 'your_password', database = 'bet_sport_data')
    cursor = connection.cursor()
    #Create a table named football in the database created above
    
    create_table = """
    CREATE TABLE IF NOT EXISTS football(
    Id VARCHAR(50) DEFAULT(0),
    `Div` VARCHAR(5),
    Date DATE,
    HomeTeam VARCHAR(155),
    AwayTeam VARCHAR(155),
    FTHG INT DEFAULT(0),
    FTAG INT DEFAULT(0));
    """
    #Execute the above sql query
    cursor.execute(create_table)
    #Commit changes made to the database
    connection.commit()

    #Create a sqlAlchemy connection. This library allows for easy loading of data to our database using the load_sql method from pandas library
    connection_engine = create_engine("mysql://{user}:{pw}@localhost/{db}".format(user = 'your_username', pw = 'your_password', db = 'bet_sport_data'))

    #load the extracted csv data into the bet_sport_data database in mysql
    sport_data.to_sql('football', con = connection_engine, if_exists = 'append', index= False)
    connection.commit()


    #Call the load data function above to perform the load data operation
    #load_data_to_mysql()

    ####################################################################################################
    ###   PERFORM UPDATE ON THE DATABASE - SET PRIMARY KEY AND ASSIGN UUID VALUES TO THE ID COLUMN   ###
    ####################################################################################################

    # Update the ID column with UUID values
    new_id = """
    UPDATE football
    SET Id = uuid();
    """
    cursor.execute(new_id)

    #Set the ID column as the primary key
    set_primary_key = """
    ALTER TABLE football
    ADD PRIMARY KEY(Id);
    """
    cursor.execute(set_primary_key)

    # Commit all changes to the database
    connection.commit()
    # Close connection and Cursor
    cursor.close()
    connection.close()

