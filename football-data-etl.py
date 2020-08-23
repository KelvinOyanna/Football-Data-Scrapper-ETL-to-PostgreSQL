#import Libraries
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from sqlalchemy import create_engine
from sqlalchemy import text
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
    football_data = football_data['Date'].to_datetime.dt.date # Transform the Date column from string to a Date object
    return football_data



#########################################
###   LOAD DATA TO MYSQL DATABASE     ###
#########################################


def load_data_to_db():
    #Create a sqlAlchemy connection. This library allows for easy loading of data to our database using the load_sql method from pandas library
    connection_engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user = 'admin', \
    pw = 'root@1987', db = 'sport_data'))
    
    # Create a table for holding the extracted data
    create_table = """
    CREATE TABLE IF NOT EXISTS football_data(
    Id SERIAL PRIMARY KEY,
    `Div` VARCHAR(5),
    Date DATE,
    HomeTeam VARCHAR(50),
    AwayTeam VARCHAR(50),
    FTHG INT DEFAULT(0),
    FTAG INT DEFAULT(0));
    """

    with connection_engine.connect() as connection:
        connection.execute(text(create_table))
        
    football_data = transform_data()
    #load the extracted csv data into the bet_sport_data database in mysql
    football_data.to_sql('football_data', con = connection_engine, if_exists = 'append', index= False)
    

load_data_to_db()