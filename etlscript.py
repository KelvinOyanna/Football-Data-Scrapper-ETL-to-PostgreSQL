#import Libraries
import pandas as pd
import requests
import BeautifulSoup
import re
from sqlalchemy import create_engine
from sqlalchemy import text
import psycopg2
from datetime import datetime


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
    # Write data to a csv file
    combined_data.to_csv('football_data.csv', header = ['div','date','home_team','away_team','fthg','ftag'], index = False)
    


##############################################################
###   DATA TRANSFORMATION ON THE SCRAPPED/EXTRACTED DATA   ###
##############################################################

def transform_data():
    football_data = pd.read_csv('football_data.csv')
    # Define a function to convert the date column to a uniform date format
    def convert_date(value):
        #for value in date:
        if re.search(r'\d+\/\d+\/\d\d\d\d', str(value)):
            new_date = datetime.strptime(str(value), '%d/%m/%Y').date()
            return new_date
        elif re.search(r'\d+\/\d+\/\d\d', str(value)):
            new_date = datetime.strptime(str(value), '%d/%m/%y').date()
            return new_date
        else:
            pass
    
    
    football_data['date'] = football_data['date'].apply(convert_date)
    return football_data

#########################################
### LOAD DATA TO POSTGRESQL DATABASE  ###
#########################################


def load_data_to_db():
    #Create a sqlAlchemy connection. This library allows for easy loading of data to our database using the load_sql method from pandas library
    try:
        engine = create_engine('postgresql+psycopg2://{user}:{pw}@localhost/{db}'.format(user = 'username', \
        pw = 'your_password', db = 'your_db_name'))
    except ConnectionError as error:
        print('Unable to connect to the database. Check your connection info and try again!')
        print(error)
    
    # Create a table for holding the extracted data
    create_table = """
    CREATE TABLE IF NOT EXISTS football_data(
    id SERIAL PRIMARY KEY,
    div VARCHAR(5),
    date DATE,
    home_team VARCHAR(50),
    away_team VARCHAR(50),
    fthg INT DEFAULT(0),
    ftag INT DEFAULT(0));
    """

    with engine.connect() as connection:
        try:
            connection.execute(text(create_table)) 
        except psycopg2.Error as error:
            print('Unable to create table!')
            print(error)
        # get the transfored data from the transform_data function above
        football_data = transform_data()
        #load to postgresql database
        football_data.to_sql('football_data', con = engine, if_exists = 'append', index= False)
        #print(football_data.head(10))

def main():
    extract_data()
    transform_data()
    load_data_to_db()

if __name__ == '__main__':
    main()
