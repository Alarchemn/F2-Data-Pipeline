import pandas as pd
from bs4 import BeautifulSoup
import requests
import time

# Race id's per season
season_2017_id = ['972','974','975','976','977','979','978','980','981','982','973']
season_2018_id = ['983','984','984','985','986','987','988','989','990','991','992','993','994']
season_2019_id = ['1007','996','997','1008','999','1000','1001','1002','1003','1004','1005','1006']
season_2020_id = ['1012','1021','1015','1014','1022','1010','1016','1017','1023','1024','1025','1026']
season_2021_id = ['1027','1028','1029','1030','1031','1032','1033','1034']
season_2022_id = ['1035','1036','1037','1038','1039','1040','1041','1042','1049','1043','1044','1045','1046','1048']
season_2023_id = ['1050','1051','1052','1053','1055','1056']

all_race_ids = []
all_race_ids.extend(season_2017_id)
all_race_ids.extend(season_2018_id)
all_race_ids.extend(season_2019_id)
all_race_ids.extend(season_2020_id)
all_race_ids.extend(season_2021_id)
all_race_ids.extend(season_2022_id)
all_race_ids.extend(season_2023_id)



def extract_data(raceid):
    """
    Extracts data from the FIA Formula 2 website for the given race IDs.
    
    Args:
        raceid (str): str with the race IDs. unfortunately they do not have a clear order
        
    Returns:
        pandas.DataFrame: Extracted data as a DataFrame.
        whit data from Race, Sprint Race/Practice2, Practice1 and Qualy of the event
    """ 

    ENDPOINT = 'http://www.fiaformula2.com/Results?raceid='
    # Create an empty DataFrame to store the extracted data
    df = pd.DataFrame()  
    # Request the data
    url = requests.get(ENDPOINT + raceid)
    # Create a BeautifulSoup object  
    soup = BeautifulSoup(url.text, 'html.parser')    
    # Read HTML tables from the web page
    raw = pd.read_html(url.text)  
    
    circuit = soup.find(class_='country-circuit').text  # Extract circuit information
    schedule = soup.find(class_='schedule').text  # Extract schedule information
    tables = soup.find_all('table')  # Find all tables on the web page (4 tables)
    events = soup.find_all(class_='collapsible-header')  # Find all collapsible headers (Event information)
    
    for index_out in range(len(tables)):
        pos = tables[index_out].find_all('div', class_='pos')  # Find all pilot race positions in the table
        car_no = tables[index_out].find_all('div', class_='car-no')  # Find car numbers in the table
        names = tables[index_out].find_all('div', class_='driver-name')  # Find driver names in the table

        event = events[index_out].find('span').text  # Extract event information
        
        # Create empty lists to store extracted data for each row
        pos_data = []
        car_no_data = []
        driver_name_data = []
        team_name_data = []
        circuit_data = []
        schedule_data = []
        event_data = []

        for index_in in range(len(pos)):
            # Append extracted data to the respective lists
            pos_data.append(pos[index_in].text)
            car_no_data.append(car_no[index_in].text)
            driver_name_data.append(names[index_in].find_all(class_='visible-desktop-up')[0].text)
            team_name_data.append(names[index_in].find_all(class_='team-name')[0].text)
            circuit_data.append(circuit)
            schedule_data.append(schedule)
            event_data.append(event)

        raw[index_out].drop('POSNumber / Driver and TeamNo / Driver', axis=1, inplace=True)
        raw[index_out]['POS'] = pos_data
        raw[index_out]['CAR'] = car_no_data
        raw[index_out]['PILOT NAME'] = driver_name_data
        raw[index_out]['TEAM'] = team_name_data
        raw[index_out]['CIRCUIT'] = circuit_data
        raw[index_out]['SCHEDULE'] = schedule_data
        raw[index_out]['TYPE'] = event_data

        # Concatenate each table (Race, Sprint Race/Practice2, Practice1 and Qualy) data to the full DataFrame
        df = pd.concat([df, raw[index_out]], ignore_index=True)
    
    return df  

def transform_data(df):
    """
    Transforms the given DataFrame by extracting and formatting specific columns.

    Args:
        df (pandas.DataFrame): The DataFrame to be transformed.

    Returns:
        None
    """

    round_num = df['SCHEDULE'].apply(lambda x: x.split('|')[0])  # Extract round number from 'SCHEDULE' column
    date = df['SCHEDULE'].apply(lambda x: x.split('|')[1].split('-')[1])  # Extract date from 'SCHEDULE' column (Only the race date)

    df.drop('SCHEDULE', axis=1, inplace=True)  # Drop unnecessary columns from the DataFrame

    df['ROUND'] = round_num  # Add 'ROUND' column with extracted round numbers
    df['DATE'] = pd.to_datetime(date)  # Convert extracted date to datetime format

    return None

# ---------------------------------------------------------------------------------------
# --------------------------- T E S T  Z O N E-------------------------------------------
# ---------------------------------------------------------------------------------------


#for id in race_ids:
#    df = extract_data(race_id)
#    transform_data(df)
#    df.to_csv(f'DATA/season_{first_season}.csv',index=False)
#    first_season += 1

# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------
