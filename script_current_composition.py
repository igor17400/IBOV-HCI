import sys
import glob
import os
from time import sleep
from tqdm import tqdm
from loguru import logger
from selenium import webdriver
import pandas as pd
from pathlib import Path
import datetime

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))

B3_URL = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/{index_code}?language=pt-br"
SLEEP_TIME = 2

class IBOVIndex():
    def __init__(self, index_name: str):
        self.index_name = index_name
        self._target_url = B3_URL.format(index_code="IBOV")
        self.today = datetime.date.today()
        self.quarter = str(pd.Timestamp(self.today).quarter)
        self.year = str(self.today.year)

    def get_current_index_composition(self) -> pd.DataFrame:
        logger.info("Accessing {} index website...".format(self.index_name))
        try:
            # Define preferences to be used in our webdriver
            preferences = {"download.default_directory": str(CUR_DIR),
                        "download.prompt_for_download": False,
                        "directory_upgrade": True,
                        "safebrowsing.enabled": True}
            chromeOptions = webdriver.ChromeOptions()
            chromeOptions.add_argument("--window-size=1480x560")
            chromeOptions.add_experimental_option("prefs", preferences)

            # Create webdriver with pre configured preferences
            driver = webdriver.Chrome(options=chromeOptions)
            driver.get(self._target_url)
            sleep(SLEEP_TIME)

            # Download data from B3 website
            driver.find_element_by_link_text("Download").click()
            sleep(SLEEP_TIME)

            # Close webdriver
            driver.close()
            
            # Open csv file downloaded and analyze it to obtain index composition
            for file in glob.glob(str(CUR_DIR) + "/" + "*.csv"):
                file_name = file
            df_index = pd.read_csv(file_name, encoding="ISO-8859-1", sep=";",
                                engine='python', skiprows=1, skipfooter=2, thousands='.', decimal=',')

            # Delete index's file downloaded
            os.remove(file_name)

            return df_index
        except Exception as E:
            logger.error(
                "An error occured while accessing index website - {}".format(E))

    def save_ibov_current_index_csv(self):
        df_index = self.get_current_index_composition()
        df = pd.DataFrame({"symbol": df_index.index.tolist()})

        df.to_csv('./historic_composition/' + self.year + '_' + self.quarter + 'Q.csv')

    def get_first_added(self):
        path = str(CUR_DIR) + "/historic_composition/"
        df_latest_index = pd.read_csv(path + self.year + '_' + self.quarter + 'Q.csv')
        symbols = df_latest_index["symbol"].tolist()
        date_first_added = {}
        for file in os.listdir(path):
            if(file.split('.')[1] == 'csv' and 'date' not in file):
                df = pd.read_csv(path + file, encoding='utf8')
                for symbol in df["symbol"].tolist():
                    if symbol in symbols and symbol not in date_first_added:
                        date_first_added[symbol] = file.split(".")[0]

        df = pd.DataFrame.from_dict(date_first_added, orient='index', columns=['Date First Added'])
        df.reset_index(inplace=True)
        df.rename(columns = {'index':'symbol'}, inplace = True)
        df.to_csv(path + 'date_first_added_' + self.year + '_' + self.quarter + 'Q.csv')

if __name__ == "__main__":
    ibov = IBOVIndex(index_name='IBOV')
    # ibov.save_ibov_current_index_csv()
    ibov.get_first_added()