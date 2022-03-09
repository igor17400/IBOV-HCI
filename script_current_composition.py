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
        df = pd.DataFrame({"código": df_index.index.tolist()})

        today = datetime.date.today()
        quarter = str(pd.Timestamp(today).quarter)
        year = str(today.year)
        df.to_csv('./historic_composition/' + year + '_' + quarter + 'Q.csv')

if __name__ == "__main__":
    ibov = IBOVIndex(index_name='IBOV')
    ibov.save_ibov_current_index_csv()