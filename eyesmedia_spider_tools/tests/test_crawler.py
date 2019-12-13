import unittest
import logging
import traceback
import warnings
from ptt.ptt_crawler import PttCrawler
from mobile.mobile_crawler import MobileCrawler

warnings.filterwarnings("ignore")

logging.basicConfig(format="%(asctime)s [%(threadName)s-%(process)d] %(levelname)-5s %(module)s - %(message)s",
                    level=logging.INFO)

class CrawlerTestCase(unittest.TestCase):

    def _mobile_spider(self):
        mobile_crawler = MobileCrawler(['-b', 'audio', '-i', '1']) # -b board name; -i page index
        prc = mobile_crawler.preprocessing(to_mongo=False) #已經存好json and csv

    def _ptt_spider(self):
        ptt_crawler = PttCrawler(['-b', 'joke', '-i', '6803', '6803']) # -b board name; -i page index or -a article id
        prc = ptt_crawler.preprocessing(to_csv=True, to_mongo=False) # 已經存好json, 準備存成csv


    def test_steps(self):

        try:
            self._mobile_spider()
            # self._ptt_spider()

        except:
            logging.error(traceback.format_exc())

if __name__=="__main__":

    unittest.main()






