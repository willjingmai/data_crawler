# Python 爬蟲工具
- [Mobile01](https://www.mobile01.com/) and [PTT](https://www.ptt.cc/bbs/index.html) crawler.


## 環境設定
- 使用 Python 3.6
- 使用 requests, beautifulsoup

  ```
  pip3 install requests
  pip3 install beautifulsoup4
  ```
  
## 使用說明

#### Mobile01 crawler
- 設定討論板關鍵字與url連結: `mobile/topic_list.txt`
- 設定topic_list 讀取路徑: `mobile/mobile_crawler.py`

```
  # 參數設定
  mobile_crawler = MobileCrawler(['-b', '討論板名稱', '-i', '爬取總頁數'])
  prc = mobile_crawler.preprocessing(to_mongo=False) # 是否儲存至mongo
```

#### PTT crawler
```
  # 參數設定
  ptt_crawler = PttCrawler(['-b', '討論板名稱', '-i', '起始頁碼', '最後頁碼'])
  ptt_crawler = PttCrawler(['-b', '討論板名稱', '-a', '文章代碼'])  

  prc = ptt_crawler.preprocessing(to_csv=True, to_mongo=False) # 是否儲存為csv; 是否儲存至mongo
```

## 輸出格式

- `tests/data/{論壇名稱}/{討論板名稱}/{爬取頁碼}.csv`
- `tests/data/{論壇名稱}/{討論板名稱}/{爬取頁碼}.json`








