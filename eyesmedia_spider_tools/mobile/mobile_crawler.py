# coding = utf-8
import logging
import codecs
import csv
import json
import datetime
import time
import argparse
import re
import sys
import requests
from bs4 import BeautifulSoup
from data_processing import MobileProcessing  

logger = logging.getLogger("eyesdeeplearning")

class MobileCrawler(object):

    def __init__(self, cmdline=None):

        parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                         description='''
                                         mobile-crawler
                                         Input: board name and page indices
                                         Output: BOARD_NAME-START_INDEX-END_INDEX.json
                                         ''')
        parser.add_argument('-b', metavar='BOARD_NAME', help='Board name', required=True)
        parser.add_argument('-i', metavar='CRAWEL_PAGES', help="Pages index to crawel", required=True)

        if cmdline:
            args = parser.parse_args(cmdline)
        else:
            args = parser.parse_args()


        self.board_idx = args.b
        self.page_index = args.i
        self.base_url = 'https://www.mobile01.com/'
        self.filename =  'mobile01_' + self.board_idx + '_' + str(self.page_index)
        self.topic_dict_path = "/Users/Chives/eyes/eyesmedia_spider_tools/mobile/topic_list.txt"
        self._crawel(to_csv=True, to_json=True)

    def preprocessing(self, to_mongo=True):
        fname = './data/'+ self.filename + '.csv'
        prc = MobileProcessing()
        res_dict = prc.processing(filename=fname, to_mongo=to_mongo)
        return res_dict

    def _crawel(self, to_csv=True, to_json=True):
        topic_dict = self.read_topic(self.topic_dict_path)

        total_page_num = int(self.get_total_page_num(topic_dict[self.board_idx][0]))
        logger.info(u"Topic {{ {} }} has {} pages in total.".format(topic_dict[self.board_idx][1], total_page_num))
        page_want_to_crawl = min(int(self.page_index), total_page_num)
        logger.info("Page index for crawling:{}".format(page_want_to_crawl))

        start = time.time()
        posts = self.get_posts(page_want_to_crawl, topic_dict[self.board_idx][0])

        posts_data = self.get_articles(posts)
        logger.info(r"Craweling time cost:{}".format(time.time() - start))

        data_lists = self.clean_content(posts_data)
        
        if to_csv:
            start = time.time()
            self.save_csv(data_lists)
            logger.info(u"csv saving time cost:{}".format(time.time()-start))

        if to_json:
            start = time.time()
            self.save_json(data_lists)
            logger.info(u"json saving time cost:{}".format(time.time() - start))

    def get_page_content(self, url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36'}
        res = requests.get(url, headers=headers)
        content = BeautifulSoup(res.text, features="html5lib")
        return content  # 把html爬下來

    def get_all_topic(self, url):
        content = self.get_page_content(url)

        all_topic = content.select_one('#top-menu').select('li')
        all_topic = [each for each in all_topic if
                     'topiclist' in each.find('a')['href'] or 'waypointtopiclist' in each.find('a')['href']]

        topic_dict = dict()
        idx = 0
        with open('topic_list_.txt', 'w', encoding='utf-8') as file:
            for each in all_topic:
                topic_link = each.find('a')['href']
                topic_page = self.get_page_content('https://www.mobile01.com/' + topic_link)
                nav = topic_page.select('p.nav')[0].text
                start = nav.find('»')
                topic_name = nav[start + 1:].lstrip().rstrip()
                while ' » ' in topic_name:
                    topic_name = topic_name.replace(' » ', '>')
                while ' ' in topic_name:
                    topic_name = topic_name.replace(' ', '')

                topic_dict[str(idx)] = [topic_link, topic_name]
                file.write(f'{idx} {topic_link} {topic_name}\n')
                logging.info(f'{idx} {topic_link} {topic_name}\n')
                idx += 1
        return topic_dict

    def read_topic(self, file):
        topic_dict = dict()
        with codecs.open(file, 'r', encoding='utf-8', errors='ignore') as file:
            for line in file:
                topic = line.replace('\n', '').split(' ')
                topic_dict[topic[0]] = [topic[1], topic[2]]
        file.close()
        return topic_dict

    def get_total_page_num(self, url):
        content = self.get_page_content('https://www.mobile01.com/' + url)
        # pagination = content.select('div.pagination') # "[1][2][3]...下一頁"這行y
        page_link = content.select('.l-tabulate__action .l-pagination__page a')
        if True:
            if page_link:
                last_page = page_link[-1]['href']  # 該行的最後一個按鈕就是最後一頁的網址
                replace = '/' + url + '&p='
                total_page = last_page.replace(replace, '')  # 把前面的字串過濾掉，只要網址後面的數字 (頁數)
            else:
                total_page = 1
        else:
            total_page = -1
        return total_page  # type = str

    def dim(self, a):
        if not type(a) == list:
            return []
        return [len(a)] + self.dim(a[0])

    def get_posts(self, page_num, url): # 取得從第 1 頁到第 page_num 頁的所有文章網址
        posts = list()
        url = url.strip()
        for i in range(1, 1 + page_num):  # 汽車綜合討論區共20頁
            logging.info("Processing page index: {}".format(i))
            # 回傳每一討論文的: 網址\標題\時間\作者\回應文數量
            content = self.get_page_content( self.base_url + url + '&p=' + str(i))
            all_link = [link.get('href') for link in content.find_all('a', {'class': 'c-link u-ellipsis'})]  # 文章網址
            all_title = [title.text for title in content.find_all('a', {'class': 'c-link u-ellipsis'})]  # 文章標題
            all_date = [date.string for date in content.find_all('div', {'class': 'o-fNotes'})]  # 文章發布日期
            all_author = [author.string for author in content.find_all('div', {'class': None})]  # 作者
            all_reply = [reply.string for reply in content.find_all('div', {'class': 'o-fMini'})]  # 回覆量
            all_author[:] = all_author[::2]  # delete repeat author name
            all_date[:] = all_date[::2]  # delete repeat date time
            # logging.info("Title:{}".format(all_title))

            links = content.find_all('a', {'class': 'c-link u-ellipsis'})  # 每一頁文章網址合集

            link_list = []
            app = {}
            app.setdefault('message', [])
            for link in links:
                link_list.append(link.get('href'))

            for j in range(len(link_list)):  # 去一頁頁地抓每一討論文的回應(存到message)
                logger.info("Processing article index: {}/{}".format(j+1, len(link_list)))
                msg, tmplist = [], []
                message_link = self.base_url + link_list[j]  # 每一個討論文的url,只有第一頁，要再加&p=2~pagination
                # logging.info("Processing message pages: {}".format(message_link))
                push_content = self.get_page_content(message_link)  # 獲取每一個討論文的第一頁url 的 html --> 為了拿pagination
                pagination_list = [page.text for page in push_content.find_all('a', {'class': 'c-pagination'})]
                pagination = pagination_list[-1] if pagination_list else 0  # 挑出pagination
                push_ipdatetime, push_userid, all_push_content = [], [], []
                for p in range(1, int(pagination) + 1):  # 每一討論文的回應文的頁數(=pagination)，這邊會去一頁頁地抓回應
                    message_link = self.base_url + link_list[j] + '&p=' + str(p)  # 更改message_link, 應該要從1~pagination
                    push_content = self.get_page_content(message_link)  # 更改push_content
                    all_push_content.extend([push.text.strip().strip("\n") for push in push_content.find_all('article',{'class': 'u-gapBottom--max c-articleLimit'})])
                    tmp_ipdatetime = [push_datetime.text.strip().strip("\n") for push_datetime in
                                      push_content.find_all('span', {'class': 'o-fNotes o-fSubMini'})]
                    if p == 1:
                        tmp_ipdatetime = tmp_ipdatetime[4:]  # 拿掉發文者時間和發文者id
                    else:
                        tmp_ipdatetime = tmp_ipdatetime[2:]  # 第二頁之後的回應也會把發文者ipdatetime紀錄進去，所以要pop掉

                    push_ipdatetime.extend([tmp_ipdatetime[i] for i in range(len(tmp_ipdatetime)) if i % 2 == 0])  # remove #1, #2, #3 ...
                    tmp_userid = [userid.text.strip().strip("\n") for userid in push_content.find_all('a', {'class': 'c-link c-link--gn u-ellipsis'})]
                    if p == 1 and j == 0:
                        tmp_userid.pop(0)  # 把發文者id拿掉，第二頁之後就沒有這個問題
                    elif p == 1 and j != 0:
                        tmp_userid.pop(0)
                        tmp_userid.pop(0)
                    push_userid.extend([tmp_userid[i] for i in range(len(tmp_userid)) if i % 2 == 0])  # 拿掉重複的回文者id
                    for k in range(len(push_userid) - 1):  # 把資訊放進msg_dict
                        msg_dict = {}
                        msg_dict['push_content'] = all_push_content[k]
                        msg_dict['push_ipdatetime'] = push_ipdatetime[k]
                        msg_dict['push_userid'] = push_userid[k]
                        msg.append(msg_dict)
                tmplist.append(msg)
                app['message'].append(tmplist[0])
            app['article_url'] = all_link
            app['article_title'] = all_title
            app['date'] = all_date
            app['author'] = all_author
            app['reply'] = all_reply
            posts.append(app)
            # logger.info("{} pages have been scraped.".format(len(posts)))

        return posts

    def parse_get_article(self, url):
        soup = self.get_page_content(self.base_url + url)
        origin = soup.find('article', {'class': 'l-publishArea topic_article'})  # 文章內文在<'article', {'class':'l-publishArea topic_article'> 底下
        if origin:
            content = str(origin)
            # replace <br>, <br\> and '\n' with a whitespace
            content = re.sub("<br\s*>", " ", content, flags=re.I)  #
            content = re.sub("<br\s*/>", " ", content, flags=re.I)  #
            content = re.sub("\n+", " ", content, flags=re.I)  #

            # remove hyperlink
            content = re.sub("<a\s+[^<>]+>(?P<aContent>[^<>]+?)</a>", "\g<aContent>", content, flags=re.I)
            content = BeautifulSoup(content, features="html5lib")
            content = ' '.join(content.text.lstrip().rstrip().split())
        else:
            content = 'None'

        return content

    def clean_regex(self, org):  # convert org_content to content
        org_contents = []
        for i in range(len(org)):
            for j in range(len(org[i])):
                org_contents.append(str(org[i][j]['org_content']))
        pat = "[\s+\d+\W+]"
        url_pat = "https?:\/\/.\/.[\d+]+"
        datetime_pat = "[0-9]{4}\-[0-9]{2}\-[0-9]{2}[\s+]*[0-9]{2}:[0-9]{2}"
        for i in range(len(org_contents)):
            org_contents[i] = re.sub(pat, "", org_contents[i], flags=re.I)
            org_contents[i] = re.sub(url_pat, "", org_contents[i], flags=re.I)
            org_contents[i] = re.sub(datetime_pat, "", org_contents[i])
        return org_contents

    def get_articles(self, post_list):
        # logging.info("post list:{}".format(post_list))
        articles = list()
        for i in range(len(post_list)):
            tmp = []
            for j in range(len(post_list[i]['article_title'])):
                tmp.append({
                    'article_title': post_list[i]['article_title'],
                    'article_url': post_list[i]['article_url'],
                    'author': post_list[i]['author'],
                    'message_count': {'all': post_list[i]['reply']},
                    'org_content': self.parse_get_article(post_list[i]['article_url'][j]),
                    'ner_content': (post_list[i]['article_title'][j] + self.parse_get_article(post_list[i]['article_url'][j])),
                    'messages': post_list[i]['message'],
                    'date': post_list[i]['date']
                })
            articles.append(tmp)
        return articles

    def clean_content(self, posts):

        titles, links, authors, replies, dates = [], [], [], [], []
        for i in range(len(posts)):  # 將article_title, article_url, author, message_count, date 分別存成list
            titles.extend(posts[i][0]['article_title'])
            links.extend(posts[i][0]['article_url'])
            authors.extend(posts[i][0]['author'])
            replies.extend(posts[i][0]['message_count']['all'])
            dates.extend(posts[i][0]['date'])

        org_contents, ner_contents, messages = [], [], []
        for i in range(len(posts)):  # 將org_content, ner_content, content, messages 分別存成list
            for j in range(len(posts[i])):
                org_contents.append(str(posts[i][j]['org_content']))
                ner_contents.append(str(posts[i][j]['ner_content']))
                content = self.clean_regex(posts)
                messages.append(posts[i][0]['messages'][j])

        return [titles, links, authors, replies, dates, org_contents, ner_contents, messages, content]

    def save_csv(self, data_lists):

        self.save_file = 'data/' + self.filename + '.csv'
        logger.info("Start saving data to {}".format(self.save_file))

        titles = data_lists[0]
        links = data_lists[1]
        authors = data_lists[2]
        replies = data_lists[3]
        dates = data_lists[4]
        org_contents = data_lists[5]
        ner_contents = data_lists[6]
        messages = data_lists[7]
        content = data_lists[8]

        with open(self.save_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['article_title', 'board', 'article_source', 'mdy_date', 'crt_date', 'article_url', 'author',
                          'message_count', 'org_content', 'ner_content', 'content', 'messages', 'date']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for i in range(len(titles)):
                row = {
                    'article_title': titles[i],
                    'board': self.board_idx,
                    'article_source': 'mobile01',
                    'mdy_date': datetime.datetime.utcnow(),
                    'crt_date': datetime.datetime.utcnow(),
                    'article_url': self.base_url + links[i],
                    'author': authors[i],
                    'message_count': replies[i],
                    'org_content': org_contents[i],
                    'ner_content': ner_contents[i],
                    'content': content[i],
                    'messages': messages[i],
                    'date': dates[i]
                }
                writer.writerow(row)

        csvfile.close()

    def save_json(self, data_lists):

        self.save_file = 'data/' + self.filename + '.json'
        logger.info("Start saving data to {}".format(self.save_file))

        titles = data_lists[0]
        links = data_lists[1]
        authors = data_lists[2]
        replies = data_lists[3]
        dates = data_lists[4]
        org_contents = data_lists[5]
        ner_contents = data_lists[6]
        messages = data_lists[7]
        content = data_lists[8]

        with open(self.save_file, 'w', encoding='utf-8') as json_file:
            for i in range(len(titles)):
                row = {
                    'article_title': titles[i],
                    'board': self.board_idx,
                    'article_source': 'mobile01',
                    'mdy_date': str(datetime.datetime.utcnow()),
                    # datatime is not JSONSerializable, convert it to string
                    'crt_date': str(datetime.datetime.utcnow()),
                    'article_url': self.base_url + links[i],
                    'author': authors[i],
                    'message_count': replies[i],
                    'org_content': org_contents[i],
                    'ner_content': ner_contents[i],
                    'content': content[i],
                    'messages': messages[i],
                    'date': dates[i]
                }
                json.dump(row, json_file, indent=4, sort_keys=True, ensure_ascii=False)
        json_file.close()

if __name__ == '__main__':
    MobileCrawler()
    #MobileCrawler(['-b', 'living', '-i', '1'])
    # MobileCrawler()._crawel(to_csv=True,to_json=True)
    # MobileCrawler().preprocessing(to_mongo=True)

