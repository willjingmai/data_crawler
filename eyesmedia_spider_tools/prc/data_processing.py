# -*- coding：utf-8 -*-
import logging
from pymongo import MongoClient
import dateutil.parser as parser
import datetime
import pandas as pd
import json, re
import ast


logger = logging.getLogger("eyesdeeplearning")

mongo_info = {
    #"address": "54.199.184.205",
    "address":"localhost:27017",
    "account": "nlubot",
    "password": "28010606",
    "database": "nlubot_dictionary",
    "collection": "nlu_corpus_source",
    "authentication": "SCRAM-SHA-1"
}


class PttProcessing(object):

    def __init__(self):

        self.collection = None
        self.df_path = None

    def processing(self, filename, to_csv=False, to_mongo=True):

        content = self._read_json(filename)
        res_dict = self._data_prc(content)
        df = pd.DataFrame.from_dict(res_dict, orient='columns')

        if to_csv:
            self.df_path = re.sub(r"\.json$", ".csv", filename)
            df.to_csv(self.df_path, index=False)
            logger.info("Write preprocessed data to {} success.".format(self.df_path))

        if to_mongo:
            self.collection = mongo_conn()
            logger.info("Ready to write processed ptt data to mongo. collection name:{}".format(mongo_info["collection"]))
            insert_mongo(res_dict, self.collection)

        return df

    def _clean_regex(self, sent):

        ptt_pat = "發信站|批踢踢實業坊\(ptt.cc\)|來自: ([0-9]+\.)+[0-9]+|Sent from JPTT on my iPhone"
        list_pat = "[0-9]+\. ?"
        date_pat = "[0-9]+(年|月|日){1}|([0-9]+/){1,2}[0-9]+|([0-9]+\.){1,2}[0-9]+"
        time_pat = "[0-9]+(點|分|秒){1}|([0-9]+:){1,2}[0-9]+"
        phone_pat = "\([0-9]+\)[0-9]{6,}|09[0-9]{8}|[0-9]{2}-[0-9]{6,}"
        punc_pat = "[\.\!\/_,:$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）：；《）《》“”()»〔〕-]+"
        url_pat = "(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?"
        space_pat = "\s+"

        sent = re.sub(ptt_pat, "", sent)
        sent = re.sub(list_pat, "", sent)
        sent = re.sub(date_pat, "", sent)
        sent = re.sub(time_pat, "", sent)
        sent = re.sub(phone_pat, "", sent)
        sent = re.sub(url_pat, "", sent)
        sent = re.sub(punc_pat, "", sent)
        sent = re.sub(space_pat, " ", sent)
        return sent

    def _data_prc(self, data_list):
        for data in data_list:
            data["content"] = self._clean_regex(data["org_content"])
            data["ner_content"] = str(data["article_title"]) + str(data["org_content"])
            data["crt_date"] = datetime.datetime.utcnow()
            data["mdy_date"] = datetime.datetime.utcnow()
            if data["date"]:
                data["date"] = parser.parse(data["date"])
            else:
                data["date"] = None
        return data_list

    def _read_json(self, file_path):
        with open(file_path) as datas:
            content = json.load(datas, encoding="utf-8")
        data_list = content["articles"]  # unlist
        return data_list

class MobileProcessing(object):

    def __init__(self):

        self.collection = None
        self.df_path = None

    def processing(self, filename, to_mongo=True):

        res_dict = self._data_prc(filename)

        if to_mongo:
            self.collection = mongo_conn()
            logger.info("Ready to write processed ptt data to mongo. collection name:{}".format(mongo_info["collection"]))
            insert_mongo(res_dict, self.collection)

        return res_dict


    def _data_prc(self, filename):
        df = pd.read_csv(filename)
        # delete row if author = Mobile01系統服務員 or Mobile01活動小組
        df = df[df["author"] != "Mobile01系統服務員"]
        df = df[df["author"] != "Mobile01活動小組"]
        df = df[df["message_count"] <= 500]

        # convert fake list to list in df["message"]
        msg_list = []
        for x in df["messages"]:
            msg_list.append(ast.literal_eval(x))
        df["messages"] = msg_list
        # revise count error
        msg_count_list = []
        for msg, msg_count in zip(df["messages"], df["message_count"]):
            count = {"all": 0}
            if len(msg) == 0:
                msg_count_list.append(count)
            else:
                count["all"] = msg_count
                msg_count_list.append(count)
        df["message_count"] = msg_count_list

        # convert df to dict
        dict_list = df.to_dict("record")
        for x in dict_list:
            x["crt_date"] = parser.parse(x["crt_date"])
            x["mdy_date"] = datetime.datetime.utcnow()
            x["date"] = parser.parse(x["date"])

        return dict_list

def mongo_conn():

    client = MongoClient(mongo_info["address"],
                         username=mongo_info["account"],
                         password=mongo_info["password"],
                         authSource=mongo_info["database"],
                         authMechanism=mongo_info["authentication"])

    db = client[mongo_info["database"]]
    collection = db[mongo_info["collection"]]
    logger.info("mongo conn success. collection name: {}".format(mongo_info["collection"]))
    return collection


def insert_mongo(dict_list, collection):

    if not dict_list:
        logger.warning("data save to {} is empty...".format(mongo_info["collection"]))
        return
    else:
        collection.insert_many(dict_list)
        logger.info("save list to {} success, action count is {}".format(mongo_info["collection"], len(dict_list)))

def update_mongo(dict_list, update=False):

    if not dict_list:
        logger.warning("data save to {} is empty...".format(mongo_info["collection"]))
        return
    if update != True:
        logger.warning("Fail update data. please reset update param value as True")
    else:
        collection = mongo_conn()
        idx = 0
        for dicts in dict_list:
            idx += 1
            myquery = {'article_id': dicts['article_id']}
            newvalue = dicts
            collection.update(myquery, newvalue, upsert=True)
            logger.info("idx:{} ; {}".format(idx, myquery))
        logger.info("update list to {} success, action count is {}".format(mongo_info["collection"], len(dict_list)))


if __name__=="__main__":

    filename = "/Users/Chives/workspace/crawler/ptt/ptt_data/car_5006_5006.json"
    prc = PttProcessing()
    res = prc.processing(filename, to_csv=False, to_mongo=True)