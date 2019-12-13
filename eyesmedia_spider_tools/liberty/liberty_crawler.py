# vim: set ts=4 sw=4 et: -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function

import logging
import re
import json
import requests
import argparse
import time
import codecs
from bs4 import BeautifulSoup
from six import u
import os
#from data_processing import PttProcessing

logger = logging.getLogger("eyesdeeplearning")

__version__ = '1.0'

class LibertyCrawler(object):

    def __init__(self, cmdline=None):
        parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                         description='''
                                         Liberty_times-crawler-version1
                                         Input: board name 
                                         Output: board_name_datetime.json
                                         ''')
        parser.add_argument('-b', metavar='BOARD_NAME', help='Board name', required=True)
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('-i', metavar=('START_INDEX', 'END_INDEX'), type=int, nargs=2, help="Start and end index")
        group.add_argument('-a', metavar='ARTICLE_ID', help="Article ID")
        parser.add_argument('-v', '--version', action='version', version='%(prog)s ' + __version__)



        if cmdline:
            args = parser.parse_args(cmdline)
        else:
            args = parser.parse_args()

        self.board = args.b
        self.page_index = args.i
        self.article_id = args.a
        self.base_url = 'https://www.ptt.cc'
        self.data_dict = {"articles": None}
        self.data_list = []
        self.filename = None