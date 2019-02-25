# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import sys
import hashlib
import os

import difflib

from scrapy.conf import settings
from scrapy.exceptions import DropItem
from scrapy.http import Request

#import MySQLdb 
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

from patent_util import PatentUtil


class DuplicatesPipeline(object):
    """ Cleanse data of any duplicates """ 
    
    def __init__(self):
        self.patent_nums_seen = set()
        self.patent_names_seen = set()


    def process_item(self, item, spider):
        
        # check for duplicate patent numbers
        if tuple(item.get('patent_num', '')) in self.patent_nums_seen:        
            raise DropItem("Duplicate patent number found: %s" % item)
        else:            
            self.patent_nums_seen.add(item)
            
        # check for duplicate titles 
        if tuple(item.get('patent_name', '')) in self.patent_names_seen:        
            raise DropItem("Duplicate title found: %s" % item)
        else:            
            self.patent_names_seen.add(item)    
        return item

"""Helper functions for patent processing"""

class PatentData(object):
    """ Various functions for cleansing patent extracted data. """ 
    def __init__(self):
        pass
    
    def check_string_diff_ratio(self, a,b):
        ''' 
        Returns a value [0,1] that corresponds to how similar the strings
        are to each other.
        '''
        return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()


class PatentUtil(object):
    """ General utility functions for patent scraping. """ 
        
    def generate_us_patent_urls(self):
        ''' 
        Generate a list of start URLs for the Spider based on predefined
        search terms. Outputs text file used by Spider for crawling. 
        '''        
        search_terms = self.define_search_terms()
        
        # cycle through all search terms to create start url list
        start_url = "http://patft.uspto.gov/netacgi/nph-Parser?Sect1=PTO2&Sect2=HITOFF&u=%2Fnetahtml%2FPTO%2Fsearch-adv.htm&r=0&p=1&f=S&l=50&Query="
        end_url = "&d=PTXT"
        
        search_url_list = []
        for i in range(len(search_terms)):
            search_terms[i] = search_terms[i].replace("/", "%2f")
            search_terms[i] = search_terms[i].replace("(", "%28")
            search_terms[i] = search_terms[i].replace(")", "%29")
            search_terms[i] = search_terms[i].replace(" ", "+")
            search_url_list.append(start_url + search_terms[i] + end_url + "\n")
         
        return search_url_list
    
    def define_search_terms(self):
        '''
        Function that returns a list of all hardcoded search terms.
        '''
        search_term_list = []
        
        search_term_list.append("abst/(quantum and (error) and (correction or correcting))")        
        search_term_list.append("abst/(qubit or qubits)")        
        search_term_list.append("abst/(single and photon)")
        search_term_list.append("abst/(quantum and (computation or computing or computer))")
        search_term_list.append("abst/(quantum and (entangled or entanglement or entangling))")
        search_term_list.append("abst/(quantum and (single-photon))")
        search_term_list.append("abst/(quantum and (key or keys or cipher))")
        search_term_list.append("abst/(quantum and (information))")
        search_term_list.append("abst/(quantum and (communication))")
        search_term_list.append("abst/(quantum and (random) and (number))")
        search_term_list.append("abst/(quantum and (encryption or cryptography or cryptographic))")
        search_term_list.append("abst/(polarization and (entangled) and (photon or photons))")
        search_term_list.append("abst/(polarization-entangled)")
        search_term_list.append("abst/(quantum and (qkd))")
        search_term_list.append("abst/(quantum and (repeater))")
        search_term_list.append("abst/(entangled and (photon) and (source))")
        search_term_list.append("abst/(entangled and (particles or systems or states or state))")
        search_term_list.append("abst/(quantum and (nitrogen or NV) and (center or centers))")
        search_term_list.append("abst/(quantum and (single) and (photon) and (detector))")
        search_term_list.append("abst/(quantum and (superposition))")
                    
        return search_term_list
        
        
    def write_file(self, file_name, file_ext, content):
        '''
        Write file of specified extension in local directory. 
        '''
        if "." not in file_ext:
            file_ext = "." + file_ext
        print ("Writing file %s%s...") % (file_name, file_ext)
        with open(file_name+file_ext, 'w') as out_file:
            out_file.write(content)
        print ("Done.")
        
        
    def list_2_str(self, _list):
        '''Converts a list of objects into a concatenation of strings.'''
        return ' '.join(map(str, _list))        
#class MysqlOutputPipeline(object):
#    """ Pipeline into MySQL database """
#    
#    def __init__(self):
#        dispatcher.connect(self.spider_opened, signals.spider_opened)
#        dispatcher.connect(self.spider_closed, signals.spider_closed)
#        self.files = {}
#
#
#    def connect(self):
#        try:
#            self.conn = MySQLdb.connect(
#            host='localhost',
#            user='root',
#            passwd='pass',
#            db='patents_db'
#            )
#            #port=22)
#        except (AttributeError, MySQLdb.OperationalError), e:
#            raise e
#            
#
#    def query(self, sql, params=()):
#        try:
#            cursor = self.conn.cursor()
#            cursor.execute(sql, params)
#        except (AttributeError, MySQLdb.OperationalError) as e:
#            print 'exception generated during sql connection: ', e
#            self.connect()
#            cursor = self.conn.cursor()
#            cursor.execute(sql, params)
#        return cursor
#
#
#    def spider_opened(self, spider):
#        self.connect()
#
#        
#    def spider_closed(self, spider):
#        pass
#
#
#    def process_item(self, item, spider):
#
#        # INSERT PATENT NAME        
#        #sql = """INSERT INTO us_patents_tb(patent_name) VALUES(%s) """    
#        #patent_name = item['patent_name']
#        #self.query(sql, patent_name)
#        
#
#        # INSERT DOCUMENT ID
#        sql = """INSERT INTO us_patents_tb(patent_name, document_identifier) VALUES(%s, %s) """    
#        params = (item['patent_name'], item['document_identifier'])
#        self.query(sql, params)
#        
#        self.conn.commit()      
#        return item
#      
##    # clean_name
##    clean_name = ''.join(e for e in item['store'] if e.isalnum()).lower()
##
##    # conditional insertion in store_meta
##    sql = """SELECT * FROM store_meta WHERE clean_name = %s"""
##    curr = self.query(sql, clean_name)
##    if not curr.fetchone():
##      sql = """INSERT INTO store_meta (clean_name) VALUES (%s)"""
##      self.query(sql, clean_name)
##      self.conn.commit()
##
##    # getting clean_id
##    sql = """SELECT clean_id FROM store_meta WHERE clean_name = %s"""
##    curr = self.query(sql, clean_name)
##    clean_id = curr.fetchone()
##
##    # conditional insertion in all_stores
##    sql = """SELECT * FROM all_stores WHERE store_name = %s"""
##    curr = self.query(sql, item['store'])
##    if not curr.fetchone():
##      sql = """INSERT INTO all_stores (store_name,clean_id) VALUES (%s,%s)"""
##      self.query(sql, (item['store'], clean_id[0]))
##      self.conn.commit()
##
##    # getting store_id
##    sql = """SELECT store_id FROM all_stores WHERE store_name =%s"""
##    curr = self.query(sql, item['store'])
##    store_id = curr.fetchone()
##
##    if item and not item['is_coupon'] 
##            and (item['store'] in ['null', ''] 
##            or item['bonus'] in ['null', '']):
##      raise DropItem(item)
##    if item and  not item['is_coupon']:# conditional insertion in discounts table
##      sql = """SELECT * 
##               FROM discounts 
##               WHERE mall=%s 
##               AND store_id=%s 
##               AND bonus=%s 
##               AND deal_url=%s"""
##      curr = self.query(
##               sql, 
##               (
##                 item['mall'], 
##                 store_id[0], 
##                 item['bonus'], 
##                 item['deal_url']
##               )
##             )
##      if not curr.fetchone():
##        self.query(
##          "INSERT INTO discounts 
##           (mall,store_id,bonus,per_action,more_than,up_to,deal_url) 
##           VALUES (%s,%s,%s,%s,%s,%s,%s)",
##           (item['mall'],
##           store_id[0],
##           item['bonus'],
##           item['per_action'],
##           item['more_than'],
##           item['up_to'],
##           item['deal_url']),
##        )
##        self.conn.commit()
##
##    # conditional insertion in crawl_coupons table
##    elif spider.name not in COUPONS_LESS_STORE: 
##      if item['expiration'] is not 'null':
##        item['expiration']=datetime.strptime(item['expiration'],'%m/%d/%Y').date()
##        sql = """SELECT * 
##               FROM crawl_coupons 
##               WHERE mall=%s 
##               AND clean_id=(SELECT clean_id FROM store_meta WHERE clean_name = %s) 
##               AND coupon_code=%s 
##               AND coupon_text=%s 
##               AND expiry_date=%s"""
##      curr = self.query(
##               sql,
##               (
##                 item['mall'], 
##                 clean_name, 
##                 item['code'], 
##                 self.conn.escape_string(item['discount']), 
##                 item['expiration']
##               )
##             )
##      if not curr.fetchone():
##          sql = """INSERT INTO crawl_coupons 
##                   (mall,clean_id,coupon_code,coupon_text,expiry_date) 
##                   VALUES (
##                     %s,
##                     (SELECT clean_id FROM store_meta WHERE clean_name = %s),
##                     %s,
##                     %s,
##                     %s
##                   )"""
##          self.query(
##            sql, 
##            (
##              item['mall'], 
##              clean_name, 
##              item['code'], 
##              self.conn.escape_string(item['discount']), 
##              item['expiration']
##            )
##          )
##          self.conn.commit()
#         
