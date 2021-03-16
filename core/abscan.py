#!/usr/bin/env python
#-*- coding:UTF-8 -*-

import os, sys

BASEDIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASEDIR)

import ConfigParser
import pymssql
import json
import Queue
import threading
from elasticsearch import Elasticsearch
from jinja2 import Environment, FileSystemLoader

from conf import settings
from query import SQLBuild, ESBuild
from logger import Logger

LOG = Logger('abnormalscan')


class Ascan(object):
    def __init__(self, scan_source, interval):
        self._getConfig()
        self._connDB()
        self._thread_num = settings.PARAMETERS['Mssql_Query_Thread']
        self._guid_list_queue = Queue.Queue(0)
        self._abscan_queue = Queue.Queue(0)
        self.scan_source = json.dumps(settings.SCAN_SOURCE_MAPPING[scan_source])
        self.es_query_interval = int(interval)
        self.buildSQL = SQLBuild(self.scan_source)

    def _getConfig(self):
        config = ConfigParser.RawConfigParser()
        config.read(settings.Config_file)
        ##mssql_server_info##
        self.mssql_server = config.get('mssql', 'server')
        self.mssql_user = config.get('mssql', 'user')
        self.mssql_passwd = config.get('mssql', 'passwd')
        self.mssql_database = config.get('mssql', 'database')
        ##es_server_info##
        self.es_server = config.get('elasticsearch', 'server')
        self.es_port = config.get('elasticsearch', 'port')

    def _connDB(self):
        try:
            conn = pymssql.connect(server=self.mssql_server,
                                   user=self.mssql_user,
                                   password=self.mssql_passwd,
                                   database=self.mssql_database)
            return conn
        except Exception as e:
            LOG.error('db error: %s' % e)
            exit(1)

    def pagingQuery(self):
        cursor = self._connDB().cursor()
        cursor.execute(self.buildSQL.countMainSQL())
        row_count = cursor.fetchone()[0]
        self._connDB().close()

        if row_count:
            paging = []
            page_start = 0
            page_querysize = row_count / self._thread_num
            page_remainder = row_count % self._thread_num

            for i in range(self._thread_num):
                page_start = page_start + page_querysize
                page_end = page_querysize * i
                m_v = page_querysize
                n_v = page_end
                paging.append((m_v, n_v))
            remainder_page = page_querysize * self._thread_num
            paging.append((page_remainder, remainder_page))

            sqllist = []
            for lmtquery in paging:
                sql = self.buildSQL.queryMainSQL(lmtquery[0], lmtquery[1])
                sqllist.append(sql)
            return sqllist

    def queryGuidList(self, pagingSQL):
        cursor = self._connDB().cursor()
        cursor.execute(pagingSQL)
        for row in cursor:
            user_guid = row[0]
            self._guid_list_queue.put(user_guid)

    def getQueue_queryES(self):
        while not self._guid_list_queue.empty():
            GUID = self._guid_list_queue.get(block=False)
            self.queryES(GUID)

    def queryES(self, GUID):
        try:
            esquery = ESBuild(self.scan_source, self.es_query_interval)
            es = Elasticsearch(host=self.es_server, port=self.es_port)
            query_body = esquery.queryES(GUID)
            res = es.search(body=query_body)
            result = res["aggregations"]["scan_sources"]["buckets"]
            LOG.info(GUID)
            if result == []:
                self._abscan_queue.put(GUID)
        except Exception as e:
            LOG.error(e)
            exit(1)

    def treadQueryMsSQLData(self):
        LOG.info('start query MSSQL ..')
        thread_list = []
        for sql in self.pagingQuery():
            thread_run = threading.Thread(target=self.queryGuidList, args=(sql,))
            thread_list.append(thread_run)
            thread_run.start()
        for th in thread_list:
            th.join()
        self._connDB().close()

    def threadQueryESData(self):
        LOG.info('start query ES ..')
        thread_list = []
        for i in range(settings.PARAMETERS['ES_Query_Thread']):
            thread_run = threading.Thread(target=self.getQueue_queryES())
            thread_list.append(thread_run)
            thread_run.start()
        for th in thread_list:
            th.join()

    def recordAbnormallisttoFile(self,scansource,data):
        file_name = scansource + '.list'
        file_path = os.path.join(settings.RECORDDIR, file_name)
        with open(file_path, 'w') as f:
            data = json.dumps(data)
            f.write(data)

    def filterLastAbscanfromFile(self,scansource):
        file_name = scansource + '.list'
        file_path = os.path.join(settings.RECORDDIR, file_name)
        with open(file_path, 'r') as f:
            fr = f.read()
            recorddata = json.loads(fr)
            return recorddata

    def getAbscanResult(self,scansource,datasource):
        if datasource is False:
            origin_guid_list = []
            while not self._abscan_queue.empty():
                origin_guid = self._abscan_queue.get(block=False)
                origin_guid_list.append(origin_guid)
            uniq_guid = set(origin_guid_list)
            self.recordAbnormallisttoFile(scansource,list(uniq_guid))
        elif datasource is True:
            uniq_guid = self.filterLastAbscanfromFile(scansource)
        return uniq_guid

    def queryAbscanDataInfo(self,scansource,datasource):
        cursor = self._connDB().cursor()
        user_info_list = []
        for guid in self.getAbscanResult(scansource,datasource):
            sql_1 = self.buildSQL.getAbscanUserInfo(guid)[0]
            cursor.execute(sql_1)
            row = cursor.fetchone()
            user_info = {'Register_Email': row[0], 'USER_DISPLAYNAME': row[1], 'COMPANY_GUID': row[2],
                         'EXPIRATION_DATA': row[3], 'ACTIVATION_CODE': row[4], 'SEATS': row[5],
                         'SCANSOURCE': scansource}

            sql_2 = self.buildSQL.getAbscanUserInfo(guid)[1]
            cursor.execute(sql_2)
            mailcount = cursor.fetchone()[0]
            user_info['MAILBOX_COUNT'] = mailcount

            sql_3 = self.buildSQL.getAbscanUserInfo(guid)[2]
            cursor.execute(sql_3)
            policycount = cursor.fetchone()[0]
            user_info['POLICY_ENABLE_COUNT'] = policycount
            user_info_list.append(user_info)
        self._connDB().close()
        return user_info_list

