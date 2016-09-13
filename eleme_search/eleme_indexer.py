#!/usr/bin/env python
#-*- coding:utf-8-*-

import os
import re
import sys
import traceback
from pyes import *

#dao = Dao()
import time

def exeTime(func):
    def newFunc(*args, **args2):
        t0 = time.time()
        print "@%s, {%s} start" % (time.strftime("%X", time.localtime()), func.__name__)
        back = func(*args, **args2)
        print "@%s, {%s} end" % (time.strftime("%X", time.localtime()), func.__name__)
        print "@%.3fs taken for {%s}" % (time.time() - t0, func.__name__)
        return back
    return newFunc


class poi_seatch(object):

    def __init__(self):
        self._index = "poi_index"
        self._type = "poi_type"
        self.conn = ES('127.0.0.1:9200', timeout=3.5)#连接ES
        #获取两个切换index名
        self._alias = ['poi_index_1', 'poi_index_2']
        try:
            self.current_alias = self.conn.indices.get_alias(self._index)
        except Exception,e:
            print e
            self.current_alias = None

        if self.current_alias:
            self.current_alias = self.current_alias[0]
        else:
            self.current_alias = None

        if self.current_alias == self._alias[0]:
            self.tmp_index = self._alias[1]
        else:
            self.tmp_index = self._alias[0]
        print self.current_alias,'->',self.tmp_index


    def init_index(self):
        #定义索引存储结构
        mapping = {u'id': {'store': 'yes',
                          'type': u'long'
                            },
                  u'prefix': {'index': 'not_analyzed',
                             'store': 'yes',
                             'type': u'string'},
                  u'prd_full_name': {'index': 'analyzed',
                             'store': 'yes',
                             'type': u'string',
                             "indexAnalyzer":"ik_smart",
                             "searchAnalyzer":"ik_smart",
                             "term_vector" : "with_positions_offsets"},
					u'prd_lift': {'store': 'yes',
                          'type': u'integer'
                            },
                  u'prd_short_name': {'index': 'analyzed',
                             'store': 'yes',
                             'type': u'string',
                             "indexAnalyzer":"ik_smart",
                             "searchAnalyzer":"ik_smart",
                             "term_vector" : "with_positions_offsets"},
                  u'prd_short_name_2': {'index': 'analyzed',
                             'store': 'yes',
                             'type': u'string',
                             "indexAnalyzer":"ik_smart",
                             "searchAnalyzer":"ik_smart",
                             "term_vector" : "with_positions_offsets"},
                  u'prd_issue_company_name': {'index': 'analyzed',
                             'store': 'yes',
                             'type': u'string',
                             "indexAnalyzer":"ik_smart",
                             "searchAnalyzer":"ik_smart",
                             "term_vector" : "with_positions_offsets"},
                  u'upstream_compnay_name': {'index': 'analyzed',
                             'store': 'yes',
                             'type': u'string',
                             "indexAnalyzer":"ik_smart",
                             "searchAnalyzer":"ik_smart",
                             "term_vector" : "with_positions_offsets"},
                  #u'baidu_id': {'index': 'not_analyzed',
                  #           'store': 'yes',
                  #           'type': u'string'},
                  #u'city_name': {'index': 'not_analyzed',
                  #           'store': 'yes',
                  #           'type': u'string'},
                  #u'road_name': {'index': 'not_analyzed',
                  #           'store': 'yes',
                  #           'type': u'string'},
                  #u'address': {'index': 'analyzed',
                  #           'store': 'yes',
                  #           'type': u'string',
                  #           "indexAnalyzer":"ik_smart",
                  #           "searchAnalyzer":"ik_smart",
                  #           "term_vector" : "with_positions_offsets"},
                  ##u'address1': {'index': 'analyzed',
                  ##           'store': 'yes',
                  ##           'type': u'string',
                  ##           "indexAnalyzer":"whitespace",
                  ##           "searchAnalyzer":"whitespace",
                  ##           "term_vector" : "with_positions_offsets"},
                  #u"location": {"type": "geo_point",
                  #              "store": "yes",
                  #              "normalize": "yes",
                  #              "lat_lon": "yes"
                  #              },
                }
        self.conn.indices.delete_index_if_exists(self.tmp_index)
        self.conn.indices.create_index(self.tmp_index)#新建一个索引

        self.conn.indices.put_mapping(self._type, {'properties':mapping}, [self.tmp_index])

    def add_doc(self):
        rs = get_data()
        count = 0
        for ret in rs:
            doc = {}
            count += 1
            doc["id"] = ret['id']
            doc["prefix"] = ret['prefix']
            doc["prd_full_name"] = ret['prd_full_name']
            doc["prd_short_name"] = ret['prd_short_name']
            doc["prd_short_name_2"] = ret['prd_short_name_2']
            doc["prd_issue_company_name"] = ret['prd_issue_company_name']
            doc["upstream_compnay_name"] = ret['upstream_compnay_name']
            doc["prd_lift"] = ret['prd_lift']
            self.conn.index(doc, self.tmp_index, self._type)
            if count % 5000 == 0:
                print '--------- %s commit' % count
                self.conn.indices.refresh()

    @exeTime
    def rebuild_all(self):
        self.conn.force_bulk()
        self.conn.bulk_size = 10000
        self.conn.raise_on_bulk_item_failure = False
        
        self.add_doc()
        self.conn.indices.refresh()

    def switch_alias(self):
        if self.current_alias == self._index:
            self.conn.indices.delete_index_if_exists(self._index)
            self._switch_alias(None, self.tmp_index)
        else:
            self._switch_alias(self.current_alias, self.tmp_index)

    def _switch_alias(self, from_index_name, to_index_name):
        actions = []
        if from_index_name:
            actions.append(('remove', from_index_name, self._index, {}))
        actions.append(('add', to_index_name, self._index, {}))
        print "actions ", actions
        self.conn.indices.change_aliases(actions)

def get_data():
	from define import cursor 
	rs = []
	stop_list = []
	cursor.execute("select id, prd_short_name, prd_full_name,  prd_issue_company_name, upstream_compnay_name, prd_lift from crm_product_info where is_delete=0 and prd_full_name<>'' and prd_full_name not like '%测试%' and main_type=2")
	#cursor.execute("select id, prd_short_name, prd_full_name,  prd_issue_company_name, upstream_compnay_name, prd_lift from crm_product_info where is_delete=0 AND prd_issue_company_name='中融信托' and prd_full_name<>''")
	for ret in cursor.fetchall():
		id, prd_short_name, prd_full_name,  prd_issue_company_name, upstream_compnay_name, prd_lift = ret 
		prefix = u''
		prd_short_name_2 = prd_short_name
		segs = re.split(u'－|•|\s+|–|\-|﹒|▪|\:|：|●|\·|—', prd_full_name)
		if len(segs)>=2:
			prefix = segs[0]
			prd_short_name_2 = "".join(segs[-1])
		if prd_lift is None:
			prd_lift = -1
		#print prefix, prd_full_name, type(prd_lift), prd_lift
		#m = re.search(u'(（|\()([0-9\u4e00-\u4e5d]+[\u4e00-\u9fa5]*)(\)|）)', prd_full_name)
		#if m is not None:
		#	print id, prd_full_name, m.group(2) 

		#if re.search(u'[^\u4e00-\u9fa5]', prd_full_name) and re.search('[^a-zA-Z0-9]', prd_full_name):
		#	for token in re.findall(u'[^\u4e00-\u9fa5]', prd_full_name) :
		#		print token
		#		if re.search('[a-zA-Z0-9]', token) is None and token.encode('utf-8') not in stop_list:
		#			#print token.encode('utf-8')
		#			stop_list.append(token.encode('utf-8'))
		##if re.search(u'[\u4e00-\u9fa5]')

		rs.append({
			"id": 						id,
			"prefix": 					prefix,
			"prd_short_name" : 			prd_short_name,
			"prd_short_name_2" : 		prd_short_name_2,
			"prd_full_name" : 			prd_full_name,
			"prd_issue_company_name" : 	prd_issue_company_name,
			"upstream_compnay_name" : 	upstream_compnay_name,
			"prd_lift":					prd_lift  
			})
	return rs

if __name__ == '__main__':
    poi = poi_seatch()
    poi.init_index()
    poi.rebuild_all()
    poi.switch_alias()
    #get_data()
