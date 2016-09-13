#!/usr/bin/env python
#-*- coding:utf-8 -*-

import os
import re
import sys
import Levenshtein
import math
import requests
from pyes import *
from pyes.filters import *
from pyes.facets import *
import traceback

INDEX_NAME = 'poi_index_1'
DOC_TYPE = "poi_type"

#conn = ES(("thrift", "127.0.0.1", "9200"), timeout=3.5)
conn = ES('127.0.0.1:9200', timeout=3.5)#连接ES
_ses = requests.session()


def cosine_similarity(s1, s2):
    excludes = [u'路', u'号', u'楼', u'弄',u'巷']
    t1 = [i for i in es_analyzer(text=s1) if i not in excludes]
    t2 = [i for i in es_analyzer(text=s2) if i not in excludes]
    #print ','.join(t1)
    #print ','.join(t2)
    terms = list(set(t1+t2))
    c1 = len(set(t1)&set(t2))
    c2 = len(terms)
    #print c1, float(c1)/c2
    return c1, float(c1)/c2
    #v1 = [0] * len(terms)
    #v2 = [0] * len(terms)
    #for i in xrange(len(terms)):
    #    if terms[i] in t1:
    #        v1[i] += 1
    #    if terms[i] in t2:
    #        v2[i] += 1
    #sum_xy = 0
    #sum_x2 = 0
    #sum_y2 = 0
    #for j in xrange(len(terms)):
    #    sum_xy += v1[j] * v2[j]
    #    sum_x2 += v1[j] ** 2
    #    sum_y2 += v2[j] ** 2
    #return sum_xy / (math.sqrt(sum_x2) * math.sqrt(sum_y2)), len(set(t1)&set(t2))

def es_analyzer(analyzer="ik_smart", text=""):
    ret = []
    payload = {"analyzer":analyzer, "pretty":"true"}
    if isinstance(text, unicode):
        text = text.encode('utf-8')
    r = _ses.get("http://localhost:9200/poi_index/_analyze", params=payload, data=text)
    if r.status_code == 200:
        if 'tokens' in r.json():
            for c in r.json()['tokens']:
                ret.append(c['token'])
    return ret

def search_poi(keyword=u'', city_name=u'', address=u'', tel='', lat=0, lon=0, radius=500, length=10, sort=""):
    ret = []
    must_f = []
    bq = []
    bq1 = []
    bq2 = []
    geo_filter = []
    if keyword:
        #full_name_q = TermQuery(u"name2", keyword, boost=10)
        #bq1.append(full_name_q)
        #name_q = TermQuery(u"name", keyword, boost=0.1)
        #bq1.append(name_q)
        for w in es_analyzer(text=keyword, analyzer="ik_smart"):
            name_q = TermQuery(u"name", w, boost=0.1)
            bq1.append(name_q)
    if address:
        for w in es_analyzer(text=address, analyzer="ik_smart"):
            addr_q = TermQuery(u"address", w, boost=0.5)
            bq1.append(addr_q)
    #else:
    #    bq1.append(MatchAllQuery())
        #single_words = " ".join([i for i in address])
        #for w in es_analyzer(text=single_words, analyzer="whitespace"):
        #    single_addr_q = TermQuery(u"address1", w, boost=0.05)
        #    bq1.append(single_addr_q)
    if city_name:
        city_name_q = TermQuery(u"city_name", city_name)
        bq2.append(city_name_q)
    if bq1:
        tq = BoolQuery(should=bq1, must=bq2)
        tq = FilteredQuery(tq, bf) if geo_filter else tq
        sort=[{'_geo_distance': {'unit': 'mi',
                            'order': 'asc',
                            'location': {"lat":lat, "lon":lon}}}]
        s = Search(tq)
        #s = Search(tq, sort=sort) if geo_filter else Search(tq, sort="_score")
        #print s.serialize()
        try:
            resultset = conn.search_raw(s, INDEX_NAME, DOC_TYPE, start=0, size=length)
            for row in resultset['hits']['hits']:
                r = row['_source']
                row = { 'id'    : r['id'],
                'name'  : r['name'],
                'road_name'  : r['road_name'],
                'city_name'  : r['city_name'],
                'address'  : r['address'],
                'distance'  : -1,
                #'score' : row["_score"]
                'baidu_id': r['baidu_id']
                }
                if address and r['address']:
                    row['intersection'], row['rate'] = cosine_similarity(address, r['address'])
                    #row['ratio'], row['intersection'] = cosine_similarity(address, r['address'])
                    r_name = r['address']
                    if isinstance(address, unicode):
                        address = address.encode('utf-8')
                    if isinstance(r_name, unicode):
                        r_name = r_name.encode('utf-8')
                    address = re.sub('\.|\s+|\(|\)|（|）| ', '', address)
                    r_name = re.sub('\.|\s+|\(|\)|（|）| ', '', r_name)
                    row['ratio'] = Levenshtein.ratio(address.lower(), r_name.lower())
                if lat>0 and lon>0:
                    row['distance'] = get_distance(lat, lon, r['location']['lat'], r['location']['lon'])
                ret.append(row)
            rs = {'cost':resultset.took, 'total': resultset['hits']['total'], 'data': ret}
            return rs
        except Exception,e:
            print traceback.format_exc()
            #print "keyword: %s\n%s" % (keyword, traceback.format_exc())
            #pass
    return {'data': ret, 'total': 0}

def get_data():
	from define import cursor 
	rs = []
	stop_list = []
	cursor.execute("select id, prd_short_name, prd_full_name,  prd_issue_company_name, prd_lift from crm_product_info where is_delete=0 and prd_full_name<>'' and prd_full_name not like '%测试%'")
	#cursor.execute("select id, prd_short_name, prd_full_name,  prd_issue_company_name, upstream_compnay_name, prd_lift from crm_product_info where is_delete=0 AND prd_issue_company_name='中融信托' and prd_full_name<>''")
	for ret in cursor.fetchall():
		id, prd_short_name, prd_full_name,  prd_issue_company_name, upstream_compnay_name, prd_lift = ret 
		prefix = u''
		prd_short_name_2 = prd_short_name
		segs = re.split(u'－|•|\s+|–|\-|﹒|▪|\:|：|●|\·|—', prd_full_name)
		if len(segs)>=2:
			prefix = segs[0]
			prd_short_name_2 = "".join(segs[1:])
		if prd_lift is None:
			prd_lift = -1


	from collections import defaultdict
	frequency = defaultdict(int)
	for text in texts:
	    for token in text:
	        frequency[token] += 1
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

if __name__ == '__main__':
