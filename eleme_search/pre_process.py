#!usr/bin/env python
#-*- coding:utf-8 -*-

import logging
from define import cursor
import re
import traceback
import json
import os
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

from gensim import corpora, models

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

from eleme_searcher import es_analyzer

if __name__ == '__main__':
	from pprint import pprint
	texts = []
	_dict = {}
	sql = "select id, prd_full_name, prd_short_name, prd_issue_company_name from crm_product_info where prd_full_name<>'' and is_delete=0 and main_type=2" 
	#sql = "select id, prd_full_name, prd_short_name, prd_issue_company_name from crm_product_info where prd_full_name<>'' and is_delete=0 and prd_issue_company_name like '%金牡丹%'" 
	cursor.execute(sql)
	for ret in cursor.fetchall():
		id, prd_full_name, prd_short_name, prd_issue_company_name = ret
		#segs = re.split(u'－|•|\s+|–|\-|﹒|▪|\:|：|●|\·|—', prd_full_name)
		#if len(segs)>=2:
		#	prefix = segs[0] 
		#	prd_short_name = "".join(segs[-1])
		segments = es_analyzer(text=prd_short_name)
		#print ",".join(segments)
		if len(segments)<=4:
			token = prd_short_name 
		elif len(segments)>=5:
			token = "".join(segments[:5])
		#print id, prd_full_name, '----->',  token
		if token in _dict:
			_dict[token] +=1 
		else: 
			_dict[token] = 1

	sort_list = sorted(_dict.iteritems(), key=lambda d:d[1], reverse=True)

	for i in sort_list[:100]:
		token, fre = i
		print token, fre
	#pprint(texts)
	#dictionary = corpora.Dictionary(texts)
	#print dictionary
	#_dict = {}
	#for token, id in dictionary.token2id.iteritems():
	#	_dict[id] = token
	#	print token, id
	##print(dictionary.token2id)
	#_s = sorted(dictionary.dfs.iteritems(), key=lambda d:d[1], reverse=True)
	##for i in _s[:100]:
	##	print _dict.get(i[0]), i[1]
	##from collections import defaultdict
	##frequency = defaultdict(int)
	##for text in texts:
    ##	for token in text:
    ##    	frequency[token] += 1
	##
	#corpus = [dictionary.doc2bow(text) for text in texts]
	#for i in corpus:
	#	print i
