#!usr/bin/env python
#-*- coding:utf-8 -*-

import re
import traceback
import json
import sys
sys.path.append('/home/chenyanpeng/common')
from model import *
from define import *
db_conn = new_session()

import redis
rc = redis.StrictRedis(host='localhost', port=6379, db=0)

from get_logger import *
mylogger = get_logger('doc_sims.log', logging.INFO, logging.getLogger('doc_sims'))
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


from gensim import corpora, models, similarities

TMP_ROOT = "/home/chenyanpeng/tmp"

class DocSimilarity(object):

    def __init__(self):
        self.corpus = get_corpus()
        #self.rid2name = json.loads(rc.get('rid2name'))
        #self.index2category = json.loads(rc.get('index2category'))

    def string2vector(self):
        texts = self.corpus
        dictionary = corpora.Dictionary(texts)
        dictionary.save('%s/food_category_0405.dict' % TMP_ROOT)

        corpus = [dictionary.doc2bow(text) for text in texts]
        corpora.MmCorpus.serialize('%s/food_category_0405.mm' % TMP_ROOT, corpus)

    def build(self, num_topics, num_features):
        dictionary = corpora.Dictionary.load('%s/food_category_0405.dict' % TMP_ROOT)
        corpus = corpora.MmCorpus('%s/food_category_0405.mm' % TMP_ROOT)
        lsi = models.LsiModel(corpus, id2word=dictionary, num_topics=num_topics)
        lsi.save('%s/model.lsi' % TMP_ROOT)
        index = similarities.MatrixSimilarity(lsi[corpus], num_features=num_features, num_best=3)
        index.save('%s/category.index' % TMP_ROOT)

def get_corpus():
    index2category = {}
    corpus = []
    #with open('category_corpus_0515.txt') as f:
    with open('category_corpus_0514.txt') as f:
        lines = f.readlines()
        for _index in xrange(len(lines)):
            line = lines[_index]
            segs = re.split('\t| ', line.rstrip())
            index2category[_index] = segs[0]
            doc = [w for w in segs[1].decode('utf-8').split(u',') if len(w) <= 5]
            doc = list(set(doc))
            corpus.append(doc)
    rc.set("index2category", json.dumps(index2category))
    return corpus


category_weight = {u"清真/新疆菜" : 0.85,
                    u"快餐" : 1.06,
                    u"咖啡" : 0.8,
                    u"面包甜点" : 0.8,
                    u"韩国料理" : 1,
                    u"东北菜" : 1,
                    u"川湘菜" : 1.03,
                    u"特色小吃" : 1.06,
                    u"日韩料理" : 1,
                    u"江浙菜" : 1.03,
                    u"西餐" : 0.95,
                    u"粤菜" : 1.05,
                    u"东南亚菜" : 0.85}

def classification():
    rid2name = json.loads(rc.get('rid2name'))
    index2category = json.loads(rc.get('index2category'))
    dictionary = corpora.Dictionary.load('/home/chenyanpeng/tmp/food_category_0405.dict')
    _dict = {}
    for token, id in dictionary.token2id.iteritems():
        _dict[id] = token
    lsi = models.LsiModel.load("%s/model.lsi" % TMP_ROOT)
    index = similarities.MatrixSimilarity.load('%s/category.index' % TMP_ROOT)
    #for ret in db_conn.execute("select * from ele_food_segments_2 where id in (65880)"):
    for ret in db_conn.execute("select * from ele_food_segments_2 where id not in (select restaurant_id from eleme_category)"):
    #for ret in db_conn.execute("select * from ele_food_segments_2 where id not in (select restaurant_id from eleme_category) order by rand() limit 100"):
        k = ret[0]
        segments = ret[1]
        #segments = ret.segments
        #if k in test_restaurant_ids:
        doc = [w for w in json.loads(segments) if len(w) <= 5]
        #print ",".join(doc)
        vec_bow = dictionary.doc2bow(doc)
        if len(vec_bow) >= 8:
            #print "餐馆名: ", rid2name.get(str(k)), "\n构建特征词query: ", ",".join([_dict.get(w[0]) for w in vec_bow])
            vec_lsi = lsi[vec_bow] # convert the query to LSI space
            sims = index[vec_lsi]
            tmp_sims = {}
            for s in sims:
                _category = index2category.get(str(s[0]))
                if s[1] >= 0.9:
                    #if _category in category_weight:
                    #    tmp_sims[_category] = s[1] * category_weight.get(_category)
                    #else:
                    tmp_sims[_category] = s[1]
            _sort = sorted(tmp_sims.iteritems(), key=lambda d:d[1], reverse=True)
            if len(_sort) >= 1 and _sort[0][0].encode('utf-8') in ['快餐', '特色小吃']:
                print "%s\t%s\t%s" % (k, rid2name.get(str(k)).encode('utf-8'), _sort[0][0].encode('utf-8'))
                #print "%s\t%s\t%s" % (k, rid2name.get(str(k)).encode('utf-8'), ",".join(["%s:%s" % (i[0].encode('utf-8'), i[1]) for i in _sort]))
        #else:
        #    print k, rid2name.get(str(k)).encode('utf-8'), ",".join(doc)
        #print rid2name.get(str(k)), ",".join(doc)
        #print "****", ",".join(doc)
        #print '----->', ",".join(["%s :%s" % (i[0], i[1]) for i in _sort]) , "\r\n"
        #sims = sorted(enumerate(sims), key=lambda item: -item[1])
        ##print k, rid2name.get(str(k)), ",".join(["%s: %s" % (index2category.get(str(i[0])), i[1]) for i in sims[:3] if i[1]>=0.5])
        #print '------------\r\n'
        #print "-------->", ",".join(["%s: %s" % (index2category.get(str(i[0])), i[1]) for i in sims if i[1]>=0.5])

            

if __name__ == '__main__':
    mymodel = DocSimilarity()
    mymodel.string2vector()
    mymodel.build(8, 1901)
    classification()
