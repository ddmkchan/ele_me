#!usr/bin/env python
#-*- coding:utf-8 -*-

import sys
sys.path.append('/home/chenyanpeng/common')
from define import *
from model import *
import re
import traceback
import json
import redis
rc = redis.StrictRedis(host='localhost', port=6379, db=0)

import os
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

from gensim import corpora, models

db_conn = new_session()

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


import mmseg

#mmseg.Dictionary.load_dictionaries()
mmseg.Dictionary.load_words('%s/feature.dict' % PROJECT_ROOT)

def get_rid_2_name():
    # to redis
    _dict = {}
    with open('%s/restaurant.txt' %  PROJECT_ROOT) as f:
        for line in f.readlines():
            segs = line.rstrip().split('\t')
            _dict[segs[0]] = segs[1]
    print len(_dict)
    rc.set('rid2name', json.dumps(_dict))

def get_corpus():
    _dict = {}
    f1 = get_data_from_files('category_corpus.txt')
    for line in f1:
        segs = line.rstrip().split('\t')
        _dict[segs[0]] = segs[1]
    f1 = get_data_from_files('0422_corpus')
    for line in f1:
        segs = line.rstrip().split('\t')
        if segs[0] in _dict:
            terms = ",".join([_dict.get(segs[0]), segs[1]])
            print "%s\t%s" % (segs[0], terms)

def save2mysql():
    # 根据餐厅名对餐厅进行简单菜系分类，并存入mysql
    count = 0
    #rid2name = json.loads(rc.get('rid2name'))
    #category2resturant = json.loads(rc.get('category2resturant'))
    #_dict = {}
    #for k, v in category2resturant.iteritems():
    #    #print k, ",".join([rid2name.get(str(i)) for i in v[:10]])
    #    for id in v:
    #        if id in _dict:
    #            _dict[id].append(k)
    #        else:
    #            _dict[id] = [k]
    #for id, categories in _dict.iteritems():
    #    try:
    #        for c in categories:
    #            item = ElemeCategory(**{'restaurant_id': id, 'name': rid2name.get(str(id)), 'category': c })
    #            count += 1
    #            db_conn.merge(item)
    #            if count % 5000 == 0:
    #                print '%s commit ' % count
    #                db_conn.commit()
    #    except Exception,e:
    #        print e
    #        db_conn.rollback() 

    with open('match_rs_0515.txt') as f:
        for line in f.readlines():
            try:
                segs = line.rstrip().split('\t')
                if len(segs) == 3:
                    id = int(segs[0])
                    item = ElemeCategory(**{'restaurant_id': id, 'name':segs[1], 'category':segs[2]}) 
                    count += 1
                    db_conn.merge(item)
                    if count % 5000 == 0:
                        print '%s commit ' % count
                        db_conn.commit()
            except Exception,e:
                print line, e
                db_conn.rollback() 
    db_conn.commit()

def get_custom_dict():
    s1 = set([])
    for ret in db_conn.execute("select * from ele_food_segments where id in (166657, 88229, 136737,31281)"):
        k = ret[0]
        segments = json.loads(ret[1])
        print k, ",".join(segments)

def ele_food_segments():
    #对每家餐厅的菜品进行分词，提取特征词, 并存入mysql
    _dict = {}
    with open('/home/chenyanpeng/food2') as f:
        for l in f.readlines():
            try:
                segs = l.rstrip().split('\t')
                if segs[0] not in _dict:
                    _dict[segs[0]] = [segs[1]]
                else:
                    _dict[segs[0]].append(segs[1])
            except Exception,e:
                print str(e), l.rstrip()
    count = 0
    for id,v in _dict.iteritems():
        words = ",".join(v)
        r = list(set([i.text for i in mmseg.Algorithm(words.decode('utf-8')) if len(i.text)>=2 and re.search('\d+', i.text) is None]))
        #r = ",".join(list(set([i.text for i in mmseg.Algorithm(words.decode('utf-8')) if len(i.text)>=2])))
        #print id, ",".join(list(set([i.text for i in mmseg.Algorithm(words.decode('utf-8')) if len(i.text)>=2 and re.search('\d+', i.text) is None])))
        item = EleFoodSegment(**{'id':id, "segments":json.dumps(r)})
        db_conn.merge(item)
        count += 1
        if count % 5000 == 0:
            print "%s \t commit" % count
            db_conn.commit()
    db_conn.commit()

def basic_categorize():
    #设定规则，根据餐厅名进行简单的菜系分类
    category2feature = {}
    category2resturant = {}
    with open('%s/0513_custom.dict' % PROJECT_ROOT) as f:
        for line in f.readlines():
            category, feature = line.rstrip().split()
            category2feature[category.decode('utf-8')] = feature.decode('utf-8')
    rid2name = json.loads(rc.get('rid2name'))
    for rid, rname in rid2name.iteritems():
        rid = int(rid)
        for c,f  in category2feature.iteritems():
            if re.search(f, rname) is not None:
                if c in category2resturant:
                    category2resturant[c].append(rid)
                else:
                    category2resturant[c] = [rid]
    for k,v in category2resturant.iteritems():
        print k, len(v)
        #print ",".join(v), "\r\n"
    rc.set('category2resturant', json.dumps(category2resturant))


def get_terms_of_category():
    #取出每个菜系top250的特征词
    corpus = []
    rid2name = json.loads(rc.get('rid2name'))
    category2resturant = json.loads(rc.get('category2resturant'))
    index2category = {}
    _index = 0
    for k, v in category2resturant.iteritems():
        if k not in [u'火锅香锅',u'烧烤',u'粥店',u'麻辣烫',u'生煎锅贴',u'饺子馄饨',u'披萨',u'炸鸡汉堡',u'米粉面馆']:
            texts = []
            ids = ",".join([str(i) for i in v])
            sql = "select * from ele_food_segments_2 where id in (%s)" % ids
            for ret in db_conn.execute(sql):
                terms = [w for w in json.loads(ret[1]) if len(w) <= 5]
                texts.append(terms)
            dictionary = corpora.Dictionary(texts)
            _s = sorted(dictionary.dfs.iteritems(), key=lambda d:d[1], reverse=True)
            _dict = {}
            for token, id in dictionary.token2id.iteritems():
                _dict[id] = token
            corpus.append([_dict.get(i[0]) for i in _s[:1000]])
            index2category[_index] = k
            _index += 1
        #print "%s\t%s" % (k.encode('utf-8'), ",".join([_dict.get(i[0]).encode('utf-8') for i in _s[:250]]))
    dictionary = corpora.Dictionary(corpus)
    _dict.clear
    for token, id in dictionary.token2id.iteritems():
        _dict[id] = token
        #print id, token
    features = set([])
    for id, df in dictionary.dfs.iteritems():
        #print _dict.get(i[0]).encode('utf-8'), i[1]
        if df <= 4:
            features.add(_dict.get(id))
    for i in xrange(len(corpus)):
        _category = index2category.get(i)
        print "%s %s" % (_category.encode('utf-8'), ",".join([k.encode('utf-8') for k in corpus[i] if k in features]))


def clean():
    index2category = {}
    corpus = []
    rs = []
    #with open('category_corpus_0514.txt') as f:
    with open('514_category.txt') as f:
        lines = f.readlines()
        print len(lines)
        for _index in xrange(len(lines)):
            line = lines[_index]
            segs = re.split('\t| ', line.rstrip())
            print segs[0], len(segs)
            index2category[_index] = segs[0]
            doc = [w for w in segs[1].decode('utf-8').split(u',') if len(w)<=5]
            corpus.append(doc)
    rc.set("index2category", json.dumps(index2category))
    dictionary = corpora.Dictionary(corpus)
    _dict = {}
    for token, id in dictionary.token2id.iteritems():
        _dict[id] = token
    _s = sorted(dictionary.dfs.iteritems(), key=lambda d:d[1], reverse=True)
    features = set([])
    for i in _s:
        if i[1]<=6:
        #print _dict.get(i[0]).encode('utf-8'), i[1]
            features.add(_dict.get(i[0]))
    for i in xrange(len(corpus)):
        _category = index2category.get(i)
        print '\n'
        print "%s %s" % (_category, ",".join([k.encode('utf-8') for k in corpus[i] if k in features]))


def get_cutomer_dict():
    _list = []
    with open('%s/0513_custom.dict' % PROJECT_ROOT) as f:
        for line in f.readlines():
            category, feature = line.rstrip().split()
            _list.extend(feature.split('|'))
    for k in _list:
        print len(k.decode('utf-8')), k

def feature_count():
    corpus = []
    with open('%s/restaurant.txt' % PROJECT_ROOT) as f:
        for line in f.readlines():
            id, restaurant = line.rstrip().split('\t')
            corpus.append([i.text for i in mmseg.Algorithm(restaurant.decode('utf-8')) if len(i.text) >= 2])
    dictionary = corpora.Dictionary(corpus)
    _dict = {}
    for token, id in dictionary.token2id.iteritems():
        _dict[id] = token
    _s = sorted(dictionary.dfs.iteritems(), key=lambda d:d[1], reverse=True)
    for i in _s[:100]:
        print _dict.get(i[0]), i[1]

def handle_unknown_category():
    corpus = []
    rid2name = json.loads(rc.get('rid2name'))
    for ret in db_conn.execute("select * from ele_food_segments_2 where id not in (select restaurant_id from eleme_category)"):
        k = ret[0]
        segments = ret[1]
        doc = [w for w in json.loads(segments) if len(w) <= 5]
        restaurant = rid2name.get(str(k))
        print k, rid2name.get(str(k)), ",".join(doc)
        #print (restaurant, '')
    #    corpus.append([i.text for i in mmseg.Algorithm(restaurant) if len(i.text) >= 2])
    #dictionary = corpora.Dictionary(corpus)
    #_dict = {}
    #for token, id in dictionary.token2id.iteritems():
    #    _dict[id] = token
    #_s = sorted(dictionary.dfs.iteritems(), key=lambda d:d[1], reverse=True)
    #for i in _s[:100]:
    #    print _dict.get(i[0]), i[1]


def add_category():
    count = 0
    with open('bod_id') as f:
        bods = set([int(i.rstrip()) for i in f.readlines()])
    for ret in db_conn.query(ElemeCategory).all():
        try:
            if ret.category in [u'东北菜',u'川湘菜',u'粤菜', u'清真/新疆菜']:
                item = ElemeCategory(**{'restaurant_id': ret.restaurant_id, 'name':ret.name, 'category':u'中式炒菜'}) 
                count += 1
                db_conn.merge(item)
                if count % 5000 == 0:
                    print '%s commit ' % count
                    db_conn.commit()
            if ret.category in [u'饮料甜点',u'面包蛋糕']:
                item = ElemeCategory(**{'restaurant_id': ret.restaurant_id, 'name':ret.name, 'category':u'下午茶'}) 
                count += 1
                db_conn.merge(item)
                if count % 5000 == 0:
                    print '%s commit ' % count
                    db_conn.commit()
            if ret.restaurant_id in bods:
                item = ElemeCategory(**{'restaurant_id': ret.restaurant_id, 'name':ret.name, 'category':u'品牌餐厅'}) 
                count += 1
                db_conn.merge(item)
                if count % 5000 == 0:
                    print '%s commit ' % count
                    db_conn.commit()
        except Exception,e:
            print line, e
            db_conn.rollback() 
    db_conn.commit()

if __name__ == '__main__':
    #basic_categorize()
    #save2mysql()
    #get_cutomer_dict()
    #get_terms_of_category()
    #feature_count()
    #handle_unknown_category()
    #add_category()
    for ret in db_conn.query(ElemeCategory).all():
        print "%s\t%s" % (ret.restaurant_id, ret.category.encode('utf-8'))
