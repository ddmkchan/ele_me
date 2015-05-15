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
    rid2name = json.loads(rc.get('rid2name'))
    category2resturant = json.loads(rc.get('category2resturant'))
    _dict = {}
    for k, v in category2resturant.iteritems():
        #print k, ",".join([rid2name.get(str(i)) for i in v[:10]])
        for id in v:
            if id in _dict:
                _dict[id].append(k)
            else:
                _dict[id] = [k]
    for id, categories in _dict.iteritems():
        try:
            for c in categories:
                item = ElemeCategory(**{'restaurant_id': id, 'name': rid2name.get(str(id)), 'category': c })
                count += 1
                db_conn.merge(item)
                if count % 5000 == 0:
                    print '%s commit ' % count
                    db_conn.commit()
        except Exception,e:
            print e
            db_conn.rollback() 

    #f1 = get_data_from_files('match_rs_0421.txt')
    #for line in f1:
    #    try:
    #        segs = line.rstrip().split('\t')
    #        if len(segs) == 3:
    #            id = int(segs[0])
    #            item = ElemeCategory(**{'id': id, 'name':segs[1], 'category':segs[2]}) 
    #            count += 1
    #            db_conn.merge(item)
    #            if count % 5000 == 0:
    #                print '%s commit ' % count
    #                db_conn.commit()
    #    except Exception,e:
    #        print line, e
    #        db_conn.rollback() 
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

def get_noise_terms():
    _s = set([])
    with open('frequency_term.txt') as f:
        for line in f.readlines():
            _s.add(line.rstrip().decode('utf-8'))
    return u"|".join(list(_s))

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
                is_match = True
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
    rid2name = json.loads(rc.get('rid2name'))
    category2resturant = json.loads(rc.get('category2resturant'))
    for k, v in category2resturant.iteritems():
        texts = []
        ids = ",".join([str(i) for i in v])
        sql = "select * from ele_food_segments_2 where id in (%s)" % ids
        for ret in db_conn.execute(sql):
            #print "%s\t%s\t%s" % (ret[0], rid2name.get(str(ret[0])).encode('utf-8'),  ",".join([w.encode('utf-8') for w in json.loads(ret[1]) if len(w) <= 5 and re.search(get_noise_terms(), w) is None]))
            #print ret[0], rid2name.get(str(ret[0])), '------------->\r\n', ",".join([w for w in json.loads(ret[1]) if len(w) <= 5 and re.search(get_noise_terms(), w) is None])
            terms = [w for w in json.loads(ret[1]) if len(w) <= 5]
            texts.append(terms)
        dictionary = corpora.Dictionary(texts)
        corpus = [dictionary.doc2bow(text) for text in texts]
        _s = sorted(dictionary.dfs.iteritems(), key=lambda d:d[1], reverse=True)
        #print k, len(_s)
        _dict = {}
        for token, id in dictionary.token2id.iteritems():
            _dict[id] = token
        #if k  in [u'江浙菜',u'清真/新疆菜']:
        #    print "%s\t%s" % (k.encode('utf-8'), ",".join([_dict.get(i[0]).encode('utf-8') for i in _s[:50]]))
        #elif k in [u'东北菜', u'东南亚菜',u'湘菜',u'韩国料理']:
        #    print "%s\t%s" % (k.encode('utf-8'), ",".join([_dict.get(i[0]).encode('utf-8') for i in _s[:80]]))
        #else:
        #    print "%s\t%s" % (k.encode('utf-8'), ",".join([_dict.get(i[0]).encode('utf-8') for i in _s[:200]]))
        print "%s\t%s" % (k.encode('utf-8'), ",".join([_dict.get(i[0]).encode('utf-8') for i in _s[:250]]))

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


if __name__ == '__main__':
    #basic_categorize()
    #save2mysql()
    get_terms_of_category()
    #clean()
