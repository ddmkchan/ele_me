#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys
sys.path.append('/home/chenyanpeng/eleme_search')
from es_analyzer import *
from eleme_searcher import *
from building_searcher import levenshtein_sim
import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool
import json
import finalseg
import traceback
import time
import os
import redis
import csv

rc = redis.StrictRedis(host='localhost', port=6379, db=0)

def exeTime(func):
    def newFunc(*args, **args2):
        t0 = time.time()
        print "@%s, {%s} start" % (time.strftime("%X", time.localtime()), func.__name__)
        back = func(*args, **args2)
        print "@%s, {%s} end" % (time.strftime("%X", time.localtime()), func.__name__)
        print "@%.3fs taken for {%s}" % (time.time() - t0, func.__name__)
        return back
    return newFunc

to_number_dict = {
                "第一": '第1',
                "第二": '第2',
                "第三": '第3',
                "第四": '第4',
                "第五": '第5',
                "第六": '第6',
                "第七": '第7',
                "第八": '第8',
                "第九": '第9',
                "第十": '第10',
                "一号": "1号", 
                "二号": "2号", 
                "三号": "3号", 
                "四号": "4号", 
                "五号": "5号", 
                "六号": "6号", 
                "七号": "7号", 
                "八号": "8号", 
                "九号": "9号", 
                "十号": "10号", 
                "十一号": "11号", 
                "十二号": "12号", 
                "十三号": "13号", 
                "十四号": "14号", 
                "十五号": "15号", 
                "十六号": "16号", 
                "十七号": "17号", 
                "十八号": "18号", 
                "十九号": "19号", 
                "二十号": "20号", 
                }

def text_sim(s1, s2):
    return cosine_similarity(s1, s2) * 0.6 + levenshtein_sim(s1, s2) * 0.4


def _calculate2(_address):
    s1 = _address[0]
    s2 = _address[1]
    _sim = text_sim(s1, s2)
    return s1, _sim

def _calculate(address_list):
    _dict = {}
    for _address in address_list:
        s1 = _address[0]
        s2 = _address[1]
        _sim = text_sim(s1, s2)
        if _sim >= 0.6:
            _dict[s1] = _sim
    return _dict
    
_output = {}


def start_process():
    print (os.getpid(), ' Starting', multiprocessing.current_process().name)

@exeTime
def search_jinshisong():
    f = open('jinshisong_0511')
    for line in f.readlines()[:]:
        id, name = line.rstrip().split('\t')[1:]
        is_match = False
        rname = name
        road_name = u''
        segs = re.split(u'[\(|\)|（|）]+', name.decode('utf-8'))
        if len(segs) >= 2:
            rname = segs[0]
            road_name = segs[1]
        for i in search_poi(keyword=rname, length=10)['data']:
            sim = levenshtein_sim(rname, i['name'])
            _segs = re.split(u'[\(|\)|（|）]+', i['name'])
            _road_name = i['road_name']
            #print road_name, '----', _road_name
            if road_name and _road_name:
                road_name = re.sub(u'店','', road_name)
                _road_name = re.sub(u'店',u'', _road_name)
                if levenshtein_sim(road_name, _road_name) >= 0.8 and sim > 0.7:
                    is_match = True
                    print '-----', id, name, '--->', i['id'], i['name'], sim
            else:
                if sim >= 0.84:
                    is_match = True
                    print '#####', id, name, '--->', i['id'], i['name'], sim
        if is_match:
            print id, name
                


def cosine_similarity(s1, s2):
    if not isinstance(s1, unicode):
        s1 = s1.lower().decode('utf-8')
    if not isinstance(s2, unicode):
        s2 = s2.lower().decode('utf-8')
    s1 = re.sub(u'\(|\)|（|）| |\s+|\#', u'', s1)
    s2 = re.sub(u'\(|\)|（|）| |\s+|\#', u'', s2)
    t1 = [i for i in finalseg.cut(s1)]
    t2 = [i for i in finalseg.cut(s2)]
    #print ",".join(t1)
    #print ",".join(t2)
    terms = list(set([i for i in t1+t2]))
    v1 = [0] * len(terms)
    v2 = [0] * len(terms)
    for i in xrange(len(terms)):
        if terms[i] in s1:
            v1[i] += 1
        if terms[i] in s2:
            v2[i] += 1
    sum_xy = 0
    sum_x2 = 0
    sum_y2 = 0
    for j in xrange(len(terms)):
        sum_xy += v1[j] * v2[j]
        sum_x2 += v1[j] ** 2
        sum_y2 += v2[j] ** 2
    #return sum_xy / (math.sqrt(sum_x2) * math.sqrt(sum_y2))
    return sum_xy / (math.sqrt(sum_x2) * math.sqrt(sum_y2)) if sum_x2!=0 and sum_y2!=0 else 0


def search_line0():
    f = open('line0')
    for line in f.readlines()[:]:
        is_match = False
        id, name, address, city_name = line.rstrip().split('\t')
        rname = name
        road_name = u''
        segs = re.split(u'[\(|\)|（|）]+', name.decode('utf-8'))
        if len(segs) >= 2:
            rname = segs[0]
            road_name = segs[1]
        #print '****', rname, road_name
        for i in search_poi(keyword=rname, length=20, city_name=city_name)['data']:
            sim = levenshtein_sim(rname, i['name'])
            addr_sim = cosine_similarity(address, i['address'])
            _segs = re.split(u'[\(|\)|（|）]+', i['name'])
            _road_name = _segs[1] if len(_segs)>=2 else ''
            if road_name and _road_name:
                road_name = re.sub(u'店','', road_name)
                _road_name = re.sub(u'店',u'', _road_name)
                if levenshtein_sim(road_name, _road_name) >= 0.8 and sim > 0.7:
                    is_match = True
            else:
                if sim >= 0.84 and addr_sim > 0.44:
                    is_match = True
                    #print '#####',address, id, name, '--->', i['id'], i['name'], sim
                else:
                    _road1 = re.search(u'[\u4e00-\u9fa5]{2,3}路', address.decode('utf-8'))
                    _road2 = re.search(u'[\u4e00-\u9fa5]{2,3}路', i['address'])
                    if _road1 is not None and _road2 is not None:
                        #print _road1.group(), _road2.group()
                        if sim >= 0.6 and cosine_similarity(_road1.group(), _road2.group()) >= 0.8:
                            is_match = True
                            #print '****', id, name, address, '--->', i['id'], i['name'], sim
        if not is_match:
            print "%s\t%s\t%s\t%s" % (id, city_name, name, address)

def search_sherpas():
    f = open('sherpas')
    for line in f.readlines()[:]:
        id, name, address = line.rstrip().split('\t')
        is_match = False
        ename, rname = re.split(' / ', name)
        segs = re.split(u'[\(|\)|（|）]+', rname.decode('utf-8'))
        if len(segs) >= 2:
            rname = segs[0]
        for i in search_poi(keyword=rname, length=10)['data']:
            sim = levenshtein_sim(rname, i['name'])
            addr_sim = cosine_similarity(address, i['address'])
            #print address, i['address'], addr_sim
            #print rname, '----',i['id'], i['name']
            if sim >= 0.84:
            #if sim > 0.8 and addr_sim > 0.6:
                is_match = True
                #print '-----', id, name, '--->', i['id'], i['name'], sim
        if not is_match:
            print "%s\t%s\t%s" % (id, name, address)

def search_dianwoba():
    d = get_data_from_excel('dw.xlsx')
    for ret in d[:]:
        is_match = False
        id,name,address,supId,dianping_num,more_4_star_num,openTime,arrivaltime,avercost,city,status,url,star,lastModified = ret
        if not isinstance(address, unicode):
            address = unicode(address)
        if isinstance(name, unicode):
            for i in search_poi(keyword=name, length=10, city_name=city)['data']:
                try:
                    sim = levenshtein_sim(name, i['name']+i['road_name'])
                    addr_sim = cosine_similarity(address, i['address'])
                    if sim >= 0.8 and addr_sim >=0.4:
                        is_match = True
                        #print '-----', id, name, address, '--->', i['id'], i['name'], sim
                    if sim >= 0.7 and (name in i['name'] or i['name'] in name) and addr_sim>=0.5:
                        is_match = True
                        #print '-----', id, name, address, '--->', i['id'], i['name'], sim
                    else:
                        if sim >= 0.7 and addr_sim >= 0.5:
                            is_match = True
                            #print '*****', id, name, address, '--->', i['id'], i['name'], sim
                        else:
                            _road1 = re.search(u'[\u4e00-\u9fa5]{3}路', address)
                            _road2 = re.search(u'[\u4e00-\u9fa5]{3}路', i['address'])
                            if _road1 is not None and _road2 is not None:
                                if sim >= 0.7 and cosine_similarity(_road1.group(), _road2.group()) >= 0.8:
                                    is_match = True
                                    #print '######', id, name, address, '--->', i['id'], i['name'], sim

                    if not is_match:
                        sim = levenshtein_sim(name, i['name'])
                        if sim >= 0.8 and addr_sim >=0.4:
                            is_match = True
                            #print '2222-----', id, name, address, '--->', i['id'], i['name'], sim
                        if sim >= 0.7 and (name in i['name'] or i['name'] in name) and addr_sim>=0.5:
                            is_match = True
                            #print '-----', id, name, address, '--->', i['id'], i['name'], sim
                        else:
                            if sim >= 0.7 and addr_sim >= 0.5:
                                is_match = True
                                #print '*****', id, name, address, '--->', i['id'], i['name'], sim
                            else:
                                _road1 = re.search(u'[\u4e00-\u9fa5]{3}路', address)
                                _road2 = re.search(u'[\u4e00-\u9fa5]{3}路', i['address'])
                                if _road1 is not None and _road2 is not None:
                                    if sim >= 0.7 and cosine_similarity(_road1.group(), _road2.group()) >= 0.8:
                                        is_match = True
                                        #print '2222######', id, name, address, '--->', i['id'], i['name'], sim

                except Exception,e:
                    pass

        if not is_match:
            try:
                if not isinstance(name, unicode):
                    name = unicode(name)
                #print id, city,name, address, '*******', isinstance(name, unicode)
                out = []
                for i in ret[:-1]:
                    if isinstance(i, unicode):
                        out.append(i.encode('utf-8'))
                    elif isinstance(i, float):
                        out.append(str(int(i)))
                print "\t".join(out)
                #print "%s\t%s\t%s\t%s" % (int(id), city.encode('utf-8'), name.encode('utf-8'), address.encode('utf-8'))
            except Exception,e:
                print str(e)

def get_data_from_excel(file, _index=0):
    import xlrd
    data = xlrd.open_workbook(file)
    table = data.sheets()[_index]
    nrows = table.nrows #行数
    ncols = table.ncols #列数
    #for i in xrange(nrows):
    #    print table.row_values(i)
    return [table.row_values(i) for i in xrange(1, nrows)]

def search_daojia():

    d = get_data_from_excel('dw.xlsx', _index=1)
    for ret in d[:]:
        id,name,area,address,orderType,note,detai,url,limitMins,city,averCost,likeNum,lastModified = ret
        is_match = False
        rname = name
        segs = re.split(u'[\(|\)|（|）]+', name)
        if len(segs) >= 2:
            rname = segs[0]
        for i in search_poi(keyword=rname, length=10, city_name=city)['data']:
            sim = levenshtein_sim(rname, i['name'])
            addr_sim = cosine_similarity(address, i['address'])
            #print address, i['address'], addr_sim
            #print rname, '----',i['id'], i['name']
            if sim >= 0.85 and addr_sim >=0.5:
                is_match = True
                #print '---1111--', id, name, address, '--->', i['id'], i['name'], sim
            if sim>=0.7 and (rname in i['name'] or i['name'] in rname) and addr_sim>=0.5:
                is_match = True
                #print '--22233333---', id, name, address, '--->', i['id'], i['name'], sim
            else:
                if sim >= 0.7 and addr_sim >= 0.5:
                    is_match = True
                    #print '*****', id, name, address, '--->', i['id'], i['name'], sim
                else:
                    _road1 = re.search(u'[\u4e00-\u9fa5]{3}路', address)
                    _road2 = re.search(u'[\u4e00-\u9fa5]{3}路', i['address'])
                    if _road1 is not None and _road2 is not None:
                        if sim >= 0.7 and cosine_similarity(_road1.group(), _road2.group()) >= 0.8:
                            is_match = True
                            #print '######', id, name, address, '--->', i['id'], i['name'], sim
        if not is_match:
            #print "%s\t%s\t%s\t%s" % (int(id), name.encode('utf-8'), city.encode('utf-8'),address.encode('utf-8'))
            out = []
            for i in ret[:-1]:
                if isinstance(i, unicode):
                    out.append(i.encode('utf-8'))
                elif isinstance(i, float):
                    out.append(str(int(i)))
            print "\t".join(out)


def restaurant_match():
    with open('restaurant') as f:
        for line in f.readlines()[50539:50555]:
            try:
                city_name, id, name, address, lat, lon = line.rstrip().split('\t')
                is_match = False
                segs = re.split(u'[\(|\)|（|）|【|】]+', name.decode('utf-8'))
                rname = name.decode('utf-8')
                road_name = u''
                if len(segs) >= 2:
                    rname = segs[0]
                    road_name = segs[1]
                print rname, '-------', road_name, address
                out = []
                rs = search_poi(keyword=rname, city_name=city_name, length=10, lat=lat, lon=lon, radius=5000)['data']
                for ret in rs:
                    sim =  levenshtein_sim(rname, ret['name'])
                    addr_sim = cosine_similarity(address, ret['address'])
                    if len(road_name) >= 1 and len(ret['road_name']) >= 1:
                        road_name_sim = levenshtein_sim(road_name, ret['road_name'])
                        if sim >= 0.8 and road_name_sim >= 0.8:
                            is_match = True
                            #print "%s\t%s\t%s\t%s\t%s\t%s" % (id, name, address, ret['baidu_id'], ret['name'].encode('utf-8'), ret['address'].encode('utf-8'))
                    if not is_match:
                        if sim >= 0.8 and addr_sim >= 0.6:
                            is_match = True
                            #print id, name, address,"*******---->", ret['id'], ret['name'], ret['address']
                            #print "%s\t%s\t%s\t%s\t%s\t%s" % (id, name, address, ret['baidu_id'], ret['name'].encode('utf-8'), ret['address'].encode('utf-8'))
                if not is_match:
                    rs = search_poi(keyword=rname, city_name=city_name, length=50)['data']
                    for ret in rs:
                        sim =  levenshtein_sim(rname, ret['name'])
                        addr_sim = cosine_similarity(address, ret['address'])
                        if len(road_name) >= 1 and len(ret['road_name']) >= 1:
                            road_name_sim = levenshtein_sim(road_name, ret['road_name'])
                            if sim >= 0.8 and road_name_sim >= 0.8:
                                is_match = True
                                #print id, name, address, "---->", ret['id'], ret['name'], ret['address']
                                #print "%s\t%s\t%s\t%s\t%s\t%s" % (id, name, address, ret['baidu_id'], ret['name'], ret['address'])
                                #print "%s\t%s\t%s\t%s\t%s\t%s" % (id, name, address, ret['baidu_id'], ret['name'].encode('utf-8'), ret['address'].encode('utf-8'))
                        if not is_match:
                            if sim >= 0.8 and addr_sim >= 0.7:
                                is_match = True
                                #print id, name, address,"*******---->", ret['id'], ret['name'], ret['address']
                                #print "%s\t%s\t%s\t%s\t%s\t%s" % (id, name, address, ret['baidu_id'], ret['name'], ret['address'])
                                #print "%s\t%s\t%s\t%s\t%s\t%s" % (id, name, address, ret['baidu_id'], ret['name'].encode('utf-8'), ret['address'].encode('utf-8'))
                if not is_match:
                    print id, name, address,"*******----"
            except Exception,e:
                #print traceback.format_exc()
                pass

def func():
    mydict = {}
    with open('mm') as f:
    #with open('match_baidu_restaurant') as f:
        for line in f.readlines()[:]:
            segs = line.rstrip().split('\t')
            baidu_address = ' '
            if len(segs) == 6:
                eleme_id, eleme_name, ele_address, baidu_id, baidu_name, baidu_address = line.rstrip().split('\t')
            elif len(segs) == 5:
                eleme_id, eleme_name, ele_address, baidu_id, baidu_name = line.rstrip().split('\t')
            c_score = levenshtein_sim(eleme_name, baidu_name) * 0.5 + cosine_similarity(ele_address, baidu_address) * 0.5
            if baidu_id in mydict:
                _score, _ = mydict[baidu_id]
                if c_score > _score:
                    mydict[baidu_id] = (c_score, line.rstrip())
            else:
                mydict[baidu_id] = (c_score, line.rstrip())
            #if eleme_id in mydict:
            #    _score, _ = mydict[eleme_id]
            #    if c_score > _score:
            #        mydict[eleme_id] = (c_score, line.rstrip())
            #else:
            #    mydict[eleme_id] = (c_score, line.rstrip())
    for _, v in mydict.iteritems():
        print v[1]
            

if __name__ == '__main__':
    #search_sherpas()
    #search_line0()
    #print levenshtein_sim('古宜粥店', '古宜粥')
    #print cosine_similarity('', '古宜粥房')
    #search_jinshisong()
    search_daojia()
    #search_dianwoba()
    #restaurant_match()
    #func()
