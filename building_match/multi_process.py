#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys
sys.path.append('/home/chenyanpeng/eleme_search')
from es_analyzer import *
from building_searcher import *
import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool
import json
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
def search_building():
    rs = []
    f = open('eleme_order/order_address_dd')
    lines = f.readlines()[:]
    print len(lines), "cur pid", os.getpid()
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count(), initializer=start_process)
    c1 = 0
    c2 = 0
    for i in lines:
        c2+=1
        line = i.rstrip()
        segs = line.split('\t')
        lat = float(segs[3])
        lon = float(segs[4])
        addr = segs[2]
        for k,v in to_number_dict.iteritems():
            if k in addr:
                addr = re.sub(k, v, addr)
        #addr = addr.decode('utf-8')
        is_match = False
        rs = search_poi(length=30, lat=lat, lon=lon, radius=1000)['data']
        if rs:
            address_list = []
            for t in rs:
                for j in t['alias']:
                    address_list.append((j, addr))
                    address_list.append((t['school']+j, addr))
                    address_list.append((t['origin_addr']+j, addr))
                    address_list.append((t['origin_addr']+t['school']+j, addr))
            #match_rs = _calculate(address_list)
            #if len(match_rs) >= 1:
            #    c1+=1
            #    is_match = True
            #    sort_list = sorted(match_rs.iteritems(), key=lambda d:d[1], reverse=True)
            #    print c2, "highest \thits", addr, "------>", sort_list[0][0].encode('utf-8'), sort_list[0][1]

            #print len(address_list)
            match_rs = pool.map_async(_calculate2, address_list).get()
            if len(match_rs) >= 1:
                match_rs.sort(key=lambda d:d[1], reverse=True)
                _hits = match_rs[0]
                if _hits[1] >= 0.6:
                    c1+=1
                    is_match = True
                    print c2, "highest \thits", addr, "------>", _hits[0].encode('utf-8'), _hits[1]
        if not is_match:
            #pass
            print c2, addr, "not hits!!!"
    print c1, c2
    pool.close()
    pool.join()

@exeTime
def get_data():
    rs = []
    f = open('eleme_order/order_address_145')
    lines = f.readlines()[:]
    for i in lines:
        line = i.rstrip()
        segs = line.split('\t')
        lat = float(segs[3])
        lon = float(segs[4])
        addr = segs[2]
        for k,v in to_number_dict.iteritems():
            if k in addr:
                addr = re.sub(k, v, addr)
        addr = addr.decode('utf-8')
        rs.append((addr, lat, lon))
    return rs

def get_m_search_rs(addr, lat, lon):
    address_list = []
    rs = search_poi(length=30, lat=lat, lon=lon, radius=1000)['data']
    if rs:
        for t in rs:
            for j in t['alias']:
                address_list.append((j, addr))
                address_list.append((t['school']+j, addr))
                address_list.append((t['origin_addr']+j, addr))
                address_list.append((t['origin_addr']+t['school']+j, addr))
    return address_list

@exeTime
def paralle_building():
    print len(lines), "cur pid", os.getpid()
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count(), initializer=start_process)
    match_rs = pool.map_async(_calculate2, address_list).get()
    if len(match_rs) >= 1:
        match_rs.sort(key=lambda d:d[1], reverse=True)
        _hits = match_rs[0]
        if _hits[1] >= 0.6:
            c1+=1
            is_match = True
            #print c2, "highest \thits", addr, "------>", _hits[0].encode('utf-8'), _hits[1]
    pool.close()
    pool.join()

@exeTime
def multi_s():
    print "cur pid", os.getpid()
    _in =  get_data()
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    address_list = [pool.apply_async(get_m_search_rs, (i[0], i[1], i[2])).get() for i in _in]
    pool.close()
    pool.join()
    return address_list

@exeTime
def get_search_rs():
    l = []
    for i in get_data():
        address_list = []
        rs = search_poi(length=30, lat=i[1], lon=i[2], radius=1000)['data']
        addr = i[0]
        if rs:
            for t in rs:
                for j in t['alias']:
                    address_list.append((j, addr))
                    address_list.append((t['school']+j, addr))
                    address_list.append((t['origin_addr']+j, addr))
                    address_list.append((t['origin_addr']+t['school']+j, addr))
        l.append(rs)
    print len(l)
    return l

def main():
    c1 = 0
    address_list = multi_s()
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count(), initializer=start_process)
    #for j in address_list:
        
    match_rs = pool.apply_async(_calculate2, address_list)
    pool.close()
    pool.join()
    print c1, len(_in)

def building_match():
    mydict = {}
    reader = csv.reader(open('baidu.csv'))
    reader.next()
    for segs in reader:
        building_name = segs[0]
        area = segs[1]
        address = segs[4]
        ele_address = segs[7]
        ele_name = segs[10]
    #    print building_name, area, address
        name_sims = levenshtein_sim(building_name, ele_name)
        address_sims = cosine_similarity(area+address, ele_address)
        if address_sims > 0.3 and name_sims > 0.4:
            matching_rate = name_sims *0.4 + address_sims * 0.5
            #print building_name, area, address, ele_name, ele_address, matching_rate
            if ele_name in mydict:
                if mydict.get(ele_name)[0] < matching_rate:
                    mydict[ele_name] = (matching_rate, ele_address, "\t".join(segs))
                    #mydict[ele_name] = (matching_rate, ele_address, ",".join([building_name, area, address]))
            else:
                mydict[ele_name] = (matching_rate, ele_address, "\t".join(segs))
                #mydict[ele_name] = (matching_rate, ele_address, ",".join([building_name, area, address]))
    for k, v in mydict.iteritems():
        #print k, v[1] , v[2]
        print v[2]
 

def building_match_v2():
    mydict = {}
    reader = csv.reader(open('meituan.csv'))
    reader.next()
    for segs in reader:
    #with open('meituan.csv') as f:
    #    for line in f.readlines()[1:400]:
    #        segs = line.rstrip().split(',')
        building_name = segs[0]
        area = segs[3]
        address = segs[1]
        address = re.sub(u'\.', '', address)
        ele_address = segs[8]
        ele_name = segs[11]
        #print building_name, area, address, ele_address, ele_name
        name_sims = levenshtein_sim(building_name, ele_name)
        address_sims = cosine_similarity(area+address, ele_address)
        if address_sims > 0.3 and name_sims > 0.4:
            matching_rate = name_sims *0.4 + address_sims * 0.5
            #print building_name, area, address, ele_name, ele_address, matching_rate
            if ele_name in mydict:
                if mydict.get(ele_name)[0] < matching_rate:
                    mydict[ele_name] = (matching_rate, ele_address, "\t".join(segs))
                    #mydict[ele_name] = (matching_rate, ele_address, ",".join([building_name, area, address]))
            else:
                mydict[ele_name] = (matching_rate, ele_address, "\t".join(segs))
                #mydict[ele_name] = (matching_rate, ele_address, ",".join([building_name, area, address]))
    for k, v in mydict.iteritems():
        #print k, v[1] , '------>', v[2]
        print v[2]
               


if __name__ == '__main__':
    #search_building()
    #get_data()
    #get_search_rs()
    #print len(multi_s())
    #main()
    building_match_v2()
