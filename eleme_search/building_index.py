#!/usr/bin/env python
#-*- coding:utf-8-*-

import os
import re
import sys
import traceback
from pyes import *
from decimal import Decimal
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


class building_seatch(object):

    def __init__(self):
        self._index = "building_index"
        self._type = "building_type"
        self.conn = ES('127.0.0.1:9200', timeout=3.5)#连接ES
        #获取两个切换index名
        self._alias = ['building_index_1', 'building_index_2']
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
        mapping = {u'building_id': {'store': 'yes',
                          'type': u'integer'
                            },
                  u'city_name': {'index': 'not_analyzed',
                             'store': 'yes',
                             'type': u'string'},
                  u'city_id': {'store' : 'yes',
                                    'type':u'integer'
                                    },
                  u'building_name': {'index': 'analyzed',
                             'store': 'yes',
                             'type': u'string',
                             "indexAnalyzer":"ik_smart",
                             "searchAnalyzer":"ik_smart",
                             "term_vector" : "with_positions_offsets"},
                  u'alias': {'index': 'analyzed',
                             'store': 'yes',
                             'type': u'string',
                             "indexAnalyzer":"ik_smart",
                             "searchAnalyzer":"ik_smart",
                             "term_vector" : "with_positions_offsets"},
                  u'school_address': {'index': 'analyzed',
                             'store': 'yes',
                             'type': u'string',
                             "indexAnalyzer":"ik_smart",
                             "searchAnalyzer":"ik_smart",
                             "term_vector" : "with_positions_offsets"},
                  u'school_name': {'index': 'not_analyzed',
                             'store': 'yes',
                             'type': u'string'},
                  u"location": {"type": "geo_point",
                                "store": "yes",
                                "normalize": "yes",
                                "lat_lon": "yes"
                                },
                }
        self.conn.indices.delete_index_if_exists(self.tmp_index)
        self.conn.indices.create_index(self.tmp_index)#新建一个索引

        self.conn.indices.put_mapping(self._type, {'properties':mapping}, [self.tmp_index])

    def add_doc(self):
        rs = get_data()
        for ret in rs:
            doc = {}
            doc["building_id"] = ret['building_id']
            doc["building_name"] = ret['building_name']
            doc["city_id"] = ret['city_id']
            doc["city_name"] = ret['city_name']
            doc["school_name"] = ret['school_name']
            doc["school_address"] = ret['school_address']
            doc["location"] = {"lat":ret['lat'],"lon":ret['lng']}
            doc["alias"] = ret['alias']
            self.conn.index(doc, self.tmp_index, self._type)

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

def get_data_v1():
    rs = []
    f = open('data/input_building')
    mydict = {}
    for i in f.readlines():
        segs = i.rstrip().split()
        try:
            if len(segs) == 8:
                mydict[segs[1]] = segs[7]
            _city_name = segs[0]
            _id = 0
            _name = segs[5]
            school = segs[1]
            origin_addr = segs[3]
            _addr = " ".join([segs[3], segs[1], segs[5]])
            alias = re.sub(",|，", " ", segs[6]).split()
            alias.append(_name)
            _region_id = 0
            lat = 0
            lng = 0
            if segs[1] in mydict:
                latlng = mydict.get(segs[1], '').split(',')
                if len(latlng) == 2:
                    lng = latlng[0]
                    lat = latlng[1]
            #if _name == '15栋':
            #    print segs[5], lat, lng
            #for k,v in mydict.iteritems():
            #    print k,v, ('', k)
            rs.append({"id": int(_id),
                        "name": _name,
                        "origin_addr": origin_addr,
                        "school": school,
                        "alias": alias,
                        "address": _addr,
                        "region_id": int(_region_id),
                        "city_name": _city_name,
                        "lat": float(lat),
                        "lng": float(lng),
                        })
        except Exception,e:
            print i.rstrip()
            print traceback.format_exc()
        #road_names = re.findall(u"(\\(.+\\)|（.+）)+", mt_name)
    f.close()
    return rs



def get_data():
    rs = []
    number2word = get_number_2_word()
    f = open('data/building_info')
    for i in f.readlines()[:]:
        segs = i.rstrip().split('\t')
        try:
            city_id = segs[0]
            city_name = segs[1]
            school_name = segs[2]
            school_address = segs[3]
            building_id = segs[4]
            building_name = segs[5]
            alias = re.sub(",|，|、", " ", segs[6]).split() if segs[6] != '无' else []
            alias.append(building_name)
            combined_name = ",".join(alias)
            for k,v in number2word.iteritems():
                if k in combined_name:
                    combined_name = re.sub(k, v, combined_name)
            lat = segs[8]
            lng = segs[7]
            rs.append({"building_id": int(building_id),
                        "building_name": combined_name,
                        "city_id": int(city_id),
                        "city_name": city_name,
                        "school_name": school_name,
                        "school_address": school_address,
                        "alias": alias,
                        "lat": float(lat),
                        "lng": float(lng),
                        })
        except Exception,e:
            print len(segs)
            print i.rstrip()
            print traceback.format_exc()
        #road_names = re.findall(u"(\\(.+\\)|（.+）)+", mt_name)
    f.close()
    return rs

def get_number_2_word():
    _map = {}
    with open('number2word.dict') as f:
        for line in f.readlines():
            segs = line.rstrip().split(':')
            _map[segs[0]] = segs[1]
    return _map

if __name__ == '__main__':
    poi = building_seatch()
    poi.init_index()
    poi.rebuild_all()
    poi.switch_alias()
    #get_data()
