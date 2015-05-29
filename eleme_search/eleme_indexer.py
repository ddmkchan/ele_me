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
                          'type': u'integer'
                            },
                  u'name': {'index': 'analyzed',
                             'store': 'yes',
                             'type': u'string',
                             "indexAnalyzer":"ik_smart",
                             "searchAnalyzer":"ik_smart",
                             "term_vector" : "with_positions_offsets"},
                  u'baidu_id': {'index': 'not_analyzed',
                             'store': 'yes',
                             'type': u'string'},
                  u'city_name': {'index': 'not_analyzed',
                             'store': 'yes',
                             'type': u'string'},
                  u'road_name': {'index': 'not_analyzed',
                             'store': 'yes',
                             'type': u'string'},
                  u'address': {'index': 'analyzed',
                             'store': 'yes',
                             'type': u'string',
                             "indexAnalyzer":"ik_smart",
                             "searchAnalyzer":"ik_smart",
                             "term_vector" : "with_positions_offsets"},
                  #u'address1': {'index': 'analyzed',
                  #           'store': 'yes',
                  #           'type': u'string',
                  #           "indexAnalyzer":"whitespace",
                  #           "searchAnalyzer":"whitespace",
                  #           "term_vector" : "with_positions_offsets"},
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
        count = 0
        for ret in rs:
            doc = {}
            count += 1
            doc["id"] = ret['id']
            doc['baidu_id'] = ret['baidu_id']
            doc["name"] = ret['name']
            doc["city_name"] = ret['city_name']
            doc["address"] = ret['address']
            doc["location"] = {"lat":ret['lat'],"lon":ret['lng']}
            doc["road_name"] = ret['road_name']
            self.conn.index(doc, self.tmp_index, self._type)
            #if count % 5000 == 0:
            #    print '--------- %s commit' % count
            #    self.conn.indices.refresh()

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
    rs = []
    #f = open('data/baidu_restaurant')
    f = open('data/restaurant')
    count = 0
    for i in f.readlines():
        try:
            #i = re.sub('\t+','\t', i)
            city_name, id, name, address_text, latitude, longitude = i.rstrip().split('\t')
            #road_names = re.findall(u"(?<=[\\(|（])[\w\W\u4e00-\u9fa5]+(?=\\)|）)", name.decode('utf-8'))
            segs = re.split(u'[\(|\)|（|）|【|】]+', name.decode('utf-8'))
            rname = name.decode('utf-8')
            road_name = u''
            if len(segs) >= 2:
                rname = segs[0]
                road_name = segs[1]
            #print ",".join(road_names)
            count += 1
            rs.append({"id": id,
                        "name": rname,
                        "road_name": road_name,
                        "address": address_text,
                        "city_name": city_name,
                        "lat": float(latitude),
                        "lng": float(longitude),
                        "baidu_id": count
                        })
        except Exception,e:
            #print "*****",  i
            print str(e)
        #road_names = re.findall(u"(\\(.+\\)|（.+）)+", mt_name)
    f.close()
    return rs

if __name__ == '__main__':
    poi = poi_seatch()
    poi.init_index()
    poi.rebuild_all()
    poi.switch_alias()
    #print get_data()
