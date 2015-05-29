#!/usr/bin/env python
#-*- coding:utf-8 -*-

import re
import sys
sys.path.append('/home/chenyanpeng/common')
from get_logger import *
from define import *
from model import NewAddLatLng
import xlrd
import json
import traceback
import requests
requests_session = requests.session()

db_conn = new_session()

sr_log = get_logger('baidu.log', logging.INFO, logging.getLogger('baidu.log'))

def args_logger(fn):
    @wraps(fn)
    def _(self, *args, **kwargs):
        sr_log.info(fn.__name__ + ' is called\n' + trans_args_to_str(args, kwargs))
        return fn(self, *args, **kwargs)
    return _

def trans_args_to_str(*argv, **kwargs):
    try:
        return '>>>args:' + str(argv)
    except Exception, e:
        print e
        return 'trans args to string failed.' + str(e)

def baidu_geocode(address='', city=''):
    try:
        url = "http://api.map.baidu.com/geocoder/v2/"
        payload = {
                "ak" : "n8rpKiKBWQ5G2M9TuNMTjz4l",
                "output": "json",
                "address" : address,
                "city" : city}
        return requests_session.get(url, params=payload).json()
    except Exception,e:
        mylogger.error("%s\t%s\t%s" % (address, city,traceback.format_exc()))
    return {'status': 1, 'msg': u"except" }


def convert_coords_fifth(lng, lat, cur_time=1, retry_time=5):
    while cur_time <= retry_time:
        coords = convert_to_gaode_coords(str(lng), str(lat))
        if coords is not None:
            return coords
        else:
            convert_coords_fifth(lng, lat, cur_time=cur_time+1)
        break
    else:
        sr_log.error("%s\t%s\t retry 5 times but fail" % (lng, lat))
        return None


def main():
    data = xlrd.open_workbook('community.xlsx')
    tables = data.sheets()
    for t in tables:
        if t.name not in [u'Sheet1', u'Sheet2', 'Sheet3']:
            #bj = tables[0]
            bj = t
            print t.name, '-----'
            count = 0
            for i in xrange(3, bj.nrows-1):
                _row = bj.row_values(i)
                ret = []
                for i in _row:
                    if i != u'':
                        if isinstance(i, unicode):
                            ret.append(i)
                            #ret.append(i.encode('utf-8'))
                        if isinstance(i, float):
                            ret.append(str(i))
                city_name, project, area, address,a, b, c, d, e, f, g, h, i = ret
                ins = db_conn.query(NewAddLatLng).filter(NewAddLatLng.address==address).filter(NewAddLatLng.building_name==project).first()
                if not ins:
                    data1 = baidu_geocode(address, city_name)
                    sr_log.info("*****input: %s,%s\trs: %s\t msg: %s" % (city_name.encode('utf-8'), address.encode('utf-8'), json.dumps(data1), data1.get('msg', u'').encode('utf-8')))
                    hit = False
                    if data1['status'] == 0:
                        rs = data1['result']
                        precise = rs.get('precise', 0)
                        confidence = rs.get('confidence',0)
                        if precise == 1 and confidence >= 70:
                            hit = True
                            lng = rs['location']['lng']
                            lat = rs['location']['lat']
                            coords = convert_coords_fifth(lng, lat)
                            if coords is not None:
                                count += 1
                                try:
                                    item = NewAddLatLng(**{
                                    'address' : address,
                                    'building_name' : project,
                                    'b_lng' : lng,
                                    'b_lat' : lat,
                                    'longitude' : coords.get('lng'),
                                    'latitude' : coords.get('lat'),
                                    })
                                    db_conn.merge(item)
                                    if count % 1000 == 0:
                                        print "%s commit...."  % count
                                        db_conn.commit()
                                except Exception,e:
                                    print traceback.format_exc()
                                    db_conn.rollback()
                    if not hit:
                        data1 = baidu_geocode(address+project, city_name)
                        sr_log.info("#########input: %s,%s\trs: %s\t msg: %s" % (city_name.encode('utf-8'), (address+project).encode('utf-8'), json.dumps(data1), data1.get('msg', u'').encode('utf-8')))
                        hit = False
                        if data1['status'] == 0:
                            rs = data1['result']
                            precise = rs.get('precise', 0)
                            confidence = rs.get('confidence',0)
                            if precise == 1 and confidence >= 70:
                                hit = True
                                lng = rs['location']['lng']
                                lat = rs['location']['lat']
                                coords = convert_coords_fifth(lng, lat)
                                if coords is not None:
                                    count += 1
                                    try:
                                        item = NewAddLatLng(**{
                                        'address' : address,
                                        'building_name' : project,
                                        'b_lng' : lng,
                                        'b_lat' : lat,
                                        'longitude' : coords.get('lng'),
                                        'latitude' : coords.get('lat'),
                                        })
                                        db_conn.merge(item)
                                        if count % 1000 == 0:
                                            print "%s commit...."  % count
                                            db_conn.commit()
                                    except Exception,e:
                                        print traceback.format_exc()
                                        db_conn.rollback()
                #else:
                #    print ins.address, 'hits'
            db_conn.commit()


def func():
    data = xlrd.open_workbook('community.xlsx')
    tables = data.sheets()
    for t in tables:
        if t.name not in [u'Sheet1', u'Sheet2', 'Sheet3']:
            #bj = tables[0]
            bj = t
            count = 0
            #for i in xrange(3, 20):
            for i in xrange(3, bj.nrows-1):
                _row = bj.row_values(i)
                ret = []
                for i in _row:
                    if i != u'':
                        if isinstance(i, unicode):
                            #ret.append(i)
                            i = re.sub(u'\t', u'', i)
                            ret.append(i.encode('utf-8'))
                        if isinstance(i, float):
                            ret.append(str(int(i)))
                city_name, project, area, address,a, b, c, d, e, f, g, h, i = ret
                lat = '0'
                lng = '0'
                ins = db_conn.query(NewAddLatLng).filter(NewAddLatLng.address==address).filter(NewAddLatLng.building_name==project).first()
                if ins:
                    lat = ins.latitude.encode('utf-8')
                    lng = ins.longitude.encode('utf-8')
                ret.append(lat)
                ret.append(lng)
                out = "\t".join(ret)
                #if len(out.split('\t')) != 15:
                print out
                 

if __name__ == '__main__':
    func()
    #main()
