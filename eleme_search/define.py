#!/usr/bin/python
#-*- coding:utf-8 -*-
import MySQLdb

conn = MySQLdb.connect(host="10.1.2.211", port=3333, user="root", passwd="123456", db="jinfuzi_erp", charset="utf8", use_unicode=True)

cursor = conn.cursor()  

#cursor.execute("select prd_full_name,  prd_issue_company_name from crm_product_info limit 20")
#
#for ret in cursor.fetchall():
#	print ret

