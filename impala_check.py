#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from impala.dbapi import connect
from datetime import datetime, timedelta
from subprocess import Popen, PIPE

######################################

def kinit():
    kinit = Popen(['kinit', 'svc_zbxmon@DOMAIN.RU'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    pwd = open('/etc/zabbix/secret', 'r').read()
    kinit.stdin.write('%s\n' % pwd)
    kinit.wait()

#####################################

def dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
    ]

#####################################

def check_compact_raw(h='localhost',raw_name='',days_ago=2):

    d = datetime.today() - timedelta(days=int(days_ago))
    try:
        conn = connect(host=h, port=21050, auth_mechanism='GSSAPI', kerberos_service_name='impala', timeout=30)
        cursor = conn.cursor()
        cursor.execute('show partitions database.%s' % raw_name)

        for col in dictfetchall(cursor):
            if 'staging' in col['Location'] and int(col['year']) == d.year and int(col['month']) == d.month and int(col['day']) == d.day:
                return 0 #failure
        return 1 #success

    except: return 0 #failure

####################################

def check_maxday_raw(h='localhost',raw_name='',days_ago=1):

    d = datetime.today() - timedelta(days=int(days_ago))
    try:
       conn = connect(host=h, port=21050, auth_mechanism='GSSAPI', kerberos_service_name='impala', timeout=30)
       cursor = conn.cursor()
       cursor.execute('show partitions database.%s' % raw_name)
       data = dictfetchall(cursor)

       if 'day' in data[0].keys():
          for col in data:
              if int(col['year']) == d.year and int(col['month']) == d.month and int(col['day']) == d.day:
                 return 1 #success
          return 0 #failure
       else:
          cursor.execute('select max(day) from database.%s where year=%s and month=%s' % (raw_name,d.year,d.month))
          row = cursor.fetchone()
          if int(row[0]) == d.day: return 1 #success
          else: return 0 #failure

    except: return 0 #failure

###################################

def check_event_date_raw(h='localhost',raw_name='',hours_ago=1):

    d = datetime.today() - timedelta(hours=int(hours_ago))
    try:
       conn = connect(host=h, port=21050, auth_mechanism='GSSAPI', kerberos_service_name='impala', timeout=30)
       cursor = conn.cursor()
       cursor.execute('select date_part("hour",max(event_date)) from database.%s where year=%s and month=%s and day=%s' % (raw_name,d.year,d.month,d.day))
       row = cursor.fetchone()
       if int(row[0]) >= d.hour: return 1 #success
       else: return 0 #failure

    except: return 0 #failure

###################################

r = -1 #allways failure return code
if len(sys.argv) == 5 and sys.argv[2] == "compact" and "raw" in sys.argv[3]:
   kinit()
   r=check_compact_raw(sys.argv[1], sys.argv[3], sys.argv[4])
   print r
elif len(sys.argv) == 5 and sys.argv[2] == "maxday" and "raw" in sys.argv[3]:
   kinit()
   r=check_maxday_raw(sys.argv[1], sys.argv[3], sys.argv[4])
   print r
elif len(sys.argv) == 5 and sys.argv[2] == "event_date" and "raw" in sys.argv[3]:
   kinit()
   r=check_event_date_raw(sys.argv[1], sys.argv[3], sys.argv[4])
   print r
else: print r
