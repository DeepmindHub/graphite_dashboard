import pandas as pd
import mysql.connector as sqlcon
from pandas.io import sql
# import statsd
# from statsd import StatsdClient
from elasticsearch import Elasticsearch
import sys
import time
import json
import datetime as dt
import numpy as np
sys.path.append('/home/ubuntu/yadu/config')
import dbConfig_elk as dbConfig
import dbConfig_locationdump as dbConfig2


def main():
    cnx = sqlcon.connect(user=dbConfig.USER, password=dbConfig.PWD,
                         host=dbConfig.HOST, database=dbConfig.DATABASE)
    cnx.start_transaction(isolation_level='READ COMMITTED')
    cnx2 = sqlcon.connect(user=dbConfig2.USER, password=dbConfig2.PWD,
                          host=dbConfig2.HOST, database=dbConfig2.DATABASE)
    cnx2.start_transaction(isolation_level='READ COMMITTED')

    es = Elasticsearch(['http://ec2-107-23-40-43.compute-1.amazonaws.com:9200/'])
    # last_id = 2073557
    last_time = dt.datetime.now() - dt.timedelta(minutes=5)
    while(1):
        order_data = getData(last_time, cnx)
        
        print 'Current time:', dt.datetime.now()
        print 'Dataframe shape:', order_data.shape
        if len(order_data):
            # last_id = order_data.iloc[:, 0].max()
            last_time = order_data.last_updated.max()
            riders = order_data.rider_id.unique().astype(str)
            location_data = getLocation(cnx2, riders)
            location_data.columns = ['ld_rider_id', 'ld_update_timestamp',
                                     'ld_latitude', 'ld_longitude']
            order_data = order_data.merge(
                location_data, how='left', left_on='rider_id', right_on='ld_rider_id')
        print 'Last update time:', str(last_time)
        print order_data.shape
        for ind in order_data.index:
            row = order_data.loc[ind]
            order_id = row.order_id
            doc_type = str(row.order_time.date())
            ind = row.isnull()
            row[ind] = None
            row = row.to_dict()  # json.dumps()
            print "sending "+str(ind)
            es.index(index="test-index_v2", doc_type="dashboard", id=order_id, body=row)
        time.sleep(60)


def getData(last_time, cnx):
    query = '''select now() current_time,
                      o.id order_id,
                      o.locality order_locality,
                      o.status order_status,
                      o.payment_mode order_payment_mode,
                      o.amount order_amount,
                      o.locality order_locality,
                      o.order_time order_time,
                      o.allot_time allot_time,
                      o.accept_time accept_time,
                      o.pickup_time pickup_time,
                      o.delivered_time delivered_time,
                      o.rider_id rider_id,
                      o.seller_id seller_id,
                      o.issue order_issue,
                      o.cancel_reason cancel_reason,
                      o.source order_source,
                      o.accepted_flag accpeted_flag,
                      o.delivered_flag delivered_flag,
                      o.pickup_flag pickup_flag,
                      o.COID COID,
                      o.scheduled_time scheduled_time,
                      o.last_updated last_updated,
                      o.cluster_id cluster_id,
                      c.cluster_name cluster_name,
                      ss.status seller_status,
                      ss.chain_id chain_id,
                      ss.category seller_category,
                      ss.city_id seller_city_id,
                      ss.outlet_name outlet_name,
                      ss.outlet_status outlet_status,
                      sr.allotted_phone rider_allotted_phone,
                      sr.status rider_status,
                      sr.rider_id rider_profile_id,
                      sr.rider_status rider_state,
                      rp.first_name rider_firstname,
                      rp.last_name rider_lastname,
                      ov.pickup_lat rider_pickup_lat,
                      ov.pickup_long rider_pickup_long,
                      ov.delivered_lat rider_delivered_lat,
                      ov.delivered_lng rider_delivered_lng,
                      ov.arrival_lat rider_arrival_lat,
                      ov.arrival_lng rider_arrival_lng,
                      ov.arrival_time rider_arrival_time
              from 

                      coreengine_order o 
                      left join coreengine_sfxseller ss 
                        on o.seller_id=ss.id 
                      left join coreengine_sfxrider sr 
                        on o.rider_id=sr.id 
                      left join coreengine_riderprofile rp 
                        on sr.rider_id=rp.id 
                      left join coreengine_cluster c 
                        on o.cluster_id=c.id 
                      left join coreengine_ordervariables ov 
                        on o.id=ov.order_id  
              where 
                      o.last_updated > "''' + str(last_time) + '''"
                      and c.cluster_name not like '%test%';'''
    # print query
    order_data = sql.read_sql(query, cnx)
    return order_data


def getLocation(cnx, riders):
    query = '''select ld.rider_id, ld.update_timestamp, ld.latitude, ld.longitude 
              from 
              coreengine_riderlocationdump ld,
              (
                select ld2.rider_id, max(ld2.update_timestamp) update_timestamp 
                from coreengine_riderlocationdump ld2 
                where ld2.rider_id in (''' + ','.join(riders) + ''') 
                group by 1 
                having update_timestamp>=now() - interval 15 minute
              ) t 
              where ld.rider_id=t.rider_id and 
              ld.update_timestamp=t.update_timestamp 
              group by 1,2;'''

    return sql.read_sql(query, cnx)

if __name__ == "__main__":
    main()
