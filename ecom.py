import pandas as pd
import mysql.connector as sqlcon
from pandas.io import sql
import statsd
# from statsd import StatsdClient
import sys
import time
import datetime as dt
sys.path.append('../config')
import dbConfig_ecom as dbConfig


def main():
    cnx = sqlcon.connect(
        user=dbConfig.USER, password=dbConfig.PWD, host=dbConfig.HOST, database=dbConfig.DATABASE)
    cnx.start_transaction(isolation_level='READ COMMITTED')
    # client = StatsdClient(host='52.90.45.207')
    statsd.init_statsd({'STATSD_HOST': '52.90.45.207'})
    last_id = 122000
    while(1):
        df = getData(last_id, cnx)
        print 'Current time:', dt.datetime.now()
        print 'Dataframe shape:', df.shape
        if len(df):
            last_id = df.request_id.max()
        print 'Last ID:', last_id
        # while (len(df)):
        #     df2 = df[:50]
        for ind in df.index:
            row = df.loc[ind]
            string = 'deploys.'+str(row.order_status)+'.'+row.client_name+'.'+str(row.dispatch_center_id)+'.'+str(row.hub_id)+'.'+str(row.rider_id) + \
                '.'+str(row.runsheet_id)+'.'+str(row.pincode)+'.' + \
                str(row.customer_contact)+'.'+str(row.payment_mode)+'.'+str(row.order_source)
            print string
            statsd.increment(string)
        # df = df[50:]
        # print df.shape
        time.sleep(60)
        # exit(-1)


def getData(last_id, cnx):
    query = "select dr.id request_id, dr.customer_contact, dr.product_value, dr.cod_amount, dr.payment_mode, dr.order_date, dr.scheduled_date, dr.order_status, dr.order_source, dr.attempt_number, dr.delivery_date, dr.last_updated, dr.cancelled_date, dr.lost_by, cl.name client_name, p.code pincode, dr.dispatch_center_id, dr.hub_id, dr.rider_id, dr.runsheet_id, r.name rider_name, r.contact_number rider_contact, h.name hub_name, c.name city from ecommerce_deliveryrequest dr, ecommerce_rider r, ecommerce_hub h, ecommerce_city c, ecommerce_client cl, ecommerce_pincode p where dr.rider_id=r.id and dr.hub_id=h.id and h.city_id=c.id and dr.client_id=cl.id and dr.customer_pincode_id=p.id and dr.id>" + \
        str(last_id)+" order by dr.id;"
    return sql.read_sql(query, cnx)

if __name__ == "__main__":
    main()
