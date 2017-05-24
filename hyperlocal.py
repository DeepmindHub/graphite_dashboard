import pandas as pd
import mysql.connector as sqlcon
from pandas.io import sql
import statsd
# from statsd import StatsdClient
import sys
import time
import datetime as dt
sys.path.append('/home/ubuntu/yadu/config')
import dbConfig_pr as dbConfig


def main():
    cnx = sqlcon.connect(
        user=dbConfig.USER, password=dbConfig.PWD, host=dbConfig.HOST, database=dbConfig.DATABASE)
    cnx.start_transaction(isolation_level='READ COMMITTED')
    # client = StatsdClient(host='52.90.45.207')
    statsd.init_statsd({'STATSD_HOST': '52.90.45.207'})
    last_id = 1915720
    while(1):
        df = getData(last_id, cnx)
        print 'Current time:', dt.datetime.now()
        print 'Dataframe shape:', df.shape
        if len(df):
            last_id = df.order_id.max()
        print 'Last ID:', last_id
        for ind in df.index:
            row = df.loc[ind]
            string = 'hyperlocal.'+str(row.cluster_id)+'.'+row.outlet_name+'.'+str(row.rider_id)
            print string
            statsd.increment(string)
        # df = df[50:]
        # print df.shape
        time.sleep(60)
        # exit(-1)


def getData(last_id, cnx):
    query = "select o.id order_id, o.cluster_id, ss.outlet_name, o.rider_id from coreengine_order o, coreengine_sfxseller ss where o.seller_id=ss.id and o.id>" + \
        str(last_id)+" order by o.id;"
    # print query
    df = sql.read_sql(query, cnx)
    # print df.shape
    return df

if __name__ == "__main__":
    main()
