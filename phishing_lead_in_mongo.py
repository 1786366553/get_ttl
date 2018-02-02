#-*- coding: UTF-8 -*-
import pymongo
import MySQLdb
import tldextract
import time
from datetime import datetime
from datetime import timedelta
import schedule


def lead_in_mongo():
    print 111111
    time_limit_left = str(datetime.now() - timedelta(days=7))[0:19]
    time_limit_right = str(datetime.now())[0:19]
    timeArray_limit_left = time.strptime(time_limit_left, "%Y-%m-%d %H:%M:%S")
    timeStamp_limit_left = int(time.mktime(timeArray_limit_left))
    timeArray_limit_right = time.strptime(time_limit_right, "%Y-%m-%d %H:%M:%S")
    timeStamp_limit_right = int(time.mktime(timeArray_limit_right))
    db = MySQLdb.connect(
        "172.29.152.249 ", "root", "platform", "malicious_domain_collection")
    cursor = db.cursor()
    sql = "select domain,insert_time from malicious_domain_collection_complete where flag_judge is null"
    cursor.execute(sql)
    results = cursor.fetchall()
    connection = pymongo.MongoClient('172.29.152.152', 27017)
    db_mongo = connection.malicious_domain_get_system
    collection = db_mongo.malicious_domain_info
    count = 0
    for row in results:
        try:
            domain = row[0].strip('\n')
            time_insert = row[1].replace("/", "-")
            suffix = tldextract.extract(str(domain)).suffix
            timeArray = time.strptime(time_insert, "%Y-%m-%d %H:%M:%S")
            timeStamp = int(time.mktime(timeArray))
            if not suffix:
                continue
            elif timeStamp < timeStamp_limit_left or timeStamp > timeStamp_limit_right:
                continue
            elif domain is None:
                continue
            elif domain == "":
                continue
            else:
                dict = {"domain": domain, "insert_time":time_insert, "flag": 0}
                count = count + 1
                try:
                    collection.insert(dict)
                except:
                    continue
        except:
            continue
    db.close()
    print count


if __name__ == "__main__":
    schedule.every().tuesday.at("08:00").do(lead_in_mongo)
    while True:
        schedule.run_pending()
        time.sleep(1)
