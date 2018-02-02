#-*- coding: UTF-8 -*-
import pymongo
import fetch_dns_ttl


def get_domain_info(domain):
    return fetch_dns_ttl.main(domain)


def list_create_a(info):
    list = []
    dict = {}
    dict['a'] = info[0]
    dict['a_ttl'] = info[1]
    dict['update_time'] = info[6]
    list.append(dict)
    return list


def list_create_ns(info):
    list = []
    dict = {}
    dict['ns'] = info[2]
    dict['ns_ttl'] = info[3]
    dict['update_time'] = info[6]
    list.append(dict)
    return list


def list_create_cname(info):
    list = []
    dict = {}
    dict['cname'] = info[4]
    dict['cname_ttl'] = info[5]
    dict['update_time'] = info[6]
    list.append(dict)
    return list


def mongo_update_a(collection,data,list_a):
    try:
        if data.has_key('a_record') == False:
            collection.update({'_id': data['_id']}, {'$set': {'a_record': list_a}})
        else:
            a_old = data['a_record'][-1]['a']
            a_new = list_a[0]['a']
            a_ttl_old = data['a_record'][-1]['a_ttl']
            a_ttl_new = list_a[0]['a_ttl']
            compare_a = set(a_old) ^ set(a_new)
            compare_ttl = set(a_ttl_old) ^ set(a_ttl_new)
            if list(compare_a) == [] and list(compare_ttl) == []:
                data['a_record'][-1]['update_time'] = list_a[0]['update_time']
                collection.update({'_id': data['_id']}, {'$set': {'a_record': data['a_record']}})
            else:
                data['a_record'].append(list_a[0])
                collection.update({'_id': data['_id']}, {'$set': {'a_record':data['a_record']}})
    except Exception, e:
        print e
        return


def mongo_update_ns(collection,data,list_ns):
    try:
        if data.has_key('ns_record') == False:
            collection.update({'_id': data['_id']}, {'$set': {'ns_record': list_ns}})
        else:
            ns_old = data['ns_record'][-1]['ns']
            ns_new = list_ns[0]['ns']
            ns_ttl_old = data['ns_record'][-1]['ns_ttl']
            ns_ttl_new = list_ns[0]['ns_ttl']
            compare_ns = set(ns_old) ^ set(ns_new)
            compare_ttl = set(ns_ttl_old) ^ set(ns_ttl_new)
            if list(compare_ns) == [] and list(compare_ttl) == []:
                data['ns_record'][-1]['update_time'] = list_ns[0]['update_time']
                collection.update({'_id': data['_id']}, {'$set': {'ns_record': data['ns_record']}})
            else:
                data['ns_record'].append(list_ns[0])
                collection.update({'_id': data['_id']}, {'$set': {'ns_record':data['ns_record']}})
    except Exception, e:
        print e
        return


def mongo_update_cname(collection,data,list_cname):
    try:
        if data.has_key('cname_record') == False:
            collection.update({'_id': data['_id']}, {'$set': {'cname_record': list_cname}})
        else:
            cname_old = data['cname_record'][-1]['cname']
            cname_new = list_cname[0]['cname']
            cname_ttl_old = data['cname_record'][-1]['cname_ttl']
            cname_ttl_new = list_cname[0]['cname_ttl']
            compare_cname = set(cname_old) ^ set(cname_new)
            compare_ttl = set(cname_ttl_old) ^ set(cname_ttl_new)
            if list(compare_cname) == [] and list(compare_ttl) == []:
                data['cname_record'][-1]['update_time'] = list_cname[0]['update_time']
                collection.update({'_id': data['_id']}, {'$set': {'cname_record': data['cname_record']}})
            else:
                data['cname_record'].append(list_cname[0])
                collection.update({'_id': data['_id']}, {'$set': {'cname_record':data['cname_record']}})
    except Exception, e:
        print e
        return


def mongo_handle():
    connection = pymongo.MongoClient('172.29.152.152', 27017)
    db = connection.malicious_domain_get_system
    collection = db.malicious_domain_info
    for data in collection.find():
        domain = data['domain']
        domain_info = get_domain_info(domain)
        list_a = list_create_a(domain_info)
        list_ns = list_create_ns(domain_info)
        list_cname = list_create_cname(domain_info)
        mongo_update_a(collection, data, list_a)
        mongo_update_ns(collection, data, list_ns)
        mongo_update_cname(collection, data, list_cname)


if __name__ == "__main__":
    while(1):
        mongo_handle()
