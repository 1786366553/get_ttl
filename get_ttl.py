# -*- coding: UTF-8 -*-
import DNS
import pymongo
import socket
socket.setdefaulttimeout(5)


def dict_handle(return_dict):
    for range in return_dict:
        range.pop('classstr')
        range.pop('class')
        range.pop('rdlength')
        range.pop('type')
    return return_dict


def query_ttl(query):
    DNS.DiscoverNameServers()
    reqobj = DNS.Request()
    answerobj_a = reqobj.req(name=query, qtype=DNS.Type.NS, server=["223.5.5.5","223.6.6.6"])
    if not answerobj_a.answers:
        blank = ""
        return (blank,blank)
    else:
        return_dict = []
        for items in answerobj_a.answers:
            server = items['data']
            query_2 = "www." + query
            answerobj_b = reqobj.req(name=query_2, qtype=DNS.Type.A, server=server,timeout=5)
            return_dict = return_dict + answerobj_b.answers
        if not return_dict:
            return_dict_2 = []
            for items in answerobj_a.answers:
                server = items['data']
                query_2 = "www." + query
                answerobj_c = reqobj.req(name=query_2, qtype=DNS.Type.CNAME, server=server,timeout=5)
                return_dict_2 = return_dict + answerobj_c.answers
            if not return_dict_2:
                return ("","")
            else:
                return return_dict_2
        else:
            return return_dict


def a_cname_jduge(return_dict):
    a_list = []
    cname_list = []
    for range in return_dict:
        if range['typename'] == 'CNAME':
            cname_list.append(range)
        elif range['typename'] == 'A':
            a_list.append(range)
    return (a_list,cname_list)



def mongo_handle_1():
    connection=pymongo.MongoClient('172.29.152.152',27017)
    db=connection.domain_ttl
    collection=db.domain_ttl_test_copy
    for data in collection.find({'flag':0}):
        domain = data['domain'][5:]
        try:
            ttl_record = query_ttl(domain)
            ttl_record = dict_handle(ttl_record)
            ttl_record = a_cname_jduge(ttl_record)
            collection.update({'_id':data['_id']},{'$set':{'A_TTL':ttl_record[0],'CNAME_TTL':ttl_record[1],'flag':1}})
        except:
            collection.update({'_id':data['_id']},{'$set':{'A_TTL':"",'CNAME_TTL':"",'flag':1}})


if __name__ == "__main__":
    mongo_handle_1()