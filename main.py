from __future__ import print_function
import json
import logging
import grpc
from zeebe_grpc import gateway_pb2, gateway_pb2_grpc
import os

from SPARQLWrapper import SPARQLWrapper, JSON
#import easygui as gui
#import uuid
import pymysql
import random
import socket

ZEEBE_GATEWAY   = os.environ['ZEEBE_GATEWAY']
JENA_SERVER     = os.environ['JENA_SERVER']
MINIO_SERVER    = os.environ['MINIO_SERVER']
MYSQL_SERVER    = os.environ['MYSQL_SERVER']
MYSQL_USER      = os.environ['MYSQL_USER']
MYSQL_PASSWD    = os.environ['MYSQL_PASSWD']
MYSQL_DB        = os.environ['MYSQL_DB']

def PPR_main():
    prefix_str = """
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            PREFIX : <http://www.ppr_scut.com#>
        """
    minioClient = Minio(endpoint=MINIO_SERVER, access_key='YOURACCESSKEY', secret_key='YOURSECRETKEY', secure=False)


    minioClient.fget_object('txt', 'cpps.txt', 'cpps.sparql')

    abc_file = open("cpps.sparql", "r")
    # print(abc_file.readlines())
    recv_data = abc_file.read()
    recv_data = recv_data.replace('#end#', '')
    print(recv_data)

    sparql = SPARQLWrapper(JENA_SERVER+"/PPR_model/update")
    sparql.setMethod('POST')
    # 测试阶段不放开创建20200321
    sparql.setQuery(recv_data)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    # print("接收到的数据：%s" % recv_data.decode("utf-8"))
    num_guid = recv_data.find('Guid ') + 6
    product_ID = recv_data[num_guid: num_guid + 36]
    print("product guid is %s" % product_ID)
    print("*********开始新的调度*************")
    str_msg = ''
    for i in range(5):  # i:0-4
        query_str = prefix_str + """
               SELECT ?product ?process ?resource ?resourceID ?productID ?feature
               WHERE {
               ?product :Guid '""" + str(product_ID) + "'. " \
                    + """?product rdf:type :Product.
               ?product :hasfeature ?feature.

               ?feature :priority ?pri.
               ?feature :FeatureDescription ?description.
               ?feature :Object_I ?object.
               ?process :generate_feature ?description.
               ?process :Object  ?object.
               ?resource :hascapability ?process.
               ?resource :Guid ?resourceID.
               """ + "filter(?pri = " + "'" + str(i + 1) + "'" + """).

           }
           LIMIT 100
           """
        print(query_str)
        sparql = SPARQLWrapper(JENA_SERVER+"/PPR_model/query")
        sparql.setMethod('POST')
        sparql.setQuery(query_str)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        product_str = ['']
        productID_str = ['']
        feature_str = ['']
        process_str = ['']
        resource_str = ['']
        resourceID_str = ['']

        index_result = 0
        target_index_result = 0
        for result in results["results"]["bindings"]:
            product_str.append(result["product"]["value"].replace('http://www.ppr_scut.com#', ''))
            process_str.append(result["process"]["value"].replace('http://www.ppr_scut.com#', ''))
            resource_str.append(result["resource"]["value"].replace('http://www.ppr_scut.com#', ''))
            productID_str.append(result["productID"]["value"].replace('http://www.ppr_scut.com#', ''))
            resourceID_str.append(result["resourceID"]["value"].replace('http://www.ppr_scut.com#', ''))
            feature_str.append(result["feature"]["value"].replace('http://www.ppr_scut.com#', ''))
            index_result += 1
            print(product_str[index_result])
            print(process_str[index_result])
            print(resource_str[index_result])
            print("\n")
        # 优化算法加在此
        if index_result > 1:
            if (random.randint(1, 10) > 5):
                target_index_result = 2
            else:
                target_index_result = 1
        else:
            target_index_result = 1
        ##################################
        print("index_result =", str(index_result), "target_index is", str(target_index_result))
        str_msg += '\n' + product_str[target_index_result] + "------------>" + process_str[
            target_index_result] + "------------>" + resource_str[target_index_result] + '\n\n'

        #########################创建assignresource###########################
        query_str = prefix_str + "INSERT DATA{ " \
                    + ":" + feature_str[target_index_result] + " :assignresource :" + resource_str[
                        target_index_result] + "}"
        sparql = SPARQLWrapper(JENA_SERVER+"/PPR_model/update")
        sparql.setMethod('POST')
        sparql.setQuery(query_str)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        #########################存储数据库###########################
        db = pymysql.connect(MYSQL_SERVER, MYSQL_USER, MYSQL_PASSWD, MYSQL_DB)
        # 使用cursor()方法获取操作游标
        cursor = db.cursor()
        # SQL 插入语句
        sql = "INSERT INTO tab_productinfo\
                (Guid_product, Product_name, Operation_name, Operation_machine_name, Operation_machine_ID)  \
               VALUES('%s', '%s', '%s', '%s', '%s')" % \
              (productID_str[target_index_result], product_str[target_index_result], \
               process_str[target_index_result], resource_str[target_index_result], resourceID_str[target_index_result])

        try:
            # 执行sql语句
            cursor.execute(sql)
            # 执行sql语句
            db.commit()
        except:
            # 发生错误时回滚
            db.rollback()

        # 关闭数据库连接
        db.close()
    return 0

with grpc.insecure_channel(ZEEBE_GATEWAY) as channel:
    stub = gateway_pb2_grpc.GatewayStub(channel)
    for jobResponse in stub.ActivateJobs(gateway_pb2.ActivateJobsRequest(type = 'payment-service', worker = 'ppr-client', timeout = 1000, maxJobsToActivate = 32)):
        for job in jobResponse.jobs:
            print(job)
            PPR_main()
            stub.CompleteJob(gateway_pb2.CompleteJobRequest(jobKey = job.key))
