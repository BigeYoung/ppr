from __future__ import print_function
import json
import logging
import grpc
from zeebe_grpc import gateway_pb2, gateway_pb2_grpc
import os
#ZEEBE_GATEWAY = os.environ['ZEEBE_GATEWAY']

from SPARQLWrapper import SPARQLWrapper, JSON
#import easygui as gui
#import uuid
import pymysql
import random
import socket


prefix_str = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX : <http://www.ppr_scut.com#>
    """
while 1:

    #zeebe信息##########################################
    def zeebe_msg(txt_path):
        print(txt_path)
        type_str = txt_path[:3]
        with grpc.insecure_channel(os.environ['ZEEBE_GATEWAY']) as channel:
            stub = gateway_pb2_grpc.GatewayStub(channel)
            variables = {type_str.lower() + "_path": txt_path}
            publishMessageResponse = stub.PublishMessage(
                publishMessageRequest=gateway_pb2.PublishMessageRequest(
                    # the name of the message
                    name=type_str.lower() + "-uploaded",
                    # how long the message should be buffered on the broker, in milliseconds
                    timeToLive=10000,
                    # correlationKey = type_str.lower()+"_path",
                    # the unique ID of the message; can be omitted. only useful to ensure only one message
                    # with the given ID will ever be published (during its lifetime)
                    # the message variables as a JSON document; to be valid, the root of the document must be an
                    # object, e.g. { "a": "foo" }. [ "foo" ] would not be valid.
                    variables=json.dumps(variables)
                )
            )
            print(publishMessageResponse)

    def on_message(client, userdata, msg):
        payload_json = bytes.decode(msg.payload)
        print(
            payload_json)  # b'{"EventName":"s3:ObjectCreated:Put","Key":"aml/_Robot_Grasp_AAS_Model.aml","Records":[{"eventVersion":"2.0","eventSource":"minio:s3","awsRegion":"","eventTime":"2020-09-03T09:16:43.983Z","eventName":"s3:ObjectCreated:Put","userIdentity":{"principalId":"YOURACCESSKEY"},"requestParameters":{"accessKey":"YOURACCESSKEY","region":"","sourceIPAddress":"10.244.0.0"},"responseElements":{"x-amz-request-id":"16313B5A71D964F8","x-minio-deployment-id":"c7cfef0d-b256-4514-94a1-0a14a6b469eb","x-minio-origin-endpoint":"http://10.244.2.25:9000"},"s3":{"s3SchemaVersion":"1.0","configurationId":"Config","bucket":{"name":"aml","ownerIdentity":{"principalId":"YOURACCESSKEY"},"arn":"arn:aws:s3:::aml"},"object":{"key":"_Robot_Grasp_AAS_Model.aml","size":5849928,"eTag":"7eafb3b517b94e1f041fc360e8b5f581","contentType":"application/octet-stream","userMetadata":{"content-type":"application/octet-stream"},"sequencer":"16313B5AD40396AF"}},"source":{"host":"10.244.0.0","port":"","userAgent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36"}}]}'
        payload = json.loads(payload_json)
        if payload['EventName'] != 's3:ObjectCreated:Put':
            return
        zeebe_msg(payload['Key'])
    ########################################################


    def createTCP():
        # 1.创建套接字
        tcp_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # 2.绑定端口
        addr = ("", 9999)
        tcp_server_socket.bind(addr)

        # 3.监听链接
        tcp_server_socket.listen(128)

        # 4.接收别人的连接
        # client_socket用来为这个客户端服务
        client_socket, client_addr = tcp_server_socket.accept()

        # 5.接收对方发送的数据
        recv_data = client_socket.recv(10000)
        print("接收到的数据：%s" % recv_data.decode("utf-8"))

        # 6.给对方发送数据
        #client_socket.send("hahaha".encode("utf-8"))

        # 7.关闭套接字
        #client_socket.close()
        #tcp_server_socket.close()


        return recv_data.decode("utf-8")


    #def getprodcutID(TCPdata):
     #   num_guid = TCPdata.find("Guid ")+1
      #  productID = TCPdata[num_guid : num_guid+36]
       # print("product guid is %s" % productID)
        #return productID

    ###################查询类型个数###################
    def querynum(Type):
        query_str = prefix_str + """
        SELECT ?a
        WHERE {
        ?a  rdf:type :"""+ Type + """.
    }
    LIMIT 1000
    """
        result_count = 0
        sparql = SPARQLWrapper("http://localhost:3030/PPR_model/query")
        sparql.setMethod('POST')
        sparql.setQuery(query_str)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        #print(query_str)
        for result in results["results"]["bindings"]:
             #print(result["a"]["value"].replace('http://www.ppr_scut.com#', ''))
             result_count += 1
        #     print("\n")
        #print(Type, "类型数量为", result_count)

        return result_count
    #################################################
    num_product = querynum("Box_wood") + querynum("Box_UDisk") + querynum("Box_bluetooth") + 1
    #product_ID = uuid.uuid1()

    #################创建产品及其特征###################
    def createproduct(index_type):#0-Box_wood 1-Box_UDisk 2-Box_bluetooth

        query_str = prefix_str + "INSERT DATA{ "\
        + ":Feature_1_product__" + str(num_product) + " rdf:type :Feature. "\
        + ":Feature_2_product__" + str(num_product) + " rdf:type :Feature. "\
        + ":Feature_3_product__" + str(num_product) + " rdf:type :Feature. "\
        + ":Feature_4_product__" + str(num_product) + " rdf:type :Feature. "\
        + ":Feature_5_product__" + str(num_product) + " rdf:type :Feature. "\
        + ":Feature_1_product__" + str(num_product) + " :priority 1. "\
        + ":Feature_2_product__" + str(num_product) + " :priority 2. "\
        + ":Feature_3_product__" + str(num_product) + " :priority 3. "\
        + ":Feature_4_product__" + str(num_product) + " :priority 4. "\
        + ":Feature_5_product__" + str(num_product) + " :priority 5. "

        feature_str = ":Feature_1_product_"
        box_str = ":Box_" + str(querynum("Box") + 1)
        cover_str = ":Cover_" + str(querynum("Cover") + 1)
        product_str = ''
        meterial_str = ''
        producttype_str = ''
        meterialtype_str = ''
        if index_type == "木雕":#0:
            producttype_str += ":Box_Wood"
            meterialtype_str += ":Wood"
            product_str += ":Box_Wood_" + str(querynum("Box_Wood") + 1)
            meterial_str += ":Wood_" + str(querynum("Wood") + 1)
            feature_str += ""
        elif index_type == "U盘":#1:
            producttype_str += ":Box_UDisk"
            meterialtype_str += ":UDisk"
            product_str += ":Box_UDisk_" + str(querynum("Box_UDisk") + 1)
            meterial_str += ":UDisk_" + str(querynum("UDisk") + 1)
        elif index_type == "蓝牙钥匙扣":#2:
            producttype_str += ":Box_Bluetooth"
            meterialtype_str += ":Bluetooth"
            product_str += ":Box_Bluetooth_" + str(querynum("Box_Bluetooth") + 1)
            meterial_str += ":Bluetooth_" + str(querynum("Bluetooth") + 1)

        query_str += product_str + " rdf:type " + producttype_str + ". "\
        + meterial_str  + " rdf:type " + meterialtype_str  + ". "\
        + box_str + " rdf:type :Box. "\
        + cover_str + " rdf:type :Cover. " \
        + product_str + " :ID '" + str(product_ID) + "'. " \
        + product_str + " :hasproductpart " + meterial_str + ". "\
        + product_str + " :hasproductpart " + box_str + ". "\
        + product_str + " :hasproductpart " + cover_str + ". "\
        + product_str + " :hasfeature " + ":Feature_1_product__" + str(num_product) + ". "\
        + product_str + " :hasfeature " + ":Feature_2_product__" + str(num_product) + ". "\
        + product_str + " :hasfeature " + ":Feature_3_product__" + str(num_product) + ". "\
        + product_str + " :hasfeature " + ":Feature_4_product__" + str(num_product) + ". "\
        + product_str + " :hasfeature " + ":Feature_5_product__" + str(num_product) + ". }"\

        print("访问内容为：", query_str)
        sparql = SPARQLWrapper("http://localhost:3030/PPR_model/update")
        sparql.setMethod('POST')
        #测试阶段不放开创建20200321
        sparql.setQuery(query_str)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        #print(results)
        return None
    #################################################
    recv_data = createTCP()
    sparql = SPARQLWrapper("http://localhost:3030/PPR_model/update")
    sparql.setMethod('POST')
    # 测试阶段不放开创建20200321
    sparql.setQuery(recv_data)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    #print("接收到的数据：%s" % recv_data.decode("utf-8"))
    num_guid = recv_data.find('Guid ') + 6
    product_ID = recv_data[num_guid: num_guid + 36]
    print("product guid is %s" % product_ID)
    #product_ID = getprodcutID(recv_data)
    #choices_str = gui.buttonbox(msg="创建一个产品进行测试", title="", choices=("U盘", "木雕", "蓝牙钥匙扣"))
    #创建产品
    #createproduct(choices_str)
    #title = gui.msgbox(msg="开始进行新调度！！",title="标题部分",ok_button="开始")

    print("*********开始新的调度*************")
    str_msg = ''
    for i in range (5):#i:0-4
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
            """ + "filter(?pri = " + "'" + str(i+1) + "'" + """).

        }
        LIMIT 100
        """
        print(query_str)
        sparql = SPARQLWrapper("http://localhost:3030/PPR_model/query")
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
#优化算法加在此
        if index_result > 1:
            if (random.randint(1, 10) > 5):
                target_index_result = 2
            else:
                target_index_result = 1
        else:
            target_index_result = 1
##################################
        print("index_result =", str(index_result), "target_index is",str(target_index_result))
        str_msg += '\n' + product_str[target_index_result] + "------------>" + process_str[target_index_result] +"------------>" + resource_str[target_index_result] + '\n\n'

        #########################创建assignresource###########################
        query_str = prefix_str + "INSERT DATA{ " \
                    + ":" +feature_str[target_index_result]+  " :assignresource :"+ resource_str[target_index_result] +"}"
        sparql = SPARQLWrapper("http://localhost:3030/PPR_model/update")
        sparql.setMethod('POST')
        sparql.setQuery(query_str)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        #########################存储数据库###########################
        db = pymysql.connect('localhost', "root", "timego666", "cpps_db")
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
        #############################################################
    #title = gui.msgbox(msg=str_msg, title="标题部分", ok_button="结束")

