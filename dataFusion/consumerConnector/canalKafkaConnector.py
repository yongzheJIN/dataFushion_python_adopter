import base64
import copy
import json
import time
# kafka连接模块
from confluent_kafka import Consumer, KafkaError
# 导入查询语句生成模块
from dataFusion.consumerConnector.common.commonFunction import organizedFunction
# 数据库连接模块
from dataFusion.mysqlConnector.mysqlConnector import mysqlConnector
from dataFusion.highgoConnector.highgoConnector import HighGoDBConnector
import datetime
import psycopg2


class kafkaConnector:
    def __init__(self, kakfkaHost, kafkaPort, kafkaTopic, kafkaGroup, mysqlip, mysqlport, mysqluser, mysqlpassword,
                 target_database_type, mysqlpdatabase, useReplace, maxKafkaPollingSize=100, kafkaConsumerModel=1,
                 mysqlread_timeout=20,
                 kafkaCosumerModel="earliest", logger=None, database_type="mysql"):
        ### kafka的设置
        self.kakfkaHost = kakfkaHost
        self.kafkaPort = kafkaPort
        self.kafkaTopic = kafkaTopic
        self.kafkaGroup = kafkaGroup
        self.database_type = database_type
        # 1代表如果不存在任何旗帜，就从最开始消费，如果存在就从消费点开始，其他的任何值都取latest只从最末尾去值
        self.kafkaConsumerModel = kafkaCosumerModel
        # 是否开始拉取数据
        self.pollingStatus = False
        # 每次拉取的最大数量
        self.maxKafkaPollingSize = maxKafkaPollingSize
        # 持久化kafka连接
        self.kafkaConsumer: Consumer
        ### mysql的设置
        self.mysqlip = mysqlip
        self.mysqlport = mysqlport
        self.mysqluser = mysqluser
        self.mysqlpassword = mysqlpassword
        self.mysqldatabase = mysqlpdatabase
        self.target_database_type = target_database_type
        self.useReplace = useReplace
        self.logger = logger

    def __enter__(self):
        try:
            # 测试mysql的连接
            if self.target_database_type == "highgo":
                with HighGoDBConnector(
                        ip=self.mysqlip, port=self.mysqlport, user=self.mysqluser, password=self.mysqlpassword,
                        database=self.mysqldatabase
                ):
                    print("连接highgo数据库成功") if self.logger == None else self.logger.info("连接mysql数据库成功")
            else:
                with mysqlConnector(ip=self.mysqlip, port=self.mysqlport, user=self.mysqluser,
                                    password=self.mysqlpassword,
                                    database=self.mysqldatabase):
                    print("连接mysql数据库成功") if self.logger == None else self.logger.info("连接mysql数据库成功")
            # 创建 Kafka 消费者配置
            consumer_conf = {
                'bootstrap.servers': f"{self.kakfkaHost}:{self.kafkaPort}",
                'group.id': self.kafkaGroup,
                # 每次都从offset的位置开始消费，而不是忘记消费失败数据
                'auto.offset.reset': 'earliest',
                # 不要自动提交事件
                'enable.auto.commit': False,
            }
            # 创建 Kafka 消费者
            self.kafkaConsumer = Consumer(consumer_conf)
            # 订阅主题
            self.kafkaConsumer.subscribe([self.kafkaTopic])
            print("连接kafka成功") if self.logger == None else self.logger.info("连接kafka成功")
            return self
        except Exception as e:
            raise ConnectionError(f"连接kafka失败{e}") if self.logger == None else self.logger.error(
                f"连接kafka失败{e}")

    # 装饰器函数,让你可以自己写自己内部的插入和更新操作
    def listenToPort(self, funcInsert, funcUpdate, funcDelete, mapAll: bool, schemaEvalution: bool):
        """
        参数说明
        funcInsert自己的Insert逻辑(会接受到event_type,header,database,table)
        funcUpdate自己的Update逻辑(会接受到event_type,header,database,table)
        内部变量说明
        event_type: 事件名称
        header:
        database: 修改的数据库

        """
        while True:
            # 控制每次拉取的数据size
            msgs = []
            res = []
            # 持续从 Kafka 主题中拉取消息
            continue_polling = True
            while continue_polling:
                msg = self.kafkaConsumer.poll(timeout=2)
                if msg is None:
                    # 如果数据伪空直接跳过结束次轮循环
                    break
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # 没有更多的消息可消费，停止继续拉取
                        continue_polling = False
                    else:
                        print(f"Error: {msg.error()}") if self.logger == None else self.logger.error(
                            f"Error: {msg.error()}")
                        break
                else:
                    # ... 处理消息的逻辑
                    msgs.append(msg)
                    # 如果达到一定的处理条数，也停止继续拉取
                    if len(msgs) > self.maxKafkaPollingSize:
                        continue_polling = False
            # sql统一存储的数组
            # 所有insert执行过的table 这里主要用于insert，把多个insert拼成一个{table:['xex_home_order'],index:['1']}
            InsertTableList = {"table": [], "index": []}
            DeleteTableList = {"table": [], "index": []}
            # 获取原始数据集
            for msg in msgs:
                originalDatas = json.loads(msg.value().decode("utf-8"))
                print('originalDatas', originalDatas)
                # 存储整个数据
                # 获取事件类型,数据库名,数据表
                # 于canalTcp适配 1-update, 2-insert,3-delete
                if self.database_type == "mysql":
                    if originalDatas['type'] == 'INSERT':
                        entryType = 1
                    elif originalDatas['type'] == "UPDATE":
                        entryType = 2
                    elif originalDatas['type'] == "DELETE":
                        entryType = 3
                    elif originalDatas['type'] == "ALTER" and schemaEvalution:
                        entryType = 4
                    elif schemaEvalution and (originalDatas['type'] == "CREATE" or originalDatas['type'] == 'ERASE'):
                        entryType = 5
                    else:
                        break
                else:
                    if originalDatas['payload']['op'] == 'c':
                        entryType = 1
                    elif originalDatas['payload']['op'] == "u":
                        entryType = 2
                    elif originalDatas['payload']['op'] == "d":
                        entryType = 3
                    elif originalDatas['payload']['op'] == "a" and schemaEvalution:
                        entryType = 4
                    else:
                        break
                # 数据变更
                datas, table = self.resolvingDataStructure(originalDatas, entryType, self.database_type)
                for data in datas:
                    if entryType == 1 or entryType == 2 or entryType == 3:
                        res = organizedFunction(event_type=entryType, funcInsert=funcInsert, funcUpdate=funcUpdate,
                                                funcDelete=funcDelete, table=table, InsertTableList=InsertTableList,
                                                DeleteTableList=DeleteTableList, res=res,
                                                data=data, useReplace=self.useReplace, mapAll=mapAll,
                                                database_type=self.database_type)
                    elif schemaEvalution and (entryType == 4 or entryType == 5):
                        res.append([data, None])
                # 表格变更
            if res:
                try:
                    # 传入数据，如果数据消费成功递交ack位置如果失败把mysqlConnector 和canalClient rollback
                    if self.target_database_type == "highgo":
                        with HighGoDBConnector(
                                ip=self.mysqlip, port=self.mysqlport, user=self.mysqluser,
                                password=self.mysqlpassword,
                                database=self.mysqldatabase
                        ) as connector:
                            # 制作cursor操作对象
                            cursor = connector.cursor()
                            for i in res:
                                cursor.execute(i[0], i[1])
                            connector.commit()
                    else:
                        with mysqlConnector(ip=self.mysqlip, port=self.mysqlport, user=self.mysqluser,
                                            password=self.mysqlpassword,
                                            database=self.mysqldatabase) as connector:
                            # 制作cursor操作对象
                            cursor = connector.cursor()
                            for i in res:
                                print(i)
                                cursor.execute(i[0], i[1])
                            connector.commit()
                    # # 成功之后递交我的ACK位置
                    self.kafkaConsumer.commit()
                    print(f"{datetime.datetime.now()}: 事件提交成功",
                          res[-1]) if self.logger == None else self.logger.info(
                        f"{datetime.datetime.now()}: 事件提交成功,{res[-1]}")
                    continue
                except Exception as e:
                    print({f"失败：回滚时间轴。"
                           f"失败原因:{e}"
                           f"{datetime.datetime.now()}失败sql:{i[0]}"}) if self.logger == None else self.logger.error(
                        {f"失败：回滚时间轴。"
                         f"失败原因:{e}"
                         f"{datetime.datetime.now()}失败sql:{i[0]}"})
                    # 如果失败就停止程序
                    break
            time.sleep(1)

    def resolvingDataStructure(self, originalDatas, entryType, database_type="mysql"):
        form_data = {}
        datas = []
        if entryType == 1 or entryType == 2 or entryType == 3:
            if database_type == "mysql":
                table = originalDatas['table']
                for index in range(len(originalDatas['data'])):
                    database = originalDatas['database']
                    # 获取主键Dict
                    form_data["primary_List"] = [{"name": i, "value": originalDatas['data'][index][i]} for i in
                                                 originalDatas["pkNames"]]
                    # 组成before数据和after数据
                    form_data['after'] = {key: values if values != None else "NULL" for key, values in
                                          originalDatas['data'][index].items()}
                    # 如果是更新,就要记录他之前的数据,如果不是直接设置为NONE,保持数据格式统一
                    if entryType == 2:
                        form_data['before'] = {key: values if values != None else "NULL" for key, values in
                                               originalDatas['data'][index].items()}
                    else:
                        form_data['before'] = {}
                    data = dict(
                        database=database,
                        table=table,
                        event_type=entryType,
                        data=form_data,
                    )
                    datas.append(copy.deepcopy(data))
                return datas, table
            else:
                # 提取 before 和 after 数据
                before_data = originalDatas['payload']['before']
                table = originalDatas['payload']['source']['table']
                database = originalDatas['payload']['source']['schema']
                data_type = {
                    data['field']: data['type'] if not data.get('name') else data.get('name')
                    for data in originalDatas['schema']['fields'][0]['fields']
                }
                data_parameter = {
                    data['field']: data.get('parameters', {})
                    for data in originalDatas['schema']['fields'][0]['fields']
                }
                if before_data:
                    form_data['primary_list'] = [
                        {
                            "name": field["field"],
                            "value": self.convert_value(originalDatas["payload"]["before"][field["field"]],
                                                        data_type[field['field']], data_parameter[field['field']])
                        }
                        for field in originalDatas["schema"]["fields"][0]["fields"]
                        if not field["optional"]
                    ]
                else:
                    form_data['primary_list'] = [
                        {
                            "name": field["field"],
                            "value": self.convert_value(originalDatas["payload"]["after"][field["field"]],
                                                        data_type[field['field']], data_parameter[field['field']])
                        }
                        for field in originalDatas["schema"]["fields"][0]["fields"]
                        if not field["optional"]
                    ]
                if before_data:
                    form_data['before'] = {key: self.convert_value(value, data_type[key], data_parameter[key]) for
                                           key, value
                                           in
                                           originalDatas["payload"]["before"].items()}
                if entryType != 3:
                    form_data['after'] = {key: self.convert_value(value, data_type[key], data_parameter[key]) for
                                          key, value
                                          in
                                          originalDatas["payload"]["after"].items()}
                data = dict(
                    database=database,
                    table=table,
                    event_type=entryType,
                    data=form_data,
                )
                return data, table
        elif entryType == 4 or entryType == 5:
            return [originalDatas['sql']], originalDatas['table']

    def convert_value(self, value, date_type, data_parameter):
        """
        针对需要特殊处理的字段类型
        """
        if value:
            # 做类型转换
            if date_type == "io.debezium.time.MicroTimestamp":
                timestamp_seconds = value / 1_000_000
                value = datetime.datetime.fromtimestamp(timestamp_seconds)
                value = str(value)
            elif date_type == "io.debezium.time.MicroTime":
                seconds, micros = divmod(value, 1_000_000)
                # 转换为 time 对象
                value = (datetime.datetime.min + datetime.timedelta(seconds=seconds, microseconds=micros)).time()
                value = str(value)
            # 原始是base64编码 -> bytes -> 二进制字符串
            elif date_type == "io.debezium.data.Bits":
                length = data_parameter['length']
                decoded_bytes = base64.b64decode(value)
                value = ''.join(f'{byte:0{length}b}' for byte in decoded_bytes)
            elif date_type == "io.debezium.time.Date":
                value = datetime.datetime(1970, 1, 1) + datetime.timedelta(days=value)
                value = str(value)
            elif date_type == "org.apache.kafka.connect.data.Decimal":
                # 1. Base64 解码
                binary_data = base64.b64decode(value)
                # 2. 转换为 BigInteger（大端序）
                value = int.from_bytes(binary_data, byteorder='big', signed=True)
                value = str(value)
            elif date_type == "bytes":
                binary_data = base64.b64decode(value)
                value = psycopg2.Binary(binary_data)
            return value
        else:
            return "NULL"

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.kafkaConsumer.close()
