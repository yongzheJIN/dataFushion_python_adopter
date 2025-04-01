import datetime

from canal.client import Client
from canal.protocol import EntryProtocol_pb2
import time

from dataFusion.consumerConnector.common.commonFunction import organizedFunction
from dataFusion.mysqlConnector.mysqlConnector import mysqlConnector


class canalConnector:
    def __init__(self, canalhost, canalport, canaltopic, canalgroup, mysqlip, mysqlport, mysqluser, mysqlpassword,
                 mysqlpdatabase, filterCondition, canalusername=None, canalpassword=None, useReplace=False):
        ### 连接canal
        self.canalhost = canalhost
        self.canalport = canalport
        self.canaltopic = canaltopic
        self.canalgroup = canalgroup.encode('utf-8') if isinstance(canalgroup, str) else canalgroup
        # 身份验证
        self.canalusername = canalusername
        self.canalpassword = canalpassword
        # 持久化TCP连接
        self.canalclient: Client
        ### 连接mysql
        self.mysqlip = mysqlip
        self.mysqlport = mysqlport
        self.mysqluser = mysqluser
        self.mysqlpassword = mysqlpassword
        self.mysqldatabase = mysqlpdatabase
        ### 是否使用replace
        self.useReplace = useReplace
        # 过滤条件
        self.filterCondition = filterCondition.encode('utf-8') if isinstance(filterCondition, str) else filterCondition

    def __enter__(self):
        try:
            # 测试mysql的连接
            with mysqlConnector(ip=self.mysqlip, port=self.mysqlport, user=self.mysqluser, password=self.mysqlpassword,
                                database=self.mysqldatabase) as cursor:
                print("连接Mysql成功")
            # 第一次： 初始化canalClient的连接
            self.canalclient = Client()
            # 尝试连接Canal
            self.canalclient.connect(self.canalhost, port=self.canalport)
            self.canalclient.check_valid(username=b'', password=b'')
            # 验证Canal的账户密码
            if self.canalusername and self.canalpassword:
                self.canalclient.check_valid(username=b'', password=b'')
            # 订阅数据库
            self.canalclient.subscribe(client_id=self.canalgroup, destination=self.canaltopic,
                                       filter=self.filterCondition)
            return self
        except Exception as e:
            raise ConnectionError(f"连接canal_失败{e}")

    # 可以自己写自己内部的插入、更新和删除操作
    def listenToPort(self, funcInsert=None, funcUpdate=None, funcDelete=None, useReplace=False, mapAll=False,
                     schemaEvalution=False):
        """
        event_type：1 insert 2 update 3 delete
        参数说明
            funcInsert自己的Insert逻辑x(会接受到{
                                db=database,
                                table=table,
                                event_type=event_type,
                                data=format_data,
                            }):返回给你数据，你可以自己组装成一个sql
            funcUpdate自己的Update逻辑(会接受到{
                                db=database,
                                table=table,
                                event_type=event_type,
                                data=format_data,
                            }):返回给你所有数据，你可以自己组装成sql
            funcDelete自己的Delete逻辑(会接收到data{
                                db=database,
                                table=table,
                                event_type=event_type,
                                data=format_data,
                            }):返回给你所有数据，你可以自己组装成sql
            useReplace是否把Insert中的Insert改成Replace语句(如果自己写了funcInsert这一条即不生效)
        """
        while True:
            # 获取信息
            res = []
            # 所有insert执行过的table 这里主要用于insert，把多个insert拼成一个{table:['xex_home_order'],index:['1']}
            InsertTableList = {"table": [], "index": []}
            DeleteTableList = {"table": [], "index": []}
            # 尝试获取100条信息。(without_ack代表不要自动去获取递交ack码，因为我想把它跟mysql cursor绑定)
            message = self.canalclient.get_without_ack(100)
            entries = message['entries']
            for entry in entries:
                entry_type = entry.entryType
                if entry_type in [EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN,
                                  EntryProtocol_pb2.EntryType.TRANSACTIONEND]:
                    continue
                # 获取他的事件类型，获取他的数据，并获取他的表格和数据库
                row_change = EntryProtocol_pb2.RowChange()
                row_change.MergeFromString(entry.storeValue)
                event_type = row_change.eventType
                header = entry.header
                database = header.schemaName
                table = header.tableName
                # 4-create table; 5-alter table; 6-drop table
                if event_type == 5 or event_type == 4 or event_type == 6:
                    if schemaEvalution and event_type == 5:
                        schema_name = row_change.ddlSchemaName
                        sql = row_change.sql.replace("\r", "")
                        sql = sql.replace("\n", "")
                        if schema_name not in sql:
                            sql = sql.replace(f"ALTER TABLE", f"ALTER TABLE `{schema_name}`.", 1)
                            sql = sql.replace(f"alter table", f"alter table `{schema_name}`.", 1)
                        res.append([sql, None])
                    elif schemaEvalution and event_type == 4:
                        schema_name = row_change.ddlSchemaName
                        sql = row_change.sql.replace("\r", "")
                        sql = sql.replace("\n", "")
                        if schema_name not in sql:
                            sql = sql.replace(f"CREATE TABLE", f"CREATE TABLE `{schema_name}`.", 1)
                            sql = sql.replace(f"create table", f"create table `{schema_name}`.", 1)
                        res.append([sql, None])
                    elif schemaEvalution and event_type == 6:
                        schema_name = row_change.ddlSchemaName
                        sql = row_change.sql.replace("\r", "")
                        sql = sql.replace("\n", "")
                        if schema_name not in sql:
                            sql = sql.replace(f"DROP TABLE", f"DROP TABLE `{schema_name}`.", 1)
                            sql = sql.replace(f"drop table", f"drop table `{schema_name}`.", 1)
                        res.append([sql, None])
                else:
                    for row in row_change.rowDatas:
                        format_data = dict()
                        format_data['before'] = format_data['after'] = dict()
                        format_data['primary_List'] = []
                        # 获取数据修改前的完整要素
                        for column in row.beforeColumns:
                            format_data['before'][column.name] = column.value
                            # 获取primaryKey List
                            if column.isKey == True:
                                format_data['primary_List'].append({"name": column.name,
                                                                    "value": column.value})
                        # 获取数据修改后的完整要素
                        for column in row.afterColumns:
                            format_data['after'][column.name] = column.value if column.isNull == False else 'NULL'
                        # 把数据组装起来传入处理逻辑
                        data = dict(
                            database=database,
                            table=table,
                            event_type=event_type,
                            data=format_data,
                        )
                        # 把insert,update和delete的逻辑都放在一起
                        res = organizedFunction(event_type=event_type, funcInsert=funcInsert, funcUpdate=funcUpdate,
                                                funcDelete=funcDelete, table=table, InsertTableList=InsertTableList,
                                                DeleteTableList=DeleteTableList,
                                                data=data, useReplace=self.useReplace, mapAll=mapAll, res=res)
            if res:
                try:
                    # 传入数据，如果数据消费成功递交ack位置如果失败把mysqlConnector 和canalClient rollback
                    with mysqlConnector(ip=self.mysqlip, port=self.mysqlport, user=self.mysqluser,
                                        password=self.mysqlpassword,
                                        database=self.mysqldatabase) as connector:
                        # 制作cursor操作对象
                        cursor = connector.cursor()
                        for i in range(len(res)):
                            tempcurrent = i
                            cursor.execute(res[i][0], res[i][1])
                        connector.commit()
                    # # 成功之后递交我的ACK位置
                    self.canalclient.ack(message_id=message["id"])
                    print(f"{datetime.datetime.now()}:事件提交成功", res[-1])
                    continue
                except Exception as e:
                    print("---------------")
                    print(f"{datetime.datetime.now()}:失败回滚时间轴", e, res[tempcurrent])
                    self.canalclient.rollback(message["id"])
                    break
            else:
                self.canalclient.ack(message_id=message["id"])
            time.sleep(1)

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.canalclient.disconnect()
        except Exception as e:
            print(f"程序已退出,因为{e}")
