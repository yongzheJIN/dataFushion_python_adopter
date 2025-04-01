import json
from dataFusion.consumerConnector.canalKafkaConnector import kafkaConnector
from dataFusion.consumerConnector.canalTcpConnector import canalConnector

if __name__ == "__main__":
    ### kafka的配置
    with open("config/others/mysql.json", encoding="utf-8") as fp:
        mysqlConfig = json.load(fp)
    with open("config/consumerConfig/wholeSetting.json", encoding="utf-8") as fp:
        wholeSetting = json.load(fp)
    if wholeSetting["serviceModel"] == "kafka":
        print("开启KAFKA模式")
        with open("config/consumerConfig/kafka.json", encoding="utf-8") as fp:
            kafkaSetting = json.load(fp)
        with kafkaConnector(kakfkaHost=kafkaSetting['host'], kafkaPort=kafkaSetting["port"],
                            kafkaTopic=kafkaSetting["topic"], kafkaGroup=kafkaSetting['group'],
                            mysqlip=mysqlConfig['ip'], mysqlport=mysqlConfig['port'], mysqluser=mysqlConfig['user'],
                            mysqlpassword=mysqlConfig['password'],
                            mysqlpdatabase=mysqlConfig['database'], useReplace=wholeSetting['useReplace'],
                            kafkaConsumerModel=kafkaSetting.get("consumerModel", None),
                            database_type=kafkaSetting['database_type'], target_database_type=mysqlConfig['database_type']) as resummer:
            # 重写了INSERT和UPDATE Function
            resummer.listenToPort(funcInsert=None, funcUpdate=None, funcDelete=None, mapAll=wholeSetting['mapAll'],
                                  schemaEvalution=wholeSetting['schemaEvalution'])
    ### tcp配置
    elif wholeSetting["serviceModel"] == "tcp":
        print("开启TCP模式")
        with open("./config/consumerConfig/canal.json", 'utf-8') as fp:
            canalSetting = json.load(fp)
        with canalConnector(canalhost=canalSetting["canalhost"], canalport=canalSetting["port"],
                            canaltopic=canalSetting["topic"], canalgroup=canalSetting["group"],
                            mysqlip=mysqlConfig['ip'], mysqlport=mysqlConfig['port'], mysqluser=mysqlConfig['user'],
                            mysqlpassword=mysqlConfig['password'],
                            mysqlpdatabase=mysqlConfig['database'], canalusername=None, canalpassword=None,
                            useReplace=wholeSetting["useReplace"],
                            filterCondition=canalSetting["filterCondition"]) as resummer:
            resummer.listenToPort(funcInsert=None, funcUpdate=None, funcDelete=None, mapAll=wholeSetting['mapAll'],
                                  schemaEvalution=wholeSetting['schemaEvalution'])
