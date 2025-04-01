import json
import logging
from threading import Thread

from dataFusion.consumerConnector.canalKafkaConnector import kafkaConnector
from dataFusion.consumerConnector.canalTcpConnector import canalConnector
import os


def process_folder(loggingPath, loggerName, configfolder):
    """
    logginerName: logger文件的路径
    folder: 配置文件的路径
    """
    # 初始化日志文件夹
    if not os.path.exists("./logs"):
        os.mkdir('logs')
    # 创建 logger 实例，使用不同的名称和日志文件
    logger = logging.getLogger(loggingPath)
    logger.setLevel(logging.INFO)
    # 创建文件处理程序，将日志记录到指定的文件中
    log_filename = os.path.join(loggingPath, f"{loggerName}.log")
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    # 设置日志记录格式
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    # 将文件处理程序添加到 logger 实例中
    logger.addHandler(file_handler)

    with open(os.path.join(configfolder, "others/mysql.json"), encoding="utf-8") as fp:
        mysqlConfig = json.load(fp)
    with open(os.path.join(configfolder, "consumerConfig/wholeSetting.json"), encoding="utf-8") as fp:
        wholeSetting = json.load(fp)

    if wholeSetting["serviceModel"] == "kafka":
        with open(os.path.join(configfolder, "consumerConfig/kafka.json"), encoding="utf-8") as fp:
            kafkaSetting = json.load(fp)
        with kafkaConnector(kakfkaHost=kafkaSetting['host'], kafkaPort=kafkaSetting["port"],
                            kafkaTopic=kafkaSetting["topic"], kafkaGroup=kafkaSetting['group'],
                            mysqlip=mysqlConfig['ip'], mysqlport=mysqlConfig['port'], mysqluser=mysqlConfig['user'],
                            mysqlpassword=mysqlConfig['password'],
                            mysqlpdatabase=mysqlConfig['database'], useReplace=wholeSetting['useReplace'],
                            kafkaConsumerModel=kafkaSetting.get("consumerModel", None), logger=logger) as resummer:
            # 重写了INSERT和UPDATE Function
            resummer.listenToPort(funcInsert=None, funcUpdate=None, funcDelete=None, mapAll=wholeSetting['mapAll'],
                                  schemaEvalution=wholeSetting['schemaEvalution'])


    elif wholeSetting["serviceModel"] == "tcp":
        with open(os.path.join(configfolder, "consumerConfig/canal.json"), encoding="utf-8") as fp:
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


if __name__ == "__main__":
    current_directory = os.getcwd()
    multiLineConfig_folder = os.path.join(current_directory, "multiLineConfig")
    log_folder = "logs"  # 日志文件夹
    os.makedirs(log_folder, exist_ok=True)  # 创建日志文件夹（如果不存在）
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # logger file的路径
    path = os.path.join(os.getcwd(), log_folder)

    # 遍历读取config路径
    for folder_name in os.listdir(multiLineConfig_folder):
        if os.path.isdir(os.path.join(multiLineConfig_folder, folder_name)) and folder_name.startswith("config"):
            folder_path = os.path.join(multiLineConfig_folder, folder_name)
            # Start a new thread to process each folder
            thread = Thread(target=process_folder, args=(path, folder_name, folder_path,))
            thread.start()
