# 组装Insert、Update和Delete三种functions
import json
import os
import re

time = 0


def organizedFunction(event_type, funcInsert, funcUpdate, funcDelete, table, InsertTableList, DeleteTableList,
                      data, useReplace, res, mapAll, database_type):
    # 根据是INSERT、DELETE和UPDATE情况分开处理，每个都有自己不同的处理逻辑
    # insert的逻辑
    global time
    if event_type == 1:
        # 如果他不存在tablelist里面说明他是第一次插入
        if mapAll:
            current = __defaultInsertFunction(data, useReplace, database_type) if not funcInsert else funcInsert(data,
                                                                                                                 useReplace,
                                                                                                                 database_type)
        else:
            current = __indicationInsert(data, useReplace, database_type) if not funcInsert else funcInsert(data,
                                                                                                            useReplace,
                                                                                                            database_type)
        if useReplace == False:
            if table not in InsertTableList['table']:
                InsertTableList['table'].append(table)
                InsertTableList['index'].append(len(res))
                res.append(current)
            else:
                time += 1
                # 获取表在 InsertTableList 中的索引
                index = InsertTableList["index"][InsertTableList['table'].index(table)]

                # 合并两个 INSERT 语句的值部分
                combined_values = ', '.join(['%s'] * len(current[1]))
                # 将当前行的值添加到原始值中
                res[index][0] = res[index][0][:-1]
                res[index][0] = f"{res[index][0]},({combined_values});"
                res[index][1].extend(current[1])
                # 打印合并后的结果
        else:
            if current != []:
                res.append(current)
    # 更新逻辑
    elif event_type == 2:
        ## 如果是更新的话都是一个单独的语句直接塞入到res中即可
        if mapAll == True:
            current = __defaultUpdateFunction(data, useReplace=useReplace,
                                              database_type=database_type) if not funcUpdate else funcUpdate(data,
                                                                                                             useReplace,
                                                                                                             database_type)
        else:
            current = __indicationUpdateFunction(data, useReplace=useReplace,
                                                 database_type=database_type) if not funcUpdate else funcUpdate(data,
                                                                                                                useReplace,
                                                                                                                database_type)
        if current != []:
            res.append(current)
    # 删除逻辑
    elif event_type == 3:
        # 如果他不存在tablelist里面说明他是第一次删除不能做拼接
        if mapAll == True:
            current = __defaultDeleteFunction(data, database_type) if not funcDelete else funcDelete(data,
                                                                                                     database_type)
        else:
            current = __indicationDeleteFunction(data, database_type) if not funcDelete else funcDelete(data,
                                                                                                        database_type)
        if current != []:
            res.append(current)
    else:
        print('不识别的事件--------------------')
    return res


def __defaultInsertFunction(data, useReplace, database_type):
    """
    构造 INSERT 或 REPLACE 语句
    :param data: {
        table: 表名,
        database: 库名,
        data: {
            after: {字段: 值},
            primary_list: [{'name': 字段名, 'value': 原值}]
        }
    }
    :param useReplace: 是否启用替换（MySQL用 REPLACE，HighGo用 ON CONFLICT）
    :param database_type: 'mysql' / 'highgo' / 'postgresql'
    """
    table = data['table']
    database = data['database']
    after_data = data['data']['after']
    primary_list = data['data'].get('primary_list', [])

    # 不同数据库使用不同字段包裹符
    quote = '`' if database_type.lower() == 'mysql' else '"'

    rescolumns = ""
    resvalues = ""
    query_values = []

    for key, value in after_data.items():
        rescolumns += f"{quote}{key}{quote},"
        resvalues += "%s,"
        query_values.append(None if value == 'NULL' else value)

    # 去除最后一个逗号
    rescolumns = rescolumns.rstrip(',')
    resvalues = resvalues.rstrip(',')

    # 生成语句
    if useReplace:
        if database_type.lower() == 'mysql':
            insert_sql = f"REPLACE INTO {quote}{database}{quote}.{quote}{table}{quote} ({rescolumns}) VALUES({resvalues});"
        elif database_type.lower() in ('highgo', 'postgresql'):
            primary_keys = [item['name'] for item in primary_list]
            if not primary_keys:
                raise ValueError("HighGo/PostgreSQL 替换语句必须提供主键字段 primary_list")
            conflict_clause = ','.join([f'{quote}{key}{quote}' for key in primary_keys])
            # 忽略主键字段更新（只更新非主键字段）
            update_clause = ', '.join([
                f'{quote}{key}{quote} = EXCLUDED.{quote}{key}{quote}'
                for key in after_data.keys()
                if key not in primary_keys
            ])

            insert_sql = f'INSERT INTO {quote}{database}{quote}.{quote}{table}{quote} ({rescolumns}) VALUES({resvalues}) ' \
                         f'ON CONFLICT ({conflict_clause}) DO UPDATE SET {update_clause};'
        else:
            raise ValueError(f"不支持的数据库类型：{database_type}")
    else:
        insert_sql = f"INSERT INTO {quote}{database}{quote}.{quote}{table}{quote} ({rescolumns}) VALUES({resvalues});"

    return [insert_sql, query_values]


# 生成Insert语句
# def __defaultInsertFunction(data, useReplace, database_type):
#     """
#     data:{table:"变化的表",data:{primary_list:"主键"}}
#     """
#     # insert 逻辑
#     table = data['table']
#     database = data['database']
#
#     rescolumns = ""
#     resvalues = ""
#     query_values = []
#     for key, value in data['data']['after'].items():
#         rescolumns = rescolumns + "`" + key + "`" + ","
#         resvalues = resvalues + "%s,"
#
#         # 如果值是 'NULL'，则将 None 添加到 query_values，否则添加原始值
#         query_values.append(None if value == 'NULL' else value)
#
#     # 去除结尾的","
#     rescolumns = rescolumns[:-1]
#     resvalues = resvalues[:-1]
#     if useReplace:
#         insert_sql = f"REPLACE INTO {database}.{table} ({rescolumns}) VALUES({resvalues});"
#     else:
#         insert_sql = f"INSERT INTO {database}.{table} ({rescolumns}) VALUES({resvalues});"
#
#     # 返回包含 SQL 语句和参数值的列表
#     return [insert_sql, query_values]


# def __indicationInsert(data, useReplace, database_type):
#     # 判断对象是不是单引号包裹
#     pattern = re.compile(r"^'.*'$")
#     # 找有没有配置文件
#     if os.path.exists(f"config/consumerConfig/tableGroup/{data['table']}.json"):
#         with open(f"config/consumerConfig/tableGroup/{data['table']}.json") as fp:
#             mapConfig = json.load(fp)
#         table = list(mapConfig.keys())[0]
#         database = data['database'] if bool(mapConfig[table].get("targetDatabase", -1) == -1) else mapConfig[table].get(
#             "targetDatabase")
#         rescolumns = ""
#         resvalues = ""
#         query_values = []
#         # 从配置表里面读取数据
#         for key, value in mapConfig[table]['data'].items():
#             # rescolumns = rescolumns + key + "," if bool(pattern.match(value)) else rescolumns + value + ","
#             # 获取目标字段的columns名称组
#             rescolumns = rescolumns + key + ","
#             #  获取占位符组
#             resvalues = resvalues + "%s,"
#             # 如果单引号包裹说明你设置的默认值，如果没有就去源数据里面取
#             if bool(pattern.match(value)):
#                 query_values.append(value[1:-1])
#             else:
#                 query_values.append(None if data['data']['after'][value] == "NULL" else data['data']['after'][value])
#         # 去除结尾的","
#         rescolumns = rescolumns[:-1]
#         resvalues = resvalues[:-1]
#         # 根据一开始是否采取了replace组成sql,你不需要单条执行每条语句的效率,因为代码中默认就做了rewriteCompress,所有可以组合在一起的sql都会放在一起，且在一个transaction里面提交
#         if useReplace:
#             insert_sql = f"REPLACE INTO {database}.{table} ({rescolumns}) VALUES({resvalues});"
#         else:
#             insert_sql = f"INSERT INTO {database}.{table} ({rescolumns}) VALUES({resvalues});"
#         # 返回包含 SQL 语句和参数值的列表
#         return [insert_sql, query_values]
#     else:
#         return []

def __indicationInsert(data, useReplace, database_type):
    pattern = re.compile(r"^'.*'$")
    # 检查是否存在配置文件
    if os.path.exists(f"config/consumerConfig/tableGroup/{data['table']}.json"):
        with open(f"config/consumerConfig/tableGroup/{data['table']}.json") as fp:
            mapConfig = json.load(fp)

        table = list(mapConfig.keys())[0]
        database = data['database'] if bool(mapConfig[table].get("targetDatabase", -1) == -1) else mapConfig[table].get(
            "targetDatabase")
        rescolumns = ""
        resvalues = ""
        query_values = []

        # 决定使用的引号符号
        quote = '`' if database_type.lower() == 'mysql' else '"'
        # 构造字段与参数
        for key, value in mapConfig[table]['data'].items():
            rescolumns += f"{quote}{key}{quote},"
            resvalues += "%s,"

            if bool(pattern.match(value)):
                query_values.append(value[1:-1])
            else:
                query_values.append(None if data['data']['after'][value] == "NULL" else data['data']['after'][value])

        rescolumns = rescolumns.rstrip(',')
        resvalues = resvalues.rstrip(',')

        # === 构造 SQL ===
        if useReplace:
            if database_type.lower() == 'mysql':
                insert_sql = f"REPLACE INTO {quote}{database}{quote}.{quote}{table}{quote} ({rescolumns}) VALUES({resvalues});"
            elif database_type.lower() in ('highgo', 'postgresql'):
                # 获取主键信息
                primary_keys = list(mapConfig[table].get('primaryKey', {}).keys())
                if not primary_keys:
                    raise ValueError("HighGo/PostgreSQL 替换语句需要 primaryKey 配置")

                conflict_clause = ','.join([f'{quote}{key}{quote}' for key in primary_keys])

                # 构造更新字段（忽略主键）
                update_clause = ', '.join([
                    f'{quote}{key}{quote} = EXCLUDED.{quote}{key}{quote}'
                    for key in mapConfig[table]['data'].keys()
                    if key not in primary_keys
                ])

                insert_sql = f'INSERT INTO {quote}{database}{quote}.{quote}{table}{quote} ({rescolumns}) VALUES({resvalues}) ' \
                             f'ON CONFLICT ({conflict_clause}) DO UPDATE SET {update_clause};'
            else:
                raise ValueError(f"不支持的数据库类型：{database_type}")
        else:
            insert_sql = f"INSERT INTO {quote}{database}{quote}.{quote}{table}{quote} ({rescolumns}) VALUES({resvalues});"

        return [insert_sql, query_values]
    else:
        return []


# 生成update语句
def __defaultUpdateFunction(data, useReplace, database_type):
    database = data['database']
    table = data['table']
    after_data = data['data']['after']

    # 决定使用的字段引号符号
    if database_type.lower() == 'mysql':
        quote = '`'
    else:
        quote = '"'

    # 构造字段部分
    rescolumns = ""
    resvalues = ""
    query_values = []
    for key, value in after_data.items():
        rescolumns += f'{quote}{key}{quote},'
        resvalues += "%s,"
        query_values.append(None if value == 'NULL' else value)

    rescolumns = rescolumns.rstrip(',')
    resvalues = resvalues.rstrip(',')
    if useReplace:
        if database_type.lower() == 'mysql':
            # MySQL 使用反引号包裹库名和表名
            update_sql = f"REPLACE INTO {quote}{database}{quote}.{quote}{table}{quote} ({rescolumns}) VALUES({resvalues});"
            return [update_sql, query_values]

        elif database_type.lower() in ('highgo', 'postgresql'):
            primary_keys = [item['name'] for item in data['data']['primary_list']]
            if not primary_keys:
                raise ValueError("HighGo 替换语句需要主键信息（primary_list）")

            conflict_clause = ','.join([f'{quote}{key}{quote}' for key in primary_keys])
            update_set_clause = ', '.join(
                [f'{quote}{key}{quote} = EXCLUDED.{quote}{key}{quote}' for key in after_data.keys()])

            update_sql = f'INSERT INTO {quote}{database}{quote}.{quote}{table}{quote} ({rescolumns}) VALUES({resvalues}) ' \
                         f'ON CONFLICT ({conflict_clause}) DO UPDATE SET {update_set_clause};'
            return [update_sql, query_values]

        else:
            raise ValueError(f"不支持的数据库类型：{database_type}")
    else:
        # 普通 UPDATE 语句
        set_clause = ""
        query_values = []

        for key, value in after_data.items():
            set_clause += f'{quote}{key}{quote} = %s, '
            query_values.append(None if value == 'NULL' else value)

        set_clause = set_clause.rstrip(', ')
        where_clause = ' AND '.join([f'{quote}{item["name"]}{quote} = %s' for item in data['data']['primary_list']])
        where_values = [item['value'] for item in data['data']['primary_list']]
        query_values += where_values

        update_sql = f'UPDATE {quote}{database}{quote}.{quote}{table}{quote} SET {set_clause} WHERE {where_clause};'
        return [update_sql, tuple(query_values)]


# def __defaultUpdateFunction(data, useReplace, database_type):
#     # update逻辑
#     database = data['database']
#     table = data['table']
#     if useReplace:
#         # 获取执行的对象表名
#         table = data['table']
#         rescolumns = ""
#         resvalues = ""
#         query_values = []
#         # 遍历形成columns的列表和values的%s
#         for key, value in data['data']['after'].items():
#             rescolumns = rescolumns + "`" + key + "`" + ","
#             resvalues = resvalues + "%s,"
#
#             # 如果值是 'NULL'，则将 None 添加到 query_values，否则添加原始值
#             query_values.append(None if value == 'NULL' else value)
#         # 去除结尾的","
#         rescolumns = rescolumns[:-1]
#         resvalues = resvalues[:-1]
#
#         update_sql = f"REPLACE INTO {database}.{table} ({rescolumns}) VALUES({resvalues});"
#         # 返回包含 SQL 语句和参数值的列表
#         return [update_sql, query_values]
#     else:
#         set_clause = ""
#         query_values = []
#
#         # 遍历形成 SET 子句
#         for key, value in data['data']['after'].items():
#             set_clause += f"{key} = %s, "
#
#             # 如果值是 'NULL'，则将 None 添加到 query_values，否则添加原始值
#             query_values.append(None if value == 'NULL' else value)
#
#         # 去除结尾的", "
#         set_clause = set_clause[:-2]
#         # 准备 WHERE 子句
#         where_clause = ' AND '.join([f"{item['name']} = %s" for item in data['data']['primary_List']])
#         where_values = [item['value'] for item in data['data']['primary_List']]
#
#         # 合并 SET 子句和 WHERE 子句的参数值
#         query_values += where_values
#         # 准备完整的 SQL 语句
#         update_sql = f"UPDATE {database}.{table} SET {set_clause} WHERE {where_clause};"
#
#         # 返回包含 SQL 语句和参数值的列表
#         return [update_sql, tuple(query_values)]


def __indicationUpdateFunction(data, useReplace, database_type):
    pattern = re.compile(r"^'.*'$")
    rescolumns = ""
    resvalues = ""
    query_values = []
    if os.path.exists(f"config/consumerConfig/tableGroup/{data['table']}.json"):
        with open(f"config/consumerConfig/tableGroup/{data['table']}.json") as fp:
            mapConfig = json.load(fp)

        table = list(mapConfig.keys())[0]
        database = data['database'] if bool(mapConfig[table].get("targetDatabase", -1) == -1) else mapConfig[table].get(
            "targetDatabase")
        quote = '`' if database_type.lower() == 'mysql' else '"'

        # === REPLACE 模式 ===
        if useReplace:
            for key, value in mapConfig[table]['data'].items():
                rescolumns += f"{quote}{key}{quote},"
                resvalues += "%s,"

                if pattern.match(value):
                    query_values.append(value[1:-1])
                else:
                    query_values.append(
                        None if data['data']['after'][value] == "NULL" else data['data']['after'][value])

            rescolumns = rescolumns.rstrip(',')
            resvalues = resvalues.rstrip(',')

            if database_type.lower() == 'mysql':
                update_sql = f"REPLACE INTO {quote}{database}{quote}.{quote}{table}{quote} ({rescolumns}) VALUES({resvalues});"
                return [update_sql, query_values]

            elif database_type.lower() in ('highgo', 'postgresql'):
                # 获取主键
                primary_keys = list(mapConfig[table].get('primaryKey', {}).keys())
                if not primary_keys:
                    raise ValueError("HighGo 替换语句需要主键信息（primaryKey）")

                conflict_clause = ','.join([f'{quote}{key}{quote}' for key in primary_keys])

                # 构建 update set 子句（不更新主键）
                update_set_clause = ', '.join([
                    f'{quote}{key}{quote} = EXCLUDED.{quote}{key}{quote}'
                    for key in mapConfig[table]['data'].keys()
                    if key not in primary_keys
                ])

                update_sql = f'INSERT INTO {quote}{database}{quote}.{quote}{table}{quote} ({rescolumns}) VALUES({resvalues}) ' \
                             f'ON CONFLICT ({conflict_clause}) DO UPDATE SET {update_set_clause};'
                return [update_sql, query_values]
            else:
                raise ValueError(f"不支持的数据库类型：{database_type}")

        # === UPDATE 模式 ===
        else:
            set_clause = ""
            query_values = []

            for key, value in mapConfig[table]['data'].items():
                set_clause += f"{quote}{key}{quote} = %s, "
                if pattern.match(value):
                    query_values.append(value[1:-1])
                else:
                    query_values.append(
                        None if data['data']['after'][value] == "NULL" else data['data']['after'][value])

            set_clause = set_clause.rstrip(', ')

            where_clause = ' AND '.join([f"{quote}{key}{quote} = %s" for key in mapConfig[table]['primaryKey'].keys()])
            where_values = [
                value[1:-1] if pattern.match(value) else data['data']['after'][value]
                for key, value in mapConfig[table]['primaryKey'].items()
            ]
            query_values += where_values

            update_sql = f"UPDATE {quote}{database}{quote}.{quote}{table}{quote} SET {set_clause} WHERE {where_clause};"
            return [update_sql, query_values]

    else:
        return []


# 生成DELETE语句不需要对照mapconfig完完全全的影子库
def __defaultDeleteFunction(data, database_type):
    database = data['database']
    primary_list = data['data']['primary_list']
    table = data['table']
    if database_type == "mysql":
        queto = "`"
    else:
        queto = '"'
    where_clasuse = ' AND '.join([f"{queto}{item['name']}{queto} = %s" for item in primary_list])
    delete_sql = f"DELETE from {database}.{table} where {where_clasuse};"
    return [delete_sql, [i['value'] for i in primary_list]]


# 生成DELETE需要对照mapConfig的部分
def __indicationDeleteFunction(data, database_type):
    # 判断对象是不是单引号包裹
    pattern = re.compile(r"^'.*'$")
    table = data['table']
    if database_type == "mysql":
        queto = "`"
    else:
        queto = '"'
    if os.path.exists(f"config/consumerConfig/tableGroup/{table}.json"):
        with open(f"config/consumerConfig/tableGroup/{table}.json") as fp:
            mapConfig = json.load(fp)
        table = list(mapConfig.keys())[0]
        database = data['database'] if bool(mapConfig[table].get("targetDatabase", -1) == -1) else mapConfig[table].get(
            "targetDatabase")
        where_clasuse = ' AND '.join(
            [f"{queto}{key}{queto} = %s" if not bool(pattern.match(value)) else f"{key} = %s" for key, value in
             mapConfig[table]['primaryKey'].items()])
        where_values = [data['data']['after'][value] if not bool(pattern.match(value)) else value[1:-1] for key, value
                        in mapConfig[table]['primaryKey'].items()]
        delete_sql = f"DELETE from {database}.{table} where {where_clasuse};"
        return [delete_sql, where_values]
    else:
        return []
