import psycopg2
from typing import Optional


class HighGoDBConnector:
    def __init__(self, ip: str, port: int, user: str, password: str,
                 database: Optional[str] = None, fix_database: bool = False):
        """
        瀚高数据库连接器

        :param ip: 数据库服务器IP
        :param port: 数据库端口
        :param user: 用户名
        :param password: 密码
        :param database: 数据库名(可选)
        :param fix_database: 是否固定数据库连接
        """
        if not isinstance(port, int):
            raise TypeError("port必须是数字")

        self.ip = ip
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connector: Optional[psycopg2.extensions.connection] = None

    def __enter__(self):
        try:
            conn_args = {
                'host': self.ip,
                'port': self.port,
                'user': self.user,
                'password': self.password,
                'database': self.database
            }
            self.connector = psycopg2.connect(**conn_args)
            # 瀚高数据库建议设置自动提交为False，使用事务控制
            self.connector.autocommit = False
            return self.connector
        except Exception as e:
            raise ConnectionError(f"瀚高数据库连接失败: {e}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connector:
            if exc_type is not None:
                print(f"发生异常，回滚事务: {exc_val}")  # 调试日志
                self.connector.rollback()
            else:
                self.connector.commit()
            self.connector.close()
            self.connector = None

    def execute_sql(self, sql: str, params: Optional[tuple] = None):
        """执行SQL语句"""
        with self as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(sql, params or ())
                    if cursor.description:  # 如果有返回结果
                        return cursor.fetchall()
                    return cursor.rowcount
                except Exception as e:
                    conn.rollback()
                    raise RuntimeError(f"SQL执行失败: {e}")
