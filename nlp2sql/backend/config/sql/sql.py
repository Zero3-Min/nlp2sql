import os
import pymysql
from pymysql.cursors import DictCursor
from typing import Optional, Dict, Any
from dotenv import load_dotenv

class DB:
    """
    - connect_to_database() 返回 bool 表示是否成功连接
    - execute_query() 返回 fetchall 或 fetchone 的结果
    - 支持上下文管理
    """
    def __init__(self, host: str, user: str, password: str, database: Optional[str] = None, port: int = 3306, connect_timeout: int = 5):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connect_timeout = connect_timeout
        self.connection: Optional[pymysql.connections.Connection] = None

    def connect_to_database(self) -> bool:
        try:
            kwargs: Dict[str, Any] = {
                "host": self.host,
                "user": self.user,
                "password": self.password,
                "port": self.port,
                "connect_timeout": self.connect_timeout,
                "cursorclass": DictCursor,
            }
            if self.database:
                kwargs["database"] = self.database
            self.connection = pymysql.connect(**kwargs)
            print("数据库连接成功")
            return True
        except Exception as e:
            print(f"数据库连接失败: {e}")
            self.connection = None
            return False

    def select_database(self, database: str) -> bool:
        """
        切换当前连接的数据库。
        若已连接则调用 connection.select_db(database)，否则仅设置属性以便后续连接时使用。
        """
        self.database = database
        if not self.connection:
            return True
        try:
            self.connection.select_db(database)
            return True
        except Exception as e:
            print(f"切换数据库失败: {e}")
            return False
        
    def execute_query(self, sql: str, fetch: str = "all"):
        if not self.connection:
            print("未连接到数据库，请先调用 connect_to_database()")
            return None
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                if fetch == "one":
                    return cursor.fetchone()
                return cursor.fetchall()
        except Exception as e:
            print(f"执行查询失败: {e}")
            return None

    def close(self):
        if self.connection:
            try:
                self.connection.close()
            except Exception:
                pass
            finally:
                self.connection = None

    def __enter__(self):
        self.connect_to_database()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


def create_db() -> DB:
    
    load_dotenv()
    host = os.getenv("DB_HOST")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = int(os.getenv("DB_PORT", 3306))
    database = os.getenv("DB_NAME") or None
    connect_timeout = int(os.getenv("DB_CONNECT_TIMEOUT", "5"))
    return DB(host=host, user=user, password=password, database=database, port=port, connect_timeout=connect_timeout)

