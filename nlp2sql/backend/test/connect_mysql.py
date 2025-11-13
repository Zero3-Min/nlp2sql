# ...existing code...
import pymysql
from pymysql.cursors import DictCursor

# 连接数据库
class DB:
    def __init__(self, host, user, password, database=None):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None  

    def connect_to_database(self) -> bool:
        """建立连接，成功返回 True，失败返回 False"""
        try:
            kwargs = {
                "host": self.host,
                "user": self.user,
                "password": self.password,
                "cursorclass": DictCursor,  # 返回字典形式的行
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

    def execute_query(self, sql, fetch='all'):
        """
        执行查询并返回结果。fetch='all' 或 'one'。
        会在执行前检查连接是否存在。
        """
        if not self.connection:
            print("未连接到数据库，请先调用 connect_to_database()")
            return None

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                if fetch == 'one':
                    results = cursor.fetchone()
                else:
                    results = cursor.fetchall()
                for row in (results or []):
                    print(row)
            return results
        except Exception as e:
            print(f"执行查询失败: {e}")
            return None

    def close(self):
        """安全关闭连接"""
        if self.connection:
            try:
                self.connection.close()
            except Exception:
                pass
            finally:
                self.connection = None

    # 支持上下文管理
    def __enter__(self):
        self.connect_to_database()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


if __name__ == "__main__":
    host = 'localhost'
    user = 'root'
    password = '123456'
    database = 'vannadb'
    sql = """SELECT YEAR(i.InvoiceDate) AS Year, i.BillingCountry AS Country,
                SUM(il.UnitPrice * il.Quantity) AS TotalSales
                FROM Invoice i
                JOIN InvoiceLine il ON i.InvoiceId = il.InvoiceId
                GROUP BY YEAR(i.InvoiceDate), i.BillingCountry
                ORDER BY Year, TotalSales DESC;
    """


    with DB(host, user, password, database) as db:
        db.execute_query(sql)