# -*- coding:utf-8 -*-
#数据库操作相关的工具类

from config_util import ConfigUtil
#python->mysql的连接器
import MySQLdb
#数据库连接池
from DBUtils.PooledDB import PooledDB
from log_util import Logger

class DBUtil(object):

    # def __init__(self,config_util,env='test'):
    #     self.config = config_util
    #     self.host = self.config.get_property_value("beeper2_bi.host")
    #     self.port = self.config.get_property_value("beeper2_bi.port")
    #     self.user_name = self.config.get_property_value("beeper2_bi.username")
    #     self.password = self.config.get_property_value("beeper2_bi.password")
    #     self.db = self.config.get_property_value("beeper2_bi.db")
    #     self.pool = PooledDB(
    #         creator=MySQLdb,
    #         mincached=1,
    #         maxcached=1,
    #         host=self.host,
    #         port=int(self.port),
    #         user=self.user_name,
    #         passwd=self.password,
    #         db=self.db,
    #         use_unicode=True,
    #         charset="utf8"
    #     )
    #     self.logger = Logger("db_util",env).get_logger()

    def __init__(self,host,port,user_name,password,db,env='test'):
        self.host = host
        self.port = port
        self.user_name = user_name
        self.password = password
        self.db = db
        self.pool = PooledDB(
            creator=MySQLdb,
            mincached=1,
            maxcached=1,
            host=self.host,
            port=int(self.port),
            user=self.user_name,
            passwd=self.password,
            db=self.db,
            use_unicode=True,
            charset="utf8"
        )
        self.logger = Logger("db_util",env).get_logger()


    def get_connection(self):
        try:
            connection = self.pool.connection()
            return  connection
        except Exception,e:
            self.logger.error("创建数据连接失败，异常信息为:%s" % (e))


    def release_connection(self,connection):
        try:
            connection.close()
        except Exception,e:
            self.logger.error("关闭mysql连接报错:%s" % (e))

    def query(self,sql):
        try:
            connection = self.get_connection()
            cursor = connection.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(sql)
            rows = cursor.fetchall()
            cursor.close()
            self.release_connection(connection)
            return rows
        except Exception,e:
            self.logger.error("执行查询:%s 出错" %(e))
            return None

    def insert_dict_into_table(self,table_name,data_dict):
        cols = ','.join(data_dict.keys())
        qmarks = ','.join(['%s'] * len(data_dict))
        insert_sql = 'insert into %s (%s) values(%s)' % (table_name,cols,qmarks)
        self.insert(insert_sql,data_dict.values())

    def insert(self,sql,values):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            cursor.execute(sql,values)
            connection.commit()
            cursor.close()
            self.release_connection(connection)
        except Exception,e:
            #self.logger.error("执行查询:%s 出错:%s" %(sql,e))
            print ("执行查询:%s 出错:%s" %(sql,e))
            connection.rollback()
            return None
        finally:
            self.release_connection(connection)

    def delete(self,sql):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            cursor.execute(sql)
            connection.commit()
            cursor.close()
            self.release_connection(connection)
        except Exception,e:
            self.logger.error("执行查询:%s 出错:%s" %(sql,e))
            connection.rollback()
            return None
        finally:
            self.release_connection(connection)

if __name__ == '__main__':
    config_util = ConfigUtil('test')
    host = config_util.get_property_value("beeper2_bi.host")
    port = config_util.get_property_value("beeper2_bi.port")
    user_name = config_util.get_property_value("beeper2_bi.username")
    password = config_util.get_property_value("beeper2_bi.password")
    db = config_util.get_property_value("beeper2_bi.db")

    db_util = DBUtil(host,port,user_name,password,db)
    connection = db_util.get_connection()
    cursor = connection.cursor()
    sql = "select count(*) from d5_report_type"
    cursor.execute(sql)
    data = cursor.fetchone()
    print data[0]
    cursor.close()
    db_util.release_connection(connection)



