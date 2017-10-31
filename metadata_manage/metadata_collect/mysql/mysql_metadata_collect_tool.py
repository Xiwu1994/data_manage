#!/usr/bin/env python
# -*- coding:utf-8 -*-
#读取mysql数据源的元数据的工具类
#需要读取的元数据包括：
#1、数据库的元数据信息
#2、数据表的元数据信息
#3、数据表字段的元数据信息

import sys
import re
import os
pwd = os.path.realpath(__file__)
print '/'.join(pwd.split('/')[:-3])
sys.path.append('/'.join(pwd.split('/')[:-3]))

from util.db_util import DBUtil
from util.config_util import ConfigUtil
from util.date_util import DateUtil


business_db_meta_dbutil = ''
current_date = ''
has_insert_point_partitioned_table = False
has_insert_order_tiles_partitioned_table = False

def delete_metadata():
    delete_metadata_by_table('business_db',current_date)
    delete_metadata_by_table('business_table', current_date)
    delete_metadata_by_table('business_column', current_date)

def collect_database_metadata(server_config,db_util):

    query_sql = "select SCHEMA_NAME,DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME from SCHEMATA"

    rows = db_util.query(query_sql)

    system_db = ['information_schema', 'mysql', 'performance_schema']

    host_name = server_config['host']
    port = server_config['port']

    for row in rows:
        db_name = row['SCHEMA_NAME']
        default_character_set_name = row['DEFAULT_CHARACTER_SET_NAME']
        default_collation_name = row['DEFAULT_COLLATION_NAME']
        if db_name not in system_db:
            db_row = {}
            db_row['host_name'] = host_name
            db_row['port'] = port
            db_row['db_name'] = db_name
            db_row['db_type'] = '1'
            db_row['business_type'] = ''
            db_row['status'] = 1
            db_row['db_comment'] = ''
            db_row['default_character_set_name'] = default_character_set_name
            db_row['default_collation_name'] = default_collation_name
            db_row['version'] = current_date
            table_count = collect_table_metadata(db_row,db_util)
            db_row['table_count'] = table_count
            insert_metadata(db_row,'business_db')

def collect_table_metadata(db_row,db_util):

    query_sql = 'select TABLE_NAME,TABLE_TYPE,ENGINE,TABLE_ROWS,TABLE_COMMENT,CREATE_TIME,UPDATE_TIME,DATA_LENGTH,TABLE_COLLATION from TABLES where TABLE_SCHEMA="'+db_row['db_name']+'"'
    rows = db_util.query(query_sql)

    is_need_insert_table = True

    global has_insert_order_tiles_partitioned_table
    global has_insert_point_partitioned_table

    for row in rows:
        table_row = {}
        table_row['db_name'] = db_row['db_name']
        table_row['host_name'] = db_row['host_name']
        table_row['port'] = db_row['port']
        table_name = row['TABLE_NAME']
        partioned_table_name = table_name
        is_find_partioned_table = is_partioned_table(table_name)
        if is_find_partioned_table:
            is_need_insert_table = False
            if table_name.startswith('order_tiles') and not has_insert_order_tiles_partitioned_table:
                has_insert_order_tiles_partitioned_table = True
                table_name = 'order_tiles_' + DateUtil().get_current_date('%Y_%m_%d')
                partioned_table_name = 'order_tiles_yyyy_MM_dd'
                print table_name,partioned_table_name
                is_need_insert_table = True
            elif table_name.startswith('point') and not has_insert_point_partitioned_table:
                has_insert_point_partitioned_table = True
                table_name = 'point_' + DateUtil().get_current_date('%Y_%m_%d')
                partioned_table_name = 'point_yyyy_MM_dd'
                print table_name,partioned_table_name
                is_need_insert_table = True

        if is_need_insert_table:
            table_row['table_name'] = table_name
            table_row['table_type'] = row['TABLE_TYPE']
            table_row['engine'] = row['ENGINE']
            table_row['table_rows'] = row['TABLE_ROWS']
            table_row['table_comment'] = row['TABLE_COMMENT']
            table_row['create_time'] = row['CREATE_TIME']
            table_row['update_time'] = row['UPDATE_TIME']
            table_row['data_length'] = row['DATA_LENGTH']
            table_row['table_collation'] = row['TABLE_COLLATION']
            table_row['status'] = 1
            table_cols = collect_column_metadata(table_row,db_util,partioned_table_name,table_name)
            table_row['table_name'] = partioned_table_name
            table_row['table_cols'] = table_cols
            table_row['version'] = current_date
            insert_metadata(table_row,'business_table')

    return len(rows)

def collect_column_metadata(table_row,db_util,partioned_table_name,table_name):

    query_sql = 'select COLUMN_NAME,DATA_TYPE,COLUMN_TYPE,IS_NULLABLE,COLUMN_DEFAULT,CHARACTER_SET_NAME,COLLATION_NAME,COLUMN_COMMENT,ORDINAL_POSITION from COLUMNS where TABLE_NAME="' + table_name +'" and TABLE_SCHEMA="'+table_row['db_name']+'"'
    rows = db_util.query(query_sql)

    for row in rows:
        column_row = {}
        column_row['db_name'] = table_row['db_name']
        column_row['host_name'] = table_row['host_name']
        column_row['port'] = table_row['port']
        column_row['table_name'] = partioned_table_name #table_row['table_name']
        column_row['column_name'] = row['COLUMN_NAME']
        column_row['data_type'] = row['DATA_TYPE']
        column_row['column_type'] = row['COLUMN_TYPE']
        column_row['is_nullable'] = row['IS_NULLABLE']
        column_row['column_default'] = row['COLUMN_DEFAULT']
        column_row['character_set_name'] = row['CHARACTER_SET_NAME']
        column_row['collation_name'] = row['COLLATION_NAME']
        column_row['column_comment'] = row['COLUMN_COMMENT']
        column_row['ordinal_position'] = row['ORDINAL_POSITION']
        column_row['version'] = current_date
        insert_metadata(column_row,'business_column')

    return len(rows)



def insert_metadata(data_dict,table_name):
    business_db_meta_dbutil.insert_dict_into_table(table_name,data_dict)

def delete_metadata_by_table(table_name,version):
    delete_sql = 'delete from ' +table_name +' where version="' + version +'"'
    business_db_meta_dbutil.delete(delete_sql)


def is_partioned_table(table_name):
    # 将正则表达式编译成pattern对象
    order_tiles_pattern = re.compile("order_tiles_\d{4}_\d{2}_\d{2}")
    point_pattern = re.compile("point_\d{4}_\d{2}_\d{2}")

    if order_tiles_pattern.match(table_name) or point_pattern.match(table_name):
        return True
    else:
        return False



if __name__ == '__main__':
    env = 'local'
    if len(sys.argv) == 2:
        env = sys.argv[1]

    configUtil = ConfigUtil(env)
    mysql_server_list = configUtil.get_all_mysql_server()

    business_db_meta_dbutil = DBUtil(configUtil.get_property_value('business_db_meta.host'),
                                     configUtil.get_property_value('business_db_meta.port'),
                                     configUtil.get_property_value('business_db_meta.username'),
                                     configUtil.get_property_value('business_db_meta.password'),
                                     configUtil.get_property_value('business_db_meta.db')
                                      )
    current_date = DateUtil().get_current_date()
    # 先删除当天的元数据－可以支持重跑
    delete_metadata()

    for key in mysql_server_list:
        server_config = mysql_server_list[key]
        host = server_config['host']
        port = server_config['port']
        user_name = server_config['user_name']
        password = server_config['password']
        db = 'information_schema'
        db_util = DBUtil(host, port, user_name, password, db)

        collect_database_metadata(server_config,db_util)
