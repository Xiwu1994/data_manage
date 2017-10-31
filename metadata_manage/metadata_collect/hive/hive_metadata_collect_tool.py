#!/usr/bin/env python
# -*- coding:utf-8 -*-
import sys
import re
import os
pwd = os.path.realpath(__file__)
print '/'.join(pwd.split('/')[:-3])
sys.path.append('/'.join(pwd.split('/')[:-3]))

from util.db_util import DBUtil
from util.config_util import ConfigUtil
from util.date_util import DateUtil

date_util = DateUtil()
yesterday = date_util.get_yesterday()
today = date_util.get_current_date()


def collect_database_meta(hive_meta_source_db_util,hive_db_meta_util):
    query_sql = 'select * from DBS'
    dbs = hive_meta_source_db_util.query(query_sql)
    for db in dbs:
        db_row = {}
        db_name = db['NAME']
        db_id = db['DB_ID']
        db_row['db_name'] = db_name
        if not is_filtered_db(db_name):
            db_row['db_comment'] = db['DESC']
            db_row['db_location_uri'] = db['DB_LOCATION_URI']
            db_row['owner_name'] = db['OWNER_NAME']
            db_row['owner_type'] = db['OWNER_TYPE']
            db_row['db_level'] = determine_level(db_name)
            db_row['table_count'] = get_table_count_by_db_id(db_id,hive_meta_source_db_util)
            db_row['version'] = today
            hive_db_meta_util.insert_dict_into_table('hive_db',db_row)
            collect_table_meta(db_id,db_row,hive_meta_source_db_util,hive_db_meta_util)

def collect_table_meta(db_id,db_row,hive_meta_source_db_util,hive_db_meta_util):
    query_sql = 'select * from TBLS where DB_ID=' + str(db_id)
    tables = hive_meta_source_db_util.query(query_sql)

    for table in tables:
        table_row = {}
        table_id = table['TBL_ID']
        db_name = db_row['db_name']
        table_name = table['TBL_NAME']
        table_row['db_name'] = db_name
        table_row['table_name'] = table_name
        table_row['table_type'] = table['TBL_TYPE']
        table_row['create_time'] = date_util.convert_timestamp_to_date(table['CREATE_TIME'])

        table_params = get_table_params_by_table_id(table_id,hive_meta_source_db_util)

        table_row['update_time'] = date_util.convert_timestamp_to_date(long(table_params.get('transient_lastDdlTime','')))
        table_row['owner_name'] = table['OWNER']
        table_row['table_comment'] = table_params.get('comment','')
        sd_id = table['SD_ID']
        table_sds_config = get_table_sds_by_table_sd_id(sd_id,hive_meta_source_db_util)
        table_row['table_store_type'] = determine_table_store_type(table_sds_config.get('input_format', ''))
        table_row['table_location_uri'] = table_sds_config.get('location', '')

        is_compressed = table_sds_config.get('is_compressed', 0)

        table_row['is_compressed'] = is_compressed
        table_row['compress_format'] = ''

        table_partition_keys = get_table_partitions_by_table_id(table_id,hive_meta_source_db_util)
        if len(table_partition_keys) == 0:#根据是否包含分区字段来判断是否为分区表
            table_row['is_partioned'] = 0
        else:
            table_row['is_partioned'] = 1
        partion_columns = []
        for table_partition_key in table_partition_keys:
            pkey_name = table_partition_key['PKEY_NAME']
            partion_columns.append(pkey_name)

        table_row['partion_columns'] = ','.join(partion_columns)

        table_columns = get_table_columns_by_table_cd_id(table_sds_config.get('cd_id', ''),hive_meta_source_db_util)
        table_cols = len(table_columns) + len(partion_columns)
        table_row['table_cols'] = table_cols

        # 从serde_params中取出field.delim 字段分隔符
        serde_id = table_sds_config.get('serde_id', '')
        table_serde_params = get_table_serde_params_by_serde_id(serde_id,hive_meta_source_db_util)

        field_delimiter = table_serde_params.get('field.delim','')
        if field_delimiter == '':
            field_delimiter = '\\001'
        table_row['field_delimiter'] = field_delimiter


        table_row['num_files'] = table_params.get('numFiles', 0)
        table_row['num_rows'] = table_params.get('numRows', 0)
        table_row['raw_data_size'] = table_params.get('rawDataSize', 0)
        table_row['total_size'] = table_params.get('totalSize', 0)

        table_row['version'] = today

        hive_db_meta_util.insert_dict_into_table('hive_table', table_row)

        insert_common_columns(db_name,table_name,table_columns,hive_db_meta_util)
        insert_partition_columns(db_name,table_name,table_partition_keys,len(table_columns),hive_db_meta_util)

def insert_partition_columns(db_name,table_name,table_partition_keys,cols,hive_db_meta_util):
    for table_partition_key in table_partition_keys:
        partition_column_row = {}
        partition_column_row['db_name'] = db_name
        partition_column_row['table_name'] = table_name
        partition_column_row['column_name'] = table_partition_key['PKEY_NAME']
        partition_column_row['column_type'] = table_partition_key['PKEY_TYPE']
        partition_column_row['column_comment'] = table_partition_key['PKEY_COMMENT']
        partition_column_row['is_partition_column'] = 1
        partition_column_row['is_index_column'] = 0
        partition_column_row['column_index'] = cols + int(table_partition_key['INTEGER_IDX'])
        partition_column_row['version'] = today

        hive_db_meta_util.insert_dict_into_table('hive_column',partition_column_row)

def insert_common_columns(db_name,table_name,table_columns,hive_db_meta_util):
    for table_column in table_columns:
        table_column_row = {}
        table_column_row['db_name'] = db_name
        table_column_row['table_name'] = table_name
        table_column_row['column_name'] = table_column['COLUMN_NAME']
        table_column_row['column_type'] = table_column['TYPE_NAME']
        table_column_row['column_comment'] = table_column['COMMENT']
        table_column_row['is_partition_column'] = 0
        table_column_row['is_index_column'] = 0
        table_column_row['column_index'] = table_column['INTEGER_IDX']
        table_column_row['version'] = today

        hive_db_meta_util.insert_dict_into_table('hive_column', table_column_row)






def get_table_params_by_table_id(table_id,hive_meta_source_db_util):
    query_sql = 'select * from TABLE_PARAMS where TBL_ID=' + str(table_id)
    tmp_table_params = hive_meta_source_db_util.query(query_sql)
    table_params = {}
    for table_param in tmp_table_params:
        param_key = table_param['PARAM_KEY']
        param_value = table_param['PARAM_VALUE']
        table_params[param_key] = param_value
    return  table_params

def get_table_sds_by_table_sd_id(sd_id,hive_meta_source_db_util):
    query_sql = 'select * from SDS where SD_ID=' + str(sd_id)
    table_sds_rows = hive_meta_source_db_util.query(query_sql)
    table_sds_config = {}
    for table_sds in table_sds_rows:
        table_sds_config['cd_id'] = table_sds['CD_ID']
        table_sds_config['input_format'] = table_sds['INPUT_FORMAT']
        is_compressed = 0
        if table_sds['IS_COMPRESSED'] == '\x00':
            is_compressed = 0
        else:
            is_compressed = 1
        table_sds_config['is_compressed'] = is_compressed
        table_sds_config['location'] = table_sds['LOCATION']
        table_sds_config['serde_id'] = table_sds['SERDE_ID']

    return table_sds_config

# 取得某个表的所有分区column
def get_table_partitions_by_table_id(table_id,hive_meta_source_db_util):
    query_sql = 'select * from PARTITION_KEYS where TBL_ID=' + str(table_id)
    table_partition_keys = hive_meta_source_db_util.query(query_sql)
    return table_partition_keys

# 取得某个表的所有非分区column
def get_table_columns_by_table_cd_id(cd_id,hive_meta_source_db_util):
    query_sql = 'select * from COLUMNS_V2 where CD_ID=' + str(cd_id)
    table_columns = hive_meta_source_db_util.query(query_sql)
    return table_columns


def get_table_serde_params_by_serde_id(serde_id,hive_meta_source_db_util):
    query_sql = 'select * from SERDE_PARAMS where SERDE_ID=' + str(serde_id)
    tmp_table_serde_params = hive_meta_source_db_util.query(query_sql)
    table_serde_params = {}
    for table_serde_param in table_serde_params:
        param_key = table_serde_param['PARAM_KEY']
        param_value = table_serde_param['PARAM_VALUE']
        table_serde_params[param_key] = param_value

    return table_serde_params




def is_filtered_db(db_name):
    filtered_dbs = ['ods','app','default','dict','dim','dwa','fact','liusc_test','zx_test','patch_history']
    if db_name in filtered_dbs:
        return True
    elif db_name.endswith('_test'):
        return True
    else:
        return False

def determine_level(object_name):
    if object_name == 'intg':
        return "intg"
    elif object_name == 'odps':
        return "odps"
    elif object_name == 'tmp':
        return "tmp"
    elif object_name == 'email':
        return "email"
    elif '_' in object_name:
        level = object_name.split('_')[0]
        return level
    else:
        print object_name
        return 'None'

def determine_table_store_type(input_format):
    if input_format == 'org.apache.hadoop.mapred.TextInputFormat':
        return 'text'
    elif input_format == 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat':
        return 'parquet'
    elif input_format == 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat':
        return 'orc'
    else:
        print input_format
        return input_format

def get_table_count_by_db_id(db_id,db_util):
    query_sql = "select count(*) as table_count from TBLS where db_id=" + str(db_id)
    rows = db_util.query(query_sql)
    if len(rows)==1:
        return rows[0]['table_count']
    else:
        return 0

def delete_metadata(verion):
    delete_metadata_by_table('hive_db',verion)
    delete_metadata_by_table('hive_table', verion)
    delete_metadata_by_table('hive_column', verion)

def delete_metadata_by_table(table_name,version):
    delete_sql = 'delete from ' +table_name +' where version="' + version +'"'
    hive_db_meta_util.delete(delete_sql)


if __name__ == '__main__':

    env = 'local'
    if len(sys.argv) == 2:
        env = sys.argv[1]

    configUtil = ConfigUtil(env)

    hive_meta_source_db_util = DBUtil(configUtil.get_property_value('metadata.hive.server.host'),
                                     configUtil.get_property_value('metadata.hive.server.port'),
                                     configUtil.get_property_value('metadata.hive.server.user_name'),
                                     configUtil.get_property_value('metadata.hive.server.password'),
                                     configUtil.get_property_value('metadata.hive.server.db')
                                      )

    hive_db_meta_util = DBUtil(configUtil.get_property_value('hive_db_meta.host'),
                                      configUtil.get_property_value('hive_db_meta.port'),
                                      configUtil.get_property_value('hive_db_meta.username'),
                                      configUtil.get_property_value('hive_db_meta.password'),
                                      configUtil.get_property_value('hive_db_meta.db')
                                      )

    # 先删除当天的元数据－可以支持重跑
    delete_metadata(today)
    collect_database_meta(hive_meta_source_db_util,hive_db_meta_util)

    # 删除昨天的元数据－避免数据量太大
    delete_metadata(yesterday)
