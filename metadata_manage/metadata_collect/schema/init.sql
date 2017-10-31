CREATE DATABASE IF NOT EXISTS business_db_meta default character set utf8 COLLATE utf8_unicode_ci;
use business_db_meta;

create table IF NOT EXISTS business_column
(
	id bigint auto_increment primary key,
	db_name varchar(255) not null comment '数据库名称',
	host_name varchar(255) not null comment 'db所在的主机名称',
	port bigint not null comment '数据库对外端口号',
	table_name varchar(255) not null comment '数据表名称',
	column_name varchar(255) not null comment '数据列名称',
	data_type varchar(255) not null comment '字段的数据类型-比如varchar',
	column_type varchar(255) not null comment '字段的类型-比如varchar(64)',
	is_nullable varchar(4) null comment '是否允许为空',
	column_default varchar(255) null comment '字段默认值',
	character_set_name varchar(255) null comment '字符集名称',
	collation_name varchar(255) null comment '字段的字符校验编码集',
	column_comment varchar(2000) null comment '字段描述',
	ordinal_position int null comment '字段在表中的位置,从1开始',
	version varchar(255) not null comment '当前数据的版本,目前使用日期来表示'
)
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
comment '存储业务数据源的表的数据列信息(mysql从information_schema.columns中获取)';

create table IF NOT EXISTS business_column_log
(
	id bigint auto_increment primary key,
	log_date date not null comment '日志记录的日期',
	operate_type varchar(255) not null comment '操作的类型,add(新增了数据列),delete(删除了数据列),change_comment(修改了数据列的comment),change_data_type(修改了数据列的data_type),change_column_type(修改了数据列的column_type),change_is_nullable(修改了数据列的is_nullable),change_column_default(修改了数据列的column_default),change_character_set_name(修改了数据列的character_set_name),change_collation_name(修改了数据列的collation_name),change_column_comment(修改了数据列的column_comment),change_ordinal_position(修改了数据列的ordinal_position)',
	db_name varchar(255) not null comment '数据库名称',
	host_name varchar(255) not null comment 'db所在的主机名称',
	port bigint not null comment '数据库对外端口号',
	table_name varchar(255) not null comment '表名称',
	column_name varchar(255) not null comment '列名称',
	change_before text null comment '修改之前的内容,json格式',
	change_after text null comment '修改之后的内容,json格式'
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
comment 'column信息的变化log记录';

create table IF NOT EXISTS business_db
(
	id bigint auto_increment primary key,
	host_name varchar(255) not null comment 'db所在的主机名称',
	port bigint not null comment '数据库对外端口号',
	db_name varchar(255) not null comment '数据库名称',
	db_type varchar(255) default '1' null comment '0-mysql主库、1-mysql从库，2-mongo主库、3-mongo从库....（默认为1）',
	business_type varchar(255) null comment '业务类型（预留字段，业务的分类规则需要待确定）',
	status tinyint default '1' null comment '0-下线,1-在线',
	db_comment varchar(255) null comment '数据库的comment信息',
	default_character_set_name varchar(255) null comment '数据库默认的字符集',
	default_collation_name varchar(255) null comment '数据库默认的collation',
	version varchar(255) null comment '当前数据的版本,目前使用日期来表示',
	table_count bigint null comment '该数据库下的表的数量'
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
comment '存储业务数据源的db信息(mysql从information_schema.SCHEMATA中获取)';

create table IF NOT EXISTS business_db_log
(
	id bigint auto_increment primary key,
	log_date date not null comment '日志记录的日期',
	operate_type varchar(255) not null comment '操作的类型,add(新增了数据库),delete(删除了数据库)',
	host_name varchar(255) not null comment 'db所在的主机名称',
	port bigint not null comment '数据库对外端口号',
	db_name varchar(255) not null comment '数据库名称'
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
comment 'db信息的变化log记录';

create table IF NOT EXISTS business_table
(
	id bigint auto_increment primary key,
	db_name varchar(255) not null comment '数据库名称',
	host_name varchar(255) not null comment 'db所在的主机名称',
	port bigint not null comment '数据库对外的端口号',
	table_name varchar(255) not null comment '表名称',
	table_type varchar(255) default 'base_table' null comment '表的类型,base_table-基本表,view-视图',
	engine varchar(255) null comment '存储引擎，比如mysql可能包括myisam、innodb等',
	table_rows bigint null comment '表的总行数',
	table_cols bigint null comment '表的总列数',
	table_comment varchar(2000) null comment '表的comment信息',
	create_time varchar(255) null comment '表的创建日期',
	update_time varchar(255) null comment '表的最近更新时间',
	data_length bigint null comment '数据大小',
	table_collation varchar(255) null comment '表的字符校验编码集',
	status tinyint default '1' null comment '0-下线,1-在线',
	version varchar(255) null comment '当前数据的版本,目前使用日期来表示'
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
comment '存储业务数据源的table信息 （mysql从information_schema.tables中获取）';

create table IF NOT EXISTS business_table_log
(
	id bigint auto_increment primary key,
	log_date date not null comment '日志记录的日期',
	operate_type varchar(255) not null comment '操作的类型,add(新增了数据表),delete(删除了数据表),change_comment(修改了comment)',
	db_name varchar(255) not null comment '数据库名称',
	host_name varchar(255) not null comment 'db所在的主机名称',
	port bigint not null comment '数据库对外端口号',
	table_name varchar(255) not null,
	change_before text null comment '修改之前的内容,json格式',
	change_after text null comment '修改之后的内容,json格式'
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
comment 'table信息的变化log记录';

grant all privileges on business_db_meta.* to bi_monitor@'%' identified by 'bi_monitor';
flush privileges;

CREATE DATABASE IF NOT EXISTS hive_db_meta default character set utf8 COLLATE utf8_unicode_ci;
use hive_db_meta;

CREATE TABLE IF NOT EXISTS `hive_db` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `db_name` varchar(255) NOT NULL COMMENT '数据库名称',
  `db_comment` varchar(255) DEFAULT NULL COMMENT '数据库comment',
  `db_location_uri` varchar(255) NOT NULL COMMENT '数据库的HDFS地址',
  `owner_name` varchar(255) DEFAULT NULL COMMENT '数据库的owner',
  `owner_type` varchar(255) DEFAULT NULL COMMENT '数据库所有人的类型(user、role)',
  `db_level` varchar(255) DEFAULT NULL COMMENT '数据库的层级（ods、intg、dim、fact、dwa、app）',
  `table_count` bigint(20) NOT NULL COMMENT '表的数量,按照DB_ID从TBLS中进行查询',
  `version` varchar(255) NOT NULL COMMENT '数据的版本',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
COMMENT='存储hive数据仓库的db信息';

CREATE TABLE IF NOT EXISTS `hive_table` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `db_name` varchar(255) NOT NULL COMMENT '数据库名称',
  `table_name` varchar(255) NOT NULL COMMENT '表的名称',
  `table_type` varchar(255) NOT NULL COMMENT '表的类型：MANAGED_TABLE(外部表)、EXTERNAL_TABLE(内部表)',
  `create_time` varchar(255) NOT NULL COMMENT '表的创建日期',
  `update_time` varchar(255) NOT NULL COMMENT '表的最近更新时间',
  `owner_name` varchar(255) NOT NULL COMMENT '表的拥有者',
  `table_comment` varchar(255) DEFAULT NULL COMMENT '表的comment',
  `table_store_type` varchar(255) NOT NULL COMMENT '表的存储格式parquet、orc等',
  `table_location_uri` varchar(255) NOT NULL COMMENT '表的hdfs存储位置',
  `is_compressed` tinyint(4) NOT NULL COMMENT '是否压缩',
  `compress_format` varchar(255) NOT NULL COMMENT '压缩格式',
  `is_partioned` tinyint(4) NOT NULL COMMENT '是否分区表',
  `partion_columns` varchar(255) NOT NULL COMMENT '分区字段列表',
  `table_cols` bigint(20) NOT NULL COMMENT '字段的数量（包括分区字段）',
  `field_delimiter` varchar(255) NOT NULL COMMENT '字段分隔符',
  `num_files` bigint(20) NOT NULL COMMENT '文件的数量',
  `num_rows` bigint(20) NOT NULL COMMENT '总行数',
  `raw_data_size` bigint(20) NOT NULL,
  `total_size` bigint(20) NOT NULL,
  `version` varchar(255) NOT NULL COMMENT '数据的版本',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
COMMENT='存储hive数据仓库的table信息';

CREATE TABLE IF NOT EXISTS `hive_column` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `db_name` varchar(255) NOT NULL COMMENT '数据库名称',
  `table_name` varchar(255) NOT NULL COMMENT '表名称',
  `column_name` varchar(255) NOT NULL COMMENT '列名称',
  `column_type` varchar(255) NOT NULL COMMENT '字段类型',
  `column_comment` varchar(1000) DEFAULT NULL COMMENT '字段注释',
  `is_partition_column` tinyint(4) NOT NULL COMMENT '是否为分区字段',
  `is_index_column` tinyint(4) NOT NULL COMMENT '是否为index索引列，目前hive未使用任何的index',
  `column_index` bigint(20) NOT NULL COMMENT '列的位置信息,从0开始',
  `version` varchar(255) NOT NULL COMMENT '数据的版本',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
COMMENT='存储hive数据仓库的column信息';



grant all privileges on hive_db_meta.* to bi_monitor@'%' identified by 'bi_monitor';
flush privileges;
