create database bigdata_realtime_report_v3;


truncate table bigdata_realtime_report_v3.report_lululemon_day_log_device_info;
CREATE TABLE IF NOT EXISTS bigdata_realtime_report_v3.report_lululemon_day_log_device_info (
    pt DATE NOT NULL COMMENT '分区日期',
    os VARCHAR(32) NOT NULL COMMENT '系统平台',
    brand VARCHAR(64) NOT NULL COMMENT '品牌',
    platv VARCHAR(64) NOT NULL COMMENT '系统版本',
    count BIGINT COMMENT '最新统计值',
    type VARCHAR(32) COMMENT '统计类型',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
)
UNIQUE KEY(pt, os, brand, platv)
PARTITION BY RANGE(pt)()
DISTRIBUTED BY HASH(pt, os, brand) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "enable_unique_key_merge_on_write" = "true",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "30",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8",
    "dynamic_partition.create_history_partition" = "true"
);


truncate table bigdata_realtime_report_v3.report_lululemon_day_log_region_info;
drop table if exists bigdata_realtime_report_v3.report_lululemon_day_log_region_info;
CREATE TABLE IF NOT EXISTS bigdata_realtime_report_v3.report_lululemon_day_log_region_info (
    pt DATE NOT NULL COMMENT '分区日期',
    region VARCHAR(128) NOT NULL COMMENT '省市区组合（省|市|运营商）',
    province VARCHAR(64) COMMENT '省份',
    city VARCHAR(64) COMMENT '城市',
    district VARCHAR(64) COMMENT '区县/运营商',
    count BIGINT COMMENT '数量',
    abnormal BOOLEAN COMMENT '是否异常',
    region_raw VARCHAR(256) COMMENT '原始区域字段',
    type VARCHAR(32) COMMENT '类型',
    ip VARCHAR(64) COMMENT 'IP地址',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
)
UNIQUE KEY(pt, region)
PARTITION BY RANGE(pt) (
    PARTITION p_init VALUES LESS THAN ("2020-01-01")    -- 模板分区，动态分区表必须存在
)
DISTRIBUTED BY HASH(pt, region) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "enable_unique_key_merge_on_write" = "true",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "30",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8",
    "dynamic_partition.create_history_partition" = "true"
);

select *
from bigdata_realtime_report_v3.report_lululemon_day_log_region_info;


truncate table bigdata_realtime_report_v3.report_lululemon_day_log_search_info;
CREATE TABLE IF NOT EXISTS bigdata_realtime_report_v3.report_lululemon_day_log_search_info (
    pt DATE NOT NULL COMMENT '分区日期',
    search_item JSON COMMENT '搜索项明细',
    emit_time DATETIME COMMENT 'flk数据产生时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
)
DUPLICATE KEY(pt)
PARTITION BY RANGE(pt) (
    PARTITION p_init VALUES LESS THAN ("1970-01-01")   -- 模板分区
)
DISTRIBUTED BY HASH(pt) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "30",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8",
    "dynamic_partition.create_history_partition" = "true",
    "storage_format" = "V2"
);


CREATE TABLE IF NOT EXISTS bigdata_realtime_report_v3.report_lululemon_day_log_page_info (
    pt DATE NOT NULL COMMENT '分区日期',
    page string COMMENT '搜索项明细',
    pv bigint COMMENT 'flk数据产生时间',
    user_ids json,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
)
DUPLICATE KEY(pt)
PARTITION BY RANGE(pt) (
    PARTITION p_init VALUES LESS THAN ("1970-01-01")   -- 模板分区
)
DISTRIBUTED BY HASH(pt) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "30",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8",
    "dynamic_partition.create_history_partition" = "true",
    "storage_format" = "V2"
);


truncate table bigdata_realtime_report_v3.report_lululemon_day_log_search_info;
select *
from bigdata_realtime_report_v3.report_lululemon_day_log_search_info;

truncate table bigdata_realtime_report_v3.report_lululemon_day_log_device_info;
select *
from bigdata_realtime_report_v3.report_lululemon_day_log_device_info;

truncate table bigdata_realtime_report_v3.report_lululemon_day_log_region_info;
select *
from bigdata_realtime_report_v3.report_lululemon_day_log_region_info;


truncate table bigdata_realtime_report_v3.report_lululemon_day_log_page_info;
select *
from bigdata_realtime_report_v3.report_lululemon_day_log_page_info;