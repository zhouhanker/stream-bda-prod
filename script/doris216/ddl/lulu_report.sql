create database bigdata_realtime_report_v3;


drop table if exists bigdata_realtime_report_v3.report_lululemon_window_gmv_topN;
create table if not exists bigdata_realtime_report_v3.report_lululemon_window_gmv_topN (
    ds DATE NOT NULL COMMENT '分区字段，统计日期',
    window_start STRING COMMENT '窗口开始时间',
    window_end STRING COMMENT '窗口结束时间',
    win_gmv DECIMAL(18,2) COMMENT '窗口累计gmv',
    win_gmv_ids STRING COMMENT '窗口内商品id列表',
    top5_product_ids STRING COMMENT '窗口内top5商品id'
)
engine = olap
duplicate key (ds)
comment 'lululemon 日内累计 gmv 窗口聚合结果'
partition by range (ds) ()
distributed by hash(ds) buckets 10
properties (
               "storage_format" = "v2",
               "light_schema_change" = "true",
               "replication_num" = "1",
               "dynamic_partition.enable" = "true",
               "dynamic_partition.time_unit" = "day",
               "dynamic_partition.start" = "-30",
               "dynamic_partition.end" = "30",
               "dynamic_partition.prefix" = "p",
               "dynamic_partition.buckets" = "10",
               "dynamic_partition.create_history_partition" = "true",
               "dynamic_partition.history_partition_num" = "30",
               "dynamic_partition.enable_delete" = "true"
);

select *
from bigdata_realtime_report_v3.report_lululemon_window_gmv_topN
where ds = '2025-10-29'