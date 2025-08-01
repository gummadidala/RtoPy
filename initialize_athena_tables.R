statements <- c(
    "CREATE EXTERNAL TABLE IF NOT EXISTS `adjusted_counts_15min`(
        `signalid` string,
        `callphase` string,
        `detector` string,
        `timeperiod` timestamp,
        `vol` int)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/adjusted_counts_15min'",
    "MSCK REPAIR TABLE adjusted_counts_15min",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `adjusted_counts_1hr`(
        `signalid` string,
        `callphase` string,
        `detector` string,
        `timeperiod` timestamp,
        `vol` int)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/adjusted_counts_1hr'",
    "MSCK REPAIR TABLE adjusted_counts_1hr",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `arrivals_on_green`(
        `signalid` string,
        `callphase` string,
        `date_hour` timestamp,
        `dow` int,
        `week` int,
        `aog` double,
        `pr` double,
        `vol` int)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/arrivals_on_green'",
    "MSCK REPAIR TABLE arrivals_on_green",



    "CREATE EXTERNAL TABLE IF NOT EXISTS `bad_detectors`(
        `signalid` string,
        `detector` string,
        `good_day` int)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/bad_detectors'",
    "MSCK REPAIR TABLE bad_detectors",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `bad_ped_detectors`(
        `signalid` string,
        `detector` string)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/bad_ped_detectors'",
    "MSCK REPAIR TABLE bad_ped_detectors",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `cctv_uptime`(
        `cameraid` string,
        `date` date,
        `size` double,
        `__index_level_0__` bigint)
    PARTITIONED BY (
        `month` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/cctv_uptime'",
    "MSCK REPAIR TABLE cctv_uptime",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `cctv_uptime_encoders`(
        `cameraid` string,
        `date` date,
        `size` double)
    PARTITIONED BY (
        `month` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/cctv_uptime_encoders'",
    "MSCK REPAIR TABLE cctv_uptime_encoders",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `cctvlogs`(
        `accept-ranges` string,
        `age` bigint,
        `cache-control` string,
        `connection` string,
        `content-length` bigint,
        `content-type` string,
        `datetime` timestamp,
        `id` string,
        `last-modified` string,
        `proxy-connection` string,
        `server` string,
        `via` string,
        `x-cache` string,
        `x-cache-hits` bigint,
        `x-response` bigint,
        `x-response-time` string,
        `x-served-by` string,
        `x-timer` string,
        `x-varnish-action` string,
        `__index_level_0__` bigint,
        `fastly-restarts` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/cctvlogs'",
    "MSCK REPAIR TABLE cctvlogs",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `cctvlogs_encoder`(
        `connection type` bigint,
        `cross street` string,
        `datetime` string,
        `dimensions` string,
        `host/ip` string,
        `include` boolean,
        `location id` string,
        `manufacturer` string,
        `response` string,
        `roadway name` string,
        `size` bigint,
        `title` string,
        `__index_level_0__` bigint)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/cctvlogs_encoder/'",
    "MSCK REPAIR TABLE ",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `cctvlogs_summary`(
        `cameraid` string,
        `date` date,
        `size` double,
        `__index_level_0__` bigint)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/cctvlogs_summary/'",
    "MSCK REPAIR TABLE ",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `comm_uptime`(
        `signalid` string,
        `callphase` string,
        `date_hour` timestamp,
        `dow` int,
        `week` int,
        `uptime` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/comm_uptime'",
    "MSCK REPAIR TABLE comm_uptime",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `cor_travel_time_metrics_1hr`(
        `corridor` string,
        `hour` timestamp,
        `tti` double,
        `pti` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/cor_travel_time_metrics_1hr'",
    "MSCK REPAIR TABLE cor_travel_time_metrics_1hr",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `cor_travel_times_1hr`(
        `corridor` string,
        `tmc_code` string,
        `miles` double,
        `hour` timestamp,
        `speed` double,
        `reference_speed` double,
        `travel_time_minutes` double,
        `confidence_score` double,
        `reference_minutes` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/cor_travel_times_1hr'",
    "MSCK REPAIR TABLE cor_travel_times_1hr",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `counts_15min`(
        `signalid` string,
        `timeperiod` timestamp,
        `detector` string,
        `callphase` string,
        `vol` int)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/counts_15min'",
    "MSCK REPAIR TABLE counts_15min",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `counts_1hr`(
        `signalid` string,
        `timeperiod` timestamp,
        `detector` string,
        `callphase` string,
        `vol` int)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/counts_1hr'",
    "MSCK REPAIR TABLE counts_1hr",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `counts_ped_15min`(
        `signalid` string,
        `timeperiod` timestamp,
        `detector` string,
        `callphase` string,
        `vol` int)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/counts_ped_15min'",
    "MSCK REPAIR TABLE counts_ped_15min",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `counts_ped_1hr`(
        `signalid` string,
        `timeperiod` timestamp,
        `detector` string,
        `callphase` string,
        `vol` int)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/counts_ped_1hr'",
    "MSCK REPAIR TABLE counts_ped_1hr",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `cycledata`(
        `signalid` int,
        `phase` int,
        `cyclestart` timestamp,
        `phasestart` timestamp,
        `phaseend` timestamp,
        `eventcode` int,
        `termtype` int,
        `duration` double,
        `volume` int)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/cycles'",
    "MSCK REPAIR TABLE cycledata",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `detectionevents`(
        `signalid` int,
        `phase` int,
        `detector` int,
        `cyclestart` timestamp,
        `phasestart` timestamp,
        `eventcode` int,
        `dettimestamp` timestamp,
        `detduration` double,
        `dettimeincycle` double,
        `dettimeinphase` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/detections'",
    "MSCK REPAIR TABLE detectionevents",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `detector_uptime_pd`(
        `signalid` string,
        `callphase` string,
        `date_hour` timestamp,
        `setback` string,
        `uptime` double,
        `all` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/detector_uptime_pd'",
    "MSCK REPAIR TABLE detector_uptime_pd",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `filtered_counts_15min`(
        `signalid` string,
        `timeperiod` timestamp,
        `detector` string,
        `callphase` string,
        `vol` double,
        `delta_vol` double,
        `good` double,
        `mean_abs_delta` double,
        `good_day` int,
        `month_hour` timestamp,
        `hour` timestamp)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/filtered_counts_1hr'",
    "MSCK REPAIR TABLE filtered_counts_1hr",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `filtered_counts_1hr`(
        `signalid` string,
        `timeperiod` timestamp,
        `detector` string,
        `callphase` string,
        `vol` double,
        `delta_vol` double,
        `good` double,
        `mean_abs_delta` int,
        `good_day` int,
        `month_hour` timestamp,
        `hour` timestamp)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/filtered_counts_1hr'",
    "MSCK REPAIR TABLE filtered_counts_1hr",


    "CREATE EXTERNAL TABLE IF NOT EXISTS `ped_actuations_15min`(
        `signalid` string,
        `week` int,
        `dow` int,
        `timeperiod` timestamp,
        `pa15` int)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/ped_actuations_15min'",
    "MSCK REPAIR TABLE ped_actuations_15min",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `ped_actuations_pd`(
        `signalid` string,
        `callphase` string,
        `week` int,
        `dow` int,
        `papd` int)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/ped_actuations_pd'",
    "MSCK REPAIR TABLE ped_actuations_pd",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `ped_actuations_ph`(
        `signalid` string,
        `callphase` string,
        `week` int,
        `dow` int,
        `paph` int)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/ped_actuations_ph'",
    "MSCK REPAIR TABLE ped_actuations_ph",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `ped_delay`(
        `signalid` string,
        `eventparam` bigint,
        `ped_call` timestamp,
        `begin_walk` timestamp,
        `duration` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/ped_delay'",
    "MSCK REPAIR TABLE ped_delay",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `ped_detector_uptime_pd`(
        `signalid` string,
        `callphase` string,
        `dow` int,
        `week` int,
        `probbad` double,
        `uptime` double,
        `all` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/ped_detector_uptime_pd'",
    "MSCK REPAIR TABLE ped_detector_uptime_pd",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `queue_spillback`(
        `signalid` string,
        `callphase` string,
        `date_hour` timestamp,
        `dow` int,
        `week` int,
        `qs` int,
        `cycles` int,
        `qs_freq` double,
        `timefromstopbar` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/queue_spillback'",
    "MSCK REPAIR TABLE queue_spillback",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `queue_spillback_15min`(
        `signalid` string,
        `callphase` string,
        `date_hour` timestamp,
        `dow` int,
        `week` int,
        `qs` int,
        `cycles` int,
        `qs_freq` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/queue_spillback_15min'",
    "MSCK REPAIR TABLE queue_spillback_15min",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `rsu_uptime`(
        `signalid` int,
        `date` date,
        `uptime` double,
        `count` int)
    PARTITIONED BY (
        `month` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/rsu_uptime'",
    "MSCK REPAIR TABLE rsu_uptime",


    "CREATE EXTERNAL TABLE IF NOT EXISTS `signal_details`(
        `signalid` int,
        `data` array<struct<hour:int,detector:int,callphase:int,vol_rc:int,vol_ac:int,bad_day:boolean>>)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
        's3://{bucket}/mark/signal_details'",
    "MSCK REPAIR TABLE signal_details",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `split_failures`(
        `signalid` string,
        `callphase` string,
        `date_hour` timestamp,
        `dow` int,
        `week` int,
        `sf` int,
        `cycles` int,
        `sf_freq` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/split_failures'",
    "MSCK REPAIR TABLE split_failures",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `split_failures_15min`(
        `signalid` string,
        `callphase` string,
        `date_hour` timestamp,
        `dow` int,
        `week` int,
        `sf` int,
        `cycles` int,
        `sf_freq` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/split_failures_15min'",
    "MSCK REPAIR TABLE split_failures_15min",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `sub_travel_time_metrics`(
        `corridor` string,
        `subcorridor` string,
        `hour` timestamp,
        `tti` double,
        `pti` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/sub_travel_time_metrics'",
    "MSCK REPAIR TABLE sub_travel_time_metrics",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `sub_travel_time_metrics_1hr`(
        `corridor` string,
        `subcorridor` string,
        `hour` timestamp,
        `tti` double,
        `pti` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/sub_travel_time_metrics_1hr'",
    "MSCK REPAIR TABLE sub_travel_time_metrics_1hr",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `sub_travel_times`(
        `corridor` string,
        `subcorridor` string,
        `tmc_code` string,
        `miles` double,
        `hour` timestamp,
        `speed` double,
        `reference_speed` double,
        `travel_time_minutes` double,
        `confidence_score` double,
        `reference_minutes` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/sub_travel_times'",
    "MSCK REPAIR TABLE sub_travel_times",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `sub_travel_times_1hr`(
        `corridor` string,
        `subcorridor` string,
        `tmc_code` string,
        `miles` double,
        `hour` timestamp,
        `speed` double,
        `reference_speed` double,
        `travel_time_minutes` double,
        `confidence_score` double,
        `reference_minutes` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/sub_travel_times_1hr'",
    "MSCK REPAIR TABLE sub_travel_times_1hr",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `throughput`(
        `signalid` string,
        `callphase` string,
        `week` int,
        `dow` int,
        `vph` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/throughput'",
    "MSCK REPAIR TABLE throughput",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `travel_time_metrics`(
        `corridor` string,
        `hour` timestamp,
        `tti` double,
        `pti` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/travel_time_metrics'",
    "MSCK REPAIR TABLE travel_time_metrics",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `travel_times`(
        `tmc_code` string,
        `hour` timestamp,
        `speed` double,
        `reference_speed` double,
        `travel_time_minutes` double,
        `confidence_score` double,
        `miles` double,
        `corridor` string,
        `reference_minutes` double)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/travel_times'",
    "MSCK REPAIR TABLE travel_times",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `vehicles_15min`(
        `signalid` string,
        `week` int,
        `dow` int,
        `timeperiod` timestamp,
        `vph` int)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/vehicles_15min'",
    "MSCK REPAIR TABLE vehicles_15min",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `vehicles_pd`(
        `signalid` string,
        `callphase` string,
        `week` int,
        `dow` int,
        `vpd` int)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoohive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/vehicles_pd'",
    "MSCK REPAIR TABLE vehicles_pd",

    "CREATE EXTERNAL TABLE IF NOT EXISTS `vehicles_ph`(
        `signalid` string,
        `week` int,
        `dow` int,
        `hour` timestamp,
        `vph` int)
    PARTITIONED BY (
        `date` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://{bucket}/mark/vehicles_ph'",
    "MSCK REPAIR TABLE vehicles_ph"
)


source("Monthly_Report_Calcs_init.R")

athena <- get_athena_connection()
bucket <- conf$bucket

for (stmt in statements) {
    print(stmt)
    dbExecute(athena, glue(stmt))
    print("---")
}

