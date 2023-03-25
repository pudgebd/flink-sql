CREATE CATALOG myhive WITH ('be.hive.catalog'=true, 'hive.metastore.uris'='thrift://cdh601:9083', 'database'='default', 'hive.metastore.kerberos.principal' = 'hive/cdh601@xxx.COM', 'hive.metastore.kerberos.keytab.file' = '/home/work/hive_cdh601.keytab');

/**
/home/work/work.keytab
hdfs://cdh601:8020/streamx-dev/platform/flink_2.12-1.11.2/work.keytab
*/

SET table.sql-dialect=hive;
CREATE TABLE `myhive`.`default`.MyHiveDimTable (
    channel STRING,
    name STRING
) PARTITIONED BY (dt String) STORED AS parquet TBLPROPERTIES (
  'connector'='filesystem',
  'path'='hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable',
  'format'='parquet',
  'partition.time-extractor.timestamp-pattern'='$dt',
  'sink.partition-commit.trigger'='partition-time',
  'sink.partition-commit.delay'='1 d',
  'sink.partition-commit.policy.kind'='metastore,success-file',
  'streaming-source.enable'='true',
  'streaming-source.partition-order'='create-time',
  'streaming-source.consume-start-offset'='2020-11-10',
  'lookup.join.cache.ttl'='1 min'
);

set table.sql-dialect=default;
create table print_table (
    channel string,
    name string
) 
with (
 'connector' = 'print'
);

insert into print_table 
select channel, name from `myhive`.`default`.MyHiveDimTable /*+ OPTIONS('streaming-source.monitor-interval'='30 seconds') */;



