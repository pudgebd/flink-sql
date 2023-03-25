-------------
-- hive dim

insert into MyHiveDimTable partition(dt='2020-11-16') values ('p1', 'hvie_a16'), ('p2', 'hive_b16');

insert into MyHiveDimTable partition(dt='2020-11-17') values  ('p1', 'hive_a17'), ('p2', 'hive_b17');

insert into MyHiveDimTable partition(dt='2020-11-18') values  ('p1', 'hive_a18'), ('p2', 'hive_b18');

alter table MyHiveDimTable drop partition(dt='2020-11-18');

ALTER TABLE MyHiveDimTable SET TBLPROPERTIES('streaming-source.monitor-interval' = '60 min');

truncate table MyHiveDimTable;

alter table MyHiveDimTable drop partition(province_code=2, sex='o');
drop table MyHiveDimTable;
drop table mykafkasrc01;
drop table mytestinsert;

-------------
-- stock

insert into hive_stock_account partition(dt='2020-11-16') values ('p1', 10000, 'c1'), ('p2', 10000, 'c2'), ('p3', 10000, 'c3');
insert into hive_stock_account partition(dt='2020-11-17') values ('p1', 11000, 'c1'), ('p2', 12000, 'c2'), ('p3', 13000, 'c3');

insert into hive_stock_product values ('p1', 1, 'product01'), ('p2', 2, 'product02'), ('p3', 3, 'product03');

pip install pymssql setuptools>=54.0

----------------------

CREATE TABLE TestOtherDbMyHiveDimTable (
    channel STRING,
    name STRING
) PARTITIONED BY (dt String) STORED AS parquet TBLPROPERTIES (
  'connector'='filesystem',
  'path'='hdfs://cdh601:8020/user/hive/warehouse/bigdata_test.db/testotherdbmyhivedimtable',
  'format'='parquet',
  'partition.time-extractor.timestamp-pattern'='$dt',
  'sink.partition-commit.trigger'='partition-time',
  'sink.partition-commit.delay'='1 d',
  'sink.partition-commit.policy.kind'='metastore,success-file',
  'streaming-source.enable'='true',
  'streaming-source.partition-order'='create-time',
  'streaming-source.consume-start-offset'='2020-11-10'
);

ALTER TABLE TestOtherDbMyHiveDimTable SET TBLPROPERTIES('path' = 'hdfs://cdh601:8020/user/hive/warehouse/bigdata_test.db/testotherdbmyhivedimtable');

insert into TestOtherDbMyHiveDimTable partition(dt='2020-11-15') values ('pta1', 'hive_ata1'), ('pta1', 'hive_bta1');

----------------------

    
CREATE TABLE FlinkWriteHiveTable (
    order_type bigint COMMENT '订单类型, 0:订单；1：撤单',
    acct_id string COMMENT '投资者账户代码',
    order_no bigint COMMENT '原始订单参考编号',
    sec_code string comment '产品代码',
    trade_dir string COMMENT '交易方向,B 或者 S',
    order_price bigint comment '交易价格，单位为分',
    order_vol bigint comment '含3位小数，比如数量为100股，则交易数量为二进制100000',
    act_no bigint COMMENT '订单确认顺序号',
    withdraw_order_no bigint COMMENT '撤单订单编号',
    pbu double COMMENT '发出此订单的报盘机编号',
    order_status string COMMENT '订单状态,0=New,1=Cancelled,2=Reject',
    ts bigint COMMENT '订单接收时间，微妙级时间戳',
    score double
) PARTITIONED BY (dt String, hour string, minute string) STORED AS parquet TBLPROPERTIES (
  'connector'='filesystem',
  'path'='hdfs://cdh601:8020/user/hive/warehouse/flinkwritehivetable',
  'format'='parquet',
  'partition.time-extractor.timestamp-pattern'='$dt $hour:$minute:00',
  'sink.partition-commit.trigger'='partition-time',
  'sink.partition-commit.delay'='1 minute',
  'sink.partition-commit.policy.kind'='metastore,success-file'
);



