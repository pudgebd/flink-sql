CREATE TABLE MyKafkaSrc01(
    channel STRING,
    name string,
    pv STRING,
    proctime as PROCTIME()
)
WITH (
 'connector' = 'kafka',
 'topic' = 'MyKafkaSrc01',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'testGroup01',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true'
);
/**
,
 'properties.security.protocol' = 'SASL_PLAINTEXT',
 'properties.sasl.mechanism' = 'GSSAPI',
 'properties.sasl.kerberos.service.name' = 'kafka',
 'properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true refreshKrb5Config=true storeKey=true serviceName=kafka keyTab="/home/work/work.keytab" principal="work@xxx.COM";'
*/

CREATE TABLE MyDimTable(
    channel STRING,
    name STRING,
    score double
 )WITH(
   'connector' = 'csv-hz',
   'hdfs.path' = '/Users/xxx/work_doc/sqls/flink_sql/debugsql/data/cq_dim_mysql.csv'
 );-- localhost  cdh601
/*
hdfs://cdh601:8020/user/work/test/cq_dim_mysql.csv
/Users/xxx/work_doc/sqls/flink_sql/debugsql/data/cq_dim_mysql.csv
*/

CREATE TABLE print_table01(
    channel STRING,
    score double
) WITH ('connector' = 'print');


insert into print_table01 select s1.channel, sum(d.score) as score from MyKafkaSrc01 s1 left join MyDimTable FOR SYSTEM_TIME AS OF s1.proctime AS d on s1.channel = d.channel 
GROUP BY s1.channel;

-- FOR SYSTEM_TIME AS OF s1.proctime AS 



