create table kafka_kerberos_test (
    id bigint,
    channel STRING,
    name string
) 
with (
 'connector' = 'kafka',
 'topic' = 'kerberos_test',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'kafka_kerberos_test_group',
 'properties.security.protocol' = 'SASL_PLAINTEXT',
 'properties.sasl.mechanism' = 'GSSAPI',
 'properties.sasl.kerberos.service.name' = 'kafka',
 'properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true refreshKrb5Config=true storeKey=true serviceName=kafka keyTab="/home/work/work.keytab" principal="work@xxx.COM";'
);
/**
{"id":1, "channel":"a", "name":"a"}
*/

-- /home/work/work.keytab
-- hdfs://cdh601:8020/streamx-dev/platform/flink_2.12-1.11.2/work.keytab
-- /Users/xxx/work_doc/kerberos/cdh601/work.keytab
/*
create table kafka_kerberos_test (
    id bigint,
    channel string,
    name string
) 
with (
 'connector' = 'datagen',
 'rows-per-second'='1',
 'fields.id.kind'='random',
 'fields.id.min'='1',
 'fields.id.max'='10',
 'fields.channel.length'='10',
 'fields.name.length'='10'
);
*/

CREATE TABLE MyDimTable(
    channel STRING,
    name STRING
 )WITH(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://192.168.1.59:3306/stream_dev?charset=utf8',
   'table-name' = 'xxx_dim_mysql',
   'username' = 'stream_dev',
   'password' = 'stream_dev'
 );
 
create table print_table (
    channel string,
    name string
) 
with (
 'connector' = 'print'
);

insert into print_table 
select channel, casT(name as stRinG) from kafka_kerberos_test;



