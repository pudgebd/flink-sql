CREATE TABLE kafka_source (
 first STRING,
 last STRING,
 user_id int,
 from_func as cast(first as string),
 proctime as proctime()
) WITH (
 'connector' = 'kafka',
 'topic' = 'test_kafka_join_mysql',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'testGroup02',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true',
 'properties.security.protocol' = 'SASL_PLAINTEXT',
 'properties.sasl.mechanism' = 'GSSAPI',
 'properties.sasl.kerberos.service.name' = 'kafka',
 'properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true refreshKrb5Config=true storeKey=true serviceName=kafka keyTab="/home/work/work.keytab" principal="work@xxx.COM";'
);


CREATE TABLE dim_mysql (
  user_id int,
  score double
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/test',
   'table-name' = 'dim_mysql',
   'username' = 'root',
   'password' = '123456'
);


CREATE TABLE insert_test (
  first STRING,
  last STRING,
  score double
) WITH ('connector' = 'print');


CREATE TABLE test_write_kafka (
  first STRING,
  last STRING,
  score double
) WITH (
 'connector' = 'kafka',
 'topic' = 'test_write_kafka',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'testGroup03',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true',
 'properties.security.protocol' = 'SASL_PLAINTEXT',
 'properties.sasl.mechanism' = 'GSSAPI',
 'properties.sasl.kerberos.service.name' = 'kafka',
 'properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true refreshKrb5Config=true storeKey=true serviceName=kafka keyTab="/home/work/work.keytab" principal="work@xxx.COM";'
);

/*
insert into insert_test 
select ks.first, ks.last, dm.score from kafka_source ks left join dim_mysql dm on ks.user_id = dm.user_id;
*/

insert into test_write_kafka 
select last_value(ks.first), last_value(ks.last), last_value(dm.score) from 
kafka_source ks left join dim_mysql dm on ks.user_id = dm.user_id;