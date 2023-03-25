CREATE TABLE MyDimTable(
    channel STRING,
    name STRING
 )WITH(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://192.168.1.59:3306/stream_dev?charset=utf8',
   'table-name' = 'cq_dim_mysql',
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



Tests in error: 
  retainRetractFlag03(com.xxx.streamx.app.test.sqlparser.TestValidateLogic)
  join01(com.xxx.streamx.app.test.sqlparser.TestValidateLogic)
  joinUnion01(com.xxx.streamx.app.test.sqlparser.TestValidateLogic)
  joinUnion02(com.xxx.streamx.app.test.sqlparser.TestValidateLogic)