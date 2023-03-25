CREATE TABLE UserScores (name STRING, score INT)
WITH (
  'connector' = 'socket',
  'hostname' = 'localhost',
  'port' = '9999',
  'byte-delimiter' = '10',
  'format' = 'changelog-csv',
  'changelog-csv.column-delimiter' = '|'
);


CREATE TABLE print_table (name STRING, score INT) 
WITH (
  'connector' = 'print'
);

insert into print_table 
select * from UserScores;

/*

未增加spi配置
http://cdh601:19888/jobhistory/logs/cdh602:8041/container_1618286011713_0482_01_000001/container_1618286011713_0482_01_000001/work/jobmanager.out/?start=0



*/