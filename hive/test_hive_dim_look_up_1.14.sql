CREATE TABLE datagen_source (
    id INT,
    date_col date,
    str_col string,
    proctime as proctime()
) WITH (
 'connector' = 'datagen',
 'rows-per-second' = '1',
 'fields.id.min' = '1',
 'fields.id.max' = '3',
 'fields.str_col.length'='1'
);


CREATE TABLE print_table(
    str_col string,
    b string
) WITH ('connector' = 'print');


CREATE CATALOG hive_catalog WITH (
  'type'='hive',
  'hive-conf-dir'='/Users/xxx/work_doc/cluster/dev'
);


insert into print_table select ds.str_col, t.d from datagen_source ds 
left join `hive_catalog`.`default`.`t2` 
/*+ OPTIONS('streaming-source.enable' = 'true',
  'streaming-source.partition.include' = 'latest',
  'streaming-source.monitor-interval' = '3 s',
  'streaming-source.partition-order' = 'partition-name') */
FOR SYSTEM_TIME AS OF ds.proctime AS t on t.b = ds.str_col;
