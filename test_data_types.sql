CREATE TABLE json_source (
    id            BIGINT,
    name          STRING,
    `date`        DATE,
    obj           ROW<time1 TIME,str STRING,lg BIGINT>,
    arr           ARRAY<ROW<f1 STRING,f2 INT>>,
    `time`        TIME,
    `timestamp`   TIMESTAMP(3),
    `map`         MAP<STRING,BIGINT>,
    mapinmap      MAP<STRING,MAP<STRING,INT>>,
    deci          DECIMAL(1, 1),
    num           NUMERIC(3, 2),
    proctime      TIMESTAMP(3),
    ms            MULTISET<STRING>
) WITH (
 'connector' = 'datagen',
 'rows-per-second' = '1'
);


CREATE TABLE print_table (
    id            BIGINT,
    name          STRING,
    `date`        DATE,
    str           STRING,
    f1            STRING,
    map_val       BIGINT,
    map_val_val   INT,
    deci          DECIMAL(1, 1),
    num           NUMERIC(3, 2),
    proctime      TIMESTAMP(3),
    ms            MULTISET<STRING>
) WITH (
 'connector' = 'print'
);

insert into print_table 
select 
id, 
name,
`date`, 
obj.str, 
arr[1].f1, 
`map`['flink'], 
mapinmap['inner_map']['key'], 
deci,
num,
proctime + INTERVAL '1' SECOND,
ms
from json_source;


/*



*/
