db.py 和 sql.py
都有接口 /adv_query  用到了 engine.query_adv

query.py
的explain、gen_sql

SELECT first(channel, 0) AS first_1, last(name, 0) AS last_1 
FROM "MyHiveDimTable" GROUP BY dt

/*+engine=MPP*/ 
SELECT first(channel, 0) AS first_1, last(name, 0) AS last_1 
FROM `MyHiveDimTable` GROUP BY dt


insert into `myhive`.FlinkWriteHiveTable select s1.order_type, s1.acct_id, s1.order_no, s1.sec_code, s1.trade_dir, s1.order_price, s1.order_vol, s1.act_no, s1.withdraw_order_no, s1.pbu, s1.order_status, s1.ts, s1.order_vol as score, DATE_FORMAT(ts_timestamp, 'yyyy-MM-dd') as dt, DATE_FORMAT(ts_timestamp, 'HH') as hour_parti, DATE_FORMAT(ts_timestamp, 'mm') as minute_parti from MyKafkaSrc01 s1;


SELECT `WM_CONCAT`(`T`.`COLUMN_NAME`)
FROM `USER_TAB_COLUMNS` AS `T`
WHERE `T`.`TABLE_NAME` = 'FBS_DATAOBJECT'


SELECT `wm_concat`(`t`.`column_name`)
FROM `user_tab_columns` AS `t`
WHERE `t`.`table_name` = 'FBS_DATAOBJECT'


SELECT `wm_concat`(`t`.`column_name`)
FROM `USER_TAB_COLUMNS` AS `t`
WHERE `t`.`table_name` = 'FBS_DATAOBJECT'