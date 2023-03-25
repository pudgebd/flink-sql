insert into kafka_sec_code_event_counts
select sec_code, sec_code as sec_code2, count(create_user) from kafka_stock_order o left join (
  (
    select c.sec_code, d.create_user, trim(d.asd) from kafka_stock_order_confirm c
    left join kafka_stock_detail d on c.sec_code = d.sec_code
    where d.ts > '2020-02-02 11:11:11'
  )
  union
  (
    select c.sec_code, isnull(h.create_user, '') from kafka_stock_order_confirm c
    inner join kafka_stock_history h on c.sec_code = sec_code
    where h.max_price > 9999
  )
) cd
on o.sec_code = cd.sec_code group by acct_id;