CREATE VIEW view_sec_code_mod AS 
select sec_code, price_change_bucket_udaf(order_type, acct_id, trade_dir, order_price, order_vol, ts_timestamp) as map,
cast(TUMBLE_START(ts_timestamp, INTERVAL '10' SECONDS) as string) as window_start,
cast(TUMBLE_END(ts_timestamp, INTERVAL '10' SECONDS) as string) as window_end
from kafka_stock_order 
group by TUMBLE(ts_timestamp, INTERVAL '10' SECONDS), sec_code, MOD(HASH_CODE(acct_id), 2048);