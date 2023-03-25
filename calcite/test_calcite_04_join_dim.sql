insert into print_table02 select s1.channel, sum(d.score) as score from MyKafkaSrc01 s1 left join MyDimTable FOR SYSTEM_TIME AS OF s1.proctime AS d on s1.channel = d.channel 
GROUP BY TUMBLE(s1.proctime, INTERVAL '1' SECONDS), s1.channel;