use lkl_card_score;

set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks= 200;

--所有合同表现，以creditloan.r_overdue_period 为依据    历史逾期天数、当前逾期天数
--================================================================================
insert overwrite table fqz_contract_performance_data
select ta.order_id,
ta.apply_time,
nvl(ta.performance,0) as history_due_day,
nvl(tb.performance,0) as current_due_day 
from (
--所有订单历史逾期天数
select 
a.order_id,
a.apply_time,
max(cast(a.stat_count as int)) as performance --最大天数
from 
creditloan.r_overdue_period a
where a.year = ${year} and a.month = ${month} and a.day = ${day}
and a.stat_type = 0  -- 0 表示时间
and a.due_time < from_unixtime(unix_timestamp(),'yyyy-MM-dd') --未超过还款截止时间
group by a.order_id,a.apply_time
) ta left join (
--所有订单当前逾期天数
select 
a.order_id,
a.apply_time,
max(cast(a.stat_count as int)) as performance --最大天数
from 
creditloan.r_overdue_period a
where a.year = ${year} and a.month = ${month} and a.day = ${day}
and a.stat_type = 0 -- 0 表示时间
and a.due_time < from_unixtime(unix_timestamp(),'yyyy-MM-dd') --当前日期超过还款截止时间
and a.overdue_type = 0  --未还清
group by a.order_id,a.apply_time) tb
on ta.order_id = tb.order_id;
