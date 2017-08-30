use lkl_card_score;

set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks= 200;

--���к�ͬ���֣���creditloan.r_overdue_period Ϊ����    ��ʷ������������ǰ��������
--================================================================================
insert overwrite table fqz_contract_performance_data
select ta.order_id,
ta.apply_time,
nvl(ta.performance,0) as history_due_day,
nvl(tb.performance,0) as current_due_day 
from (
--���ж�����ʷ��������
select 
a.order_id,
a.apply_time,
max(cast(a.stat_count as int)) as performance --�������
from 
creditloan.r_overdue_period a
where a.year = ${year} and a.month = ${month} and a.day = ${day}
and a.stat_type = 0  -- 0 ��ʾʱ��
and a.due_time < from_unixtime(unix_timestamp(),'yyyy-MM-dd') --δ���������ֹʱ��
group by a.order_id,a.apply_time
) ta left join (
--���ж�����ǰ��������
select 
a.order_id,
a.apply_time,
max(cast(a.stat_count as int)) as performance --�������
from 
creditloan.r_overdue_period a
where a.year = ${year} and a.month = ${month} and a.day = ${day}
and a.stat_type = 0 -- 0 ��ʾʱ��
and a.due_time < from_unixtime(unix_timestamp(),'yyyy-MM-dd') --��ǰ���ڳ��������ֹʱ��
and a.overdue_type = 0  --δ����
group by a.order_id,a.apply_time) tb
on ta.order_id = tb.order_id;
