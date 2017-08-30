use lkl_card_score;

set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks= 200;

--���ж����������� �������к�ͬ�Լ��ܾ��Ľ����� �������֤�š��Ƿ�ں�ͬ
--================================================================================
insert overwrite table fqz_order_performance_data_new
select f.*,u.cert_no,
(case when fb.orderno is not null then 1   -- �ں�ͬ
when  (fb.orderno is null and f.type = 'pass' 
and nvl(f.history_due_day,0) <= 0 and nvl(f.current_due_day,0) <= 0 )then 0  -- û�����ڵĺ�ͬ 0
else 2 end) as label --������Ϊ 2
from fqz_order_performance_data f 
join creditloan.s_c_loan_apply  a on f.order_id = a.order_id
left join fqz_black_contract fb on f.order_id = fb.orderno
join creditloan.s_c_apply_user u on u.id = a.id
where a.year = ${year} and a.month = ${month} and a.day = ${day}
and u.year = ${year} and u.month = ${month} and u.day = ${day};
