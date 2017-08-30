use lkl_card_score;

-- 2�ȹ�������
drop table overdue_cnt_1_2_instant;
create table overdue_cnt_1_2_instant as 
-- 2��_��������     
SELECT a.order_src,'order_cnt' title, 
count(distinct a.order2) cnt 
FROM fqz_order_data_inc a     --order��������
where a.degree_type='2' 
and a.apply_time_src>a.apply_time2 --��ͼ��ʱ�䶼�����ڵ�ǰ����ʱ��֮ǰ
and a.apply_time_src>a.apply_time1
group by  a.order_src    
union all 
-- 2��_ID����     
SELECT a.order_src,'id_cnt' title,
count(distinct a.cert_no2) cnt 
FROM fqz_order_data_inc a     --order��������
where a.degree_type='2' 
and a.apply_time_src>a.apply_time2 --��ͼ��ʱ�䶼�����ڵ�ǰ����ʱ��֮ǰ
and a.apply_time_src>a.apply_time1
group by a.order_src 
union all
-- 2��_�ں�ͬ����
SELECT a.order_src,'black_cnt' title,count(distinct a.order2) cnt FROM fqz_order_data_inc a     --order��������
where a.degree_type='2' 
and a.apply_time_src>a.apply_time2 --��ͼ��ʱ�䶼�����ڵ�ǰ����ʱ��֮ǰ 
and a.apply_time_src>a.apply_time1
and a.label2 = 1
group by a.order_src
-- 2��_Q��ܾ�����  2  
union all 
SELECT a.order_src,'q_refuse_cnt' title,
count(distinct a.order2) cnt 
FROM fqz_order_data_inc a     --order��������  
where performance2='q_refuse' 
and a.degree_type='2' 
and a.apply_time_src>a.apply_time2 --��ͼ��ʱ�䶼�����ڵ�ǰ����ʱ��֮ǰ 
and a.apply_time_src>a.apply_time1
group by a.order_src 
union all
-- 2��_ͨ����ͬ����  2  
SELECT a.order_src,'pass_cnt' title,
count(distinct a.order2) cnt 
FROM fqz_order_data_inc a     --order��������
where a.type2='pass'  
and a.degree_type='2' 
and a.apply_time_src>a.apply_time2 --��ͼ��ʱ�䶼�����ڵ�ǰ����ʱ��֮ǰ
and a.apply_time_src>a.apply_time1
group by a.order_src ;


--�ϲ����ȹ������� 
drop table overdue_cnt_2_sum_instant;
create table  lkl_card_score.overdue_cnt_2_sum_instant  as
select order_src,
sum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt ,           
sum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt ,   
sum(case when title= 'black_cnt' then cnt else 0 end ) black_cnt , 
sum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt ,    
sum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt 
from  overdue_cnt_1_2_instant
group by order_src;
