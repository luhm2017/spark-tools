use lkl_card_score;

--���ȹ����ų�����
-- ����_��ǰ����������        
-- ����_��ǰ3+����            
-- ����_��ǰ30+���� 
-- ����_��ʷ����������        
-- ����_��ʷ3+����            
-- ����_��ʷ30+����
 
drop table overdue_cnt_2_2_tmp;
create table overdue_cnt_2_2_tmp as 
select c.order_src,
        'overdue0' title
       ,count(distinct c.order2) cnt 
       from 
   fqz_order_related_graph c 
 where c.type2='pass'       --ͨ�� 
   and c.current_due_day2<=0  --��ǰ
   and c.degree_type='2' 
   and c.apply_time_src>c.apply_time1  
   and c.apply_time_src>c.apply_time2 
   group by c.order_src
union all
select c.order_src,
        'overdue3' title
       ,count(distinct c.order2) cnt 
       from 
   fqz_order_related_graph c 
 where c.type2='pass'       --ͨ�� 
   and c.current_due_day2>3 --��ǰ 
   and c.degree_type='2' and c.apply_time_src>c.apply_time1 
   and c.apply_time_src>c.apply_time2    
  group by c.order_src
union all
select c.order_src,
        'overdue30' title
       ,count(distinct c.order2) cnt 
       from 
   fqz_order_related_graph c 
 where c.type2='pass'       --ͨ�� 
   and c.current_due_day2>30  --��ǰ
   and c.degree_type='2' and c.apply_time_src>c.apply_time1 
   and c.apply_time_src>c.apply_time2 
  group by c.order_src 
union all
select c.order_src,
        'overdue0_ls' title
       ,count(distinct c.order2) cnt 
       from 
   fqz_order_related_graph c 
 where c.type2='pass'       --ͨ�� 
   and c.history_due_day2<=0 --��ʷ
   and c.degree_type='2' and c.apply_time_src>c.apply_time1 
   and c.apply_time_src>c.apply_time2    
  group by c.order_src
union all
select c.order_src,
        'overdue3_ls' title
       ,count(distinct c.order2) cnt 
       from 
   fqz_order_related_graph c 
 where c.type2='pass'       --ͨ�� 
   and c.history_due_day2> 3 --��ʷ
   and c.degree_type='2' and c.apply_time_src>c.apply_time1  
   and c.apply_time_src>c.apply_time2 
  group by c.order_src
union all
select c.order_src,
        'overdue30_ls' title
       ,count(distinct c.order2) cnt 
       from 
   fqz_order_related_graph c 
 where c.type2='pass'       --ͨ�� 
   and c.history_due_day2> 30 --��ʷ
   and c.degree_type='2' and c.apply_time_src>c.apply_time1 
   and c.apply_time_src>c.apply_time2    
   group by c.order_src
   ;
   
--�ϲ����ȹ��� ��������
drop table  lkl_card_score.overdue_cnt_2_2;
create table  lkl_card_score.overdue_cnt_2_2  as
select order_src,
sum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           
sum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   
sum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  
sum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, 
sum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,
sum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls 
from  overdue_cnt_2_2_tmp
group by order_src;
