use lkl_card_score;


--一度关联自身
-- 一度_当前无逾期数量        
-- 一度_当前3+数量            
-- 一度_当前30+数量 
-- 一度_历史无逾期数量        
-- 一度_历史3+数量            
-- 一度_历史30+数量            
drop table overdue_cnt_2_self_tmp_instant;
create table overdue_cnt_2_self_tmp_instant as 
select c.order_src,
        'overdue0' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_data_inc c 
 where c.type1='pass'       --通过 
   and c.current_due_day1<=0  --当前
   and c.degree_type='1' and c.apply_time_src>c.apply_time1
   --一度关联自身
   and c.cert_no_src = c.cert_no1
   group by c.order_src
union all
select c.order_src,
        'overdue3' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_data_inc c 
 where c.type1='pass'       --通过 
   and c.current_due_day1>3  --当前
   and c.degree_type='1' and c.apply_time_src>c.apply_time1  
   --一度关联自身
   and c.cert_no_src = c.cert_no1
  group by c.order_src
union all
select c.order_src,
        'overdue30' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_data_inc c 
 where c.type1='pass'       --通过 
   and c.current_due_day1>30 --当前
   and c.degree_type='1' and c.apply_time_src>c.apply_time1
   --一度关联自身
   and c.cert_no_src = c.cert_no1
  group by c.order_src 
union all
--历史逾期
select c.order_src,
        'overdue0_ls' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_data_inc c 
 where c.type1='pass'       --通过 
   and c.history_due_day1<=0  --历史
   and c.degree_type='1' and c.apply_time_src>c.apply_time1
   --一度关联自身
   and c.cert_no_src = c.cert_no1   
  group by c.order_src
union all
select c.order_src,
        'overdue3_ls' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_data_inc c 
 where c.type1='pass'       --通过 
   and c.history_due_day1>3 --历史
   and c.degree_type='1' and c.apply_time_src>c.apply_time1
   --一度关联自身
   and c.cert_no_src = c.cert_no1   
  group by c.order_src
union all
select c.order_src,
        'overdue30_ls' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_data_inc c 
 where c.type1='pass'       --通过 
   and c.history_due_day1>30  --历史
   and c.degree_type='1' and c.apply_time_src>c.apply_time1  
   --一度关联自身
   and c.cert_no_src = c.cert_no1
   group by c.order_src;
   
--合并一度关联 逾期数据(关联自身)
drop table  lkl_card_score.overdue_cnt_2_self_instant;
create table  lkl_card_score.overdue_cnt_2_self_instant  as
select order_src,
sum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           
sum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   
sum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  
sum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, 
sum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,
sum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls 
from  overdue_cnt_2_self_tmp_instant
group by order_src;   
