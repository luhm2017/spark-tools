set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks= 200;
set mapreduce.job.queuename=szbigdata;
set mapreduce.job.queuename=szrisk;

--导出一度二度原始图数据
kl_card_score.fqz_score_order_201703_20170810_data1

lkl_card_score.fqz_score_order_201703_20170810_data2

--添加带日期需跑数订单
create table lkl_card_score.tbl_order_id_1703_to_1708_timestamp as 
select a.order_id,a.contract_no,b.loan_date
from lkl_card_score.tbl_order_id_1703_to_1708 a 
join r_creditloan.s_c_loan_apply b on a.order_id=b.order_id
where b.year='2017' and b.month='08' and b.day='28'

--关联一度二度图原始数据
--一度
create table lkl_card_score.fqz_score_order_201703_data1_temp as 
select a.*
from lkl_card_score.fqz_score_order_201703_20170810_data1 a 
join lkl_card_score.tbl_order_id_1703_to_1708_timestamp b on a.c0=b.order_id
where b.loan_date>='2017-03-01 00:00:00' and b.loan_date<'2017-04-01 00:00:00'

--二度
create table lkl_card_score.fqz_score_order_201703_data2_temp as 
select a.*
from lkl_card_score.fqz_score_order_201703_20170810_data2 a 
join lkl_card_score.tbl_order_id_1703_to_1708_timestamp b on a.c0=b.order_id
where b.loan_date>='2017-03-01 00:00:00' and b.loan_date<'2017-04-01 00:00:00'

--一度去重
create table lkl_card_score.fqz_score_order_201703_data1 as 
select c0,c1,c2,c3,c4
from lkl_card_score.fqz_score_order_201703_data1_temp
group by c0,c1,c2,c3,c4

--二度去重
create table lkl_card_score.fqz_score_order_201703_data2 as
select c0,c1,c2,c3,c4,c5,c6,c7,c8
from lkl_card_score.fqz_score_order_201703_data2_temp
group by c0,c1,c2,c3,c4,c5,c6,c7,c8





----------------------------------------------------------------------------------
use lkl_card_score;
--一度二度关联进件表现数据(取表现表28号数据)
drop table fqz_order_related_graph_1703;

set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks= 200;
set mapreduce.job.queuename=szrisk;

create table fqz_order_related_graph_1703 as
select '1' as degree_type,a.c0 as order_src,b.performance as performance_src,b.apply_time as apply_time_src,b.type as type_src,
        b.history_due_day as history_due_day_src,b.current_due_day as current_due_day_src,b.cert_no as cert_no_src,b.label as label_src,a.c1,a.c2,a.c3,
        a.c4 as order1,c.performance as performance1,c.apply_time as apply_time1,c.type as type1,c.history_due_day as history_due_day1,
        c.current_due_day as current_due_day1,c.cert_no as cert_no1,c.label as label1,'null' as c5,'null' as c6,'null' as c7,'null' as order2,
        'null' as performance2,'null' as apply_time2,'null' as type2,0 as history_due_day2,0 as current_due_day2,'null' as cert_no2,0 as label2
from fqz_score_order_201703_data1 a   
join fqz_order_performance_data_new b on a.c0 = b.order_id and b.year='${year}' and b.month='${month}' and b.day='${day}'
join fqz_order_performance_data_new c on a.c4 = c.order_id and c.year='${year}' and c.month='${month}' and c.day='${day}'

union all

select '2' as degree_type,a.c0 as order_src,b.performance as performance_src,b.apply_time as apply_time_src,b.type as type_src,
        b.history_due_day as history_due_day_src,b.current_due_day as current_due_day_src,b.cert_no as cert_no_src,b.label as label_src,a.c1,a.c2,a.c3,
        a.c4 as order1,c.performance as performance1,c.apply_time as apply_time1,c.type as type1,c.history_due_day as history_due_day1,
        c.current_due_day as current_due_day1,c.cert_no as cert_no1,c.label as label1,a.c5,a.c6,a.c7,a.c8 as order2,d.performance as performance2,
        d.apply_time as apply_time2,d.type as type2,d.history_due_day as history_due_day2,d.current_due_day as current_due_day2,d.cert_no as cert_no2,
        d.label as label2
from fqz_score_order_201703_data2 a 
join fqz_order_performance_data_new b on a.c0 = b.order_id and b.year='${year}' and b.month='${month}' and b.day='${day}'
join fqz_order_performance_data_new c on a.c4 = c.order_id and c.year='${year}' and c.month='${month}' and c.day='${day}'
join fqz_order_performance_data_new d on a.c8 = d.order_id and d.year='${year}' and d.month='${month}' and d.day='${day}';

--基于关系图谱数据的清洗
--================================================================================
drop table overdue_cnt_self_1703;

create table overdue_cnt_self_1703 as 
-- 一度关联自身_订单数量:该进件下同身份证申请订单数量     
SELECT a.order_src,'order_cnt' title, count(distinct a.order1) cnt 
FROM fqz_order_related_graph_1703 a     --order订单表现
where a.degree_type='1' and a.apply_time_src>a.apply_time1 and a.cert_no_src = a.cert_no1
group by  a.order_src    

union all 

-- 一度关联自身_ID数量:该进件下同身份证申请身份证数(人数)  
SELECT a.order_src,'id_cnt' title,count(distinct a.cert_no1) cnt 
FROM fqz_order_related_graph_1703 a     --order订单表现
where a.degree_type='1' and a.apply_time_src>a.apply_time1 and a.cert_no_src = a.cert_no1
group by a.order_src 

union all

-- 一度关联自身_黑合同数量:该进件下同身份证申请黑订单数量
SELECT a.order_src,'black_cnt' title,count(distinct a.order1) cnt 
FROM fqz_order_related_graph_1703 a     --order订单表现
where a.degree_type='1' and a.apply_time_src>a.apply_time1 and a.cert_no_src = a.cert_no1 and a.label1 = 1
group by a.order_src

union all 

-- 一度关联自身_Q标拒绝数量:该进件下同身份证申请被Q标拒绝订单数量
SELECT a.order_src,'q_refuse_cnt' title,count(distinct a.order1) cnt 
FROM fqz_order_related_graph_1703 a     --order订单表现  
where performance1='q_refuse' and a.degree_type='1' and a.apply_time_src>a.apply_time1 and a.cert_no_src = a.cert_no1
group by a.order_src 

union all

-- 一度关联自身_通过合同数量:该进件下同身份证申请通过订单数量
SELECT a.order_src,'pass_cnt' title,count(distinct a.order1) cnt 
FROM fqz_order_related_graph_1703 a     --order订单表现
where a.type1='pass'  and a.degree_type='1' and a.apply_time_src>a.apply_time1 and a.cert_no_src = a.cert_no1
group by a.order_src ;

--合并数据  一度关联自身
drop table  lkl_card_score.overdue_cnt_self_sum_1703;

create table  lkl_card_score.overdue_cnt_self_sum_1703  as
select order_src,
       sum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt_self ,           
       sum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt_self ,   
       sum(case when title= 'black_cnt' then cnt else 0 end ) black_cnt_self , 
       sum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt_self,    
       sum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt_self 
from  overdue_cnt_self_1703
group by order_src;

--一度关联自身
-- 一度_当前无逾期数量        
-- 一度_当前3数量            
-- 一度_当前30数量 
-- 一度_历史无逾期数量        
-- 一度_历史3+数量            
-- 一度_历史30+数量            
drop table overdue_cnt_2_self_tmp_1703;

create table overdue_cnt_2_self_tmp_1703 as 
-- 一度_当前无逾期数量:该进件下同身份证申请通过当前无逾期订单数量
select c.order_src,'overdue0' title,count(distinct c.order1) cnt 
from fqz_order_related_graph_1703 c 
where c.type1='pass' and c.current_due_day1<=0 and c.degree_type='1' and c.apply_time_src>c.apply_time1 and c.cert_no_src = c.cert_no1
group by c.order_src

union all

-- 一度_当前3数量:该进件下同身份证申请通过当前逾期天数大于3天订单数量
select c.order_src,'overdue3' title,count(distinct c.order1) cnt 
from fqz_order_related_graph_1703 c 
where c.type1='pass' and c.current_due_day1>3 and c.degree_type='1' and c.apply_time_src>c.apply_time1 and c.cert_no_src = c.cert_no1
group by c.order_src

union all

-- 一度_当前30数量:该进件下同身份证申请通过当前逾期天数大于30天订单数量
select c.order_src,'overdue30' title,count(distinct c.order1) cnt 
from fqz_order_related_graph_1703 c 
where c.type1='pass' and c.current_due_day1>30 and c.degree_type='1' and c.apply_time_src>c.apply_time1 and c.cert_no_src = c.cert_no1
group by c.order_src 

union all

-- 一度_历史无逾期数量:该进件下同身份证申请通过历史无逾期订单数量
select c.order_src,'overdue0_ls' title,count(distinct c.order1) cnt 
from fqz_order_related_graph_1703 c 
where c.type1='pass' and c.history_due_day1<=0 and c.degree_type='1' and c.apply_time_src>c.apply_time1 and c.cert_no_src = c.cert_no1   
group by c.order_src

union all

-- 一度_历史3+数量:该进件下同身份证申请通过历史逾期天数大于3天订单数量
select c.order_src,'overdue3_ls' title,count(distinct c.order1) cnt 
from fqz_order_related_graph_1703 c 
where c.type1='pass' and c.history_due_day1>3 and c.degree_type='1' and c.apply_time_src>c.apply_time1 and c.cert_no_src = c.cert_no1   
group by c.order_src

union all

-- 一度_历史30+数量:该进件下同身份证申请通过历史逾期天数大于30天订单数量
select c.order_src,'overdue30_ls' title,count(distinct c.order1) cnt 
from fqz_order_related_graph_1703 c 
where c.type1='pass' and c.history_due_day1>30 and c.degree_type='1' and c.apply_time_src>c.apply_time1 and c.cert_no_src = c.cert_no1
group by c.order_src;   

drop table  lkl_card_score.overdue_cnt_2_self_1703;

create table  lkl_card_score.overdue_cnt_2_self_1703  as
select order_src,
       sum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           
       sum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   
       sum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  
       sum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, 
       sum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,
       sum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls 
from  overdue_cnt_2_self_tmp_1703
group by order_src;

--一度关联排除自身   
--================================================================================================
drop table overdue_cnt_1_1703;

create table overdue_cnt_1_1703 as 
-- 一度_订单数量:该进件下除身份证外其他信息关联申请订单数量
SELECT a.order_src,'order_cnt' title, count(distinct a.order1) cnt 
FROM fqz_order_related_graph_1703 a     --order订单表现
where a.degree_type='1' and a.apply_time_src>a.apply_time1 and a.cert_no_src <> a.cert_no1
group by  a.order_src   

union all 

-- 一度_ID数量:该进件下除身份证外其他信息关联的身份证数量(人数) 
SELECT a.order_src,'id_cnt' title,count(distinct a.cert_no1) cnt 
FROM fqz_order_related_graph_1703 a     --order订单表现
where a.degree_type='1' and a.apply_time_src>a.apply_time1 and a.cert_no_src <> a.cert_no1
group by a.order_src 

union all

-- 一度关联自身_黑合同数量:该进件下除身份证外其他信息关联黑订单数量
SELECT a.order_src,'black_cnt' title,count(distinct a.order1) cnt 
FROM fqz_order_related_graph_1703 a     --order订单表现
where a.degree_type='1' and a.apply_time_src>a.apply_time1 and a.cert_no_src <> a.cert_no1 and a.label1 = 1
group by a.order_src

union all 

-- 一度_Q标拒绝数量:该进件下除身份证外其他信息关联Q标拒绝订单数量
SELECT a.order_src,'q_refuse_cnt' title,count(distinct a.order1) cnt 
FROM fqz_order_related_graph_1703 a      
where performance1='q_refuse' and a.degree_type='1' and a.apply_time_src>a.apply_time1 and a.cert_no_src <> a.cert_no1
group by a.order_src 

union all

-- 一度_通过合同数量:该进件下除身份证外其他信息关联通过订单数量
SELECT a.order_src,'pass_cnt' title,count(distinct a.order1) cnt 
FROM fqz_order_related_graph_1703 a     --order订单表现
where a.type1='pass'  and a.degree_type='1' and a.apply_time_src>a.apply_time1 and a.cert_no_src <> a.cert_no1
group by a.order_src ;

drop table  lkl_card_score.overdue_cnt_1_sum_1703;

create table  lkl_card_score.overdue_cnt_1_sum_1703  as
select order_src,
       sum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt ,           
       sum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt ,
       sum(case when title= 'black_cnt' then cnt else 0 end ) black_cnt ,    
       sum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt ,    
       sum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt 
from  overdue_cnt_1_1703
group by order_src;
--一度关联排除自身
-- 一度_当前无逾期数量        
-- 一度_当前3+数量            
-- 一度_当前30+数量 
-- 一度_历史无逾期数量        
-- 一度_历史3+数量            
-- 一度_历史30+数量    
drop table overdue_cnt_2_tmp_1703;

create table overdue_cnt_2_tmp_1703 as 
--一度关联排除自身:该进件下除身份证外其他信息关联通过当前无逾期订单数量
select c.order_src,'overdue0' title,count(distinct c.order1) cnt 
from fqz_order_related_graph_1703 c 
where c.type1='pass' and c.current_due_day1<=0 and c.degree_type='1' and c.apply_time_src>c.apply_time1 and c.cert_no_src <> c.cert_no1
group by c.order_src

union all

-- 一度_当前3+数量:该进件下除身份证外其他信息关联通过当前逾期天数大于3天订单数量
select c.order_src,'overdue3' title,count(distinct c.order1) cnt 
from fqz_order_related_graph_1703 c 
where c.type1='pass' and c.current_due_day1>3 and c.degree_type='1' and c.apply_time_src>c.apply_time1 and c.cert_no_src <> c.cert_no1
group by c.order_src

union all

-- 一度_当前30+数量:该进件下除身份证外其他信息关联通过当前逾期天数大于30天订单数量
select c.order_src,'overdue30' title,count(distinct c.order1) cnt 
from fqz_order_related_graph_1703 c 
where c.type1='pass' and c.current_due_day1>30 and c.degree_type='1' and c.apply_time_src>c.apply_time1 and c.cert_no_src <> c.cert_no1
group by c.order_src 

union all

-- 一度_历史无逾期数量:该进件下除身份证外其他信息关联通过历史无逾期订单数量
select c.order_src,'overdue0_ls' title,count(distinct c.order1) cnt 
from fqz_order_related_graph_1703 c 
where c.type1='pass' and c.history_due_day1<=0 and c.degree_type='1' and c.apply_time_src>c.apply_time1 and c.cert_no_src <> c.cert_no1   
group by c.order_src

union all

-- 一度_历史3+数量:该进件下除身份证外其他信息关联通过历史逾期天数大于3天订单数量
select c.order_src,'overdue3_ls' title,count(distinct c.order1) cnt 
from fqz_order_related_graph_1703 c 
where c.type1='pass' and c.history_due_day1>3 and c.degree_type='1' and c.apply_time_src>c.apply_time1 and c.cert_no_src <> c.cert_no1   
group by c.order_src

union all

-- 一度_历史30+数量:该进件下除身份证外其他信息关联通过历史逾期天数大于30天订单数量
select c.order_src,'overdue30_ls' title,count(distinct c.order1) cnt 
from fqz_order_related_graph_1703 c 
where c.type1='pass' and c.history_due_day1>30 and c.degree_type='1' and c.apply_time_src>c.apply_time1 and c.cert_no_src <> c.cert_no1
group by c.order_src;
   
--合并一度关联 逾期数据(排除自身)
drop table  lkl_card_score.overdue_cnt_2_1703;

create table  lkl_card_score.overdue_cnt_2_1703 as
select order_src,
       sum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           
       sum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   
       sum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  
       sum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, 
       sum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,
       sum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls 
from  overdue_cnt_2_tmp_1703
group by order_src;
 
--==================================================================================================== 
-- 2度关联数据
drop table overdue_cnt_1_2_1703;

create table overdue_cnt_1_2_1703 as 
-- 2度_订单数量:该进件二度关联申请订单数量
SELECT a.order_src,'order_cnt' title, count(distinct a.order2) cnt 
FROM fqz_order_related_graph_1703 a     --order订单表现
where a.degree_type='2' and a.apply_time_src>a.apply_time2 and a.apply_time_src>a.apply_time1
group by  a.order_src  

union all 

-- 2度_ID数量:该进件二度关联申请身份证数量(人数)
SELECT a.order_src,'id_cnt' title,count(distinct a.cert_no2) cnt 
FROM fqz_order_related_graph_1703 a     --order订单表现
where a.degree_type='2' and a.apply_time_src>a.apply_time2 and a.apply_time_src>a.apply_time1
group by a.order_src 

union all

-- 2度_黑合同数量:该进件二度关联订单为黑的订单数量
SELECT a.order_src,'black_cnt' title,count(distinct a.order2) cnt 
FROM fqz_order_related_graph_1703 a     --order订单表现
where a.degree_type='2' and a.apply_time_src>a.apply_time2 and a.apply_time_src>a.apply_time1 and a.label2 = 1
group by a.order_src
 
union all 

-- 2度_Q标拒绝数量:该进件二度关联订单为Q标拒绝的订单数量
SELECT a.order_src,'q_refuse_cnt' title,count(distinct a.order2) cnt 
FROM fqz_order_related_graph_1703 a     --order订单表现  
where performance2='q_refuse' and a.degree_type='2' and a.apply_time_src>a.apply_time2 and a.apply_time_src>a.apply_time1
group by a.order_src 

union all

-- 2度_通过合同数量:该进件二度关联订单为通过的订单数量
SELECT a.order_src,'pass_cnt' title,count(distinct a.order2) cnt 
FROM fqz_order_related_graph_1703 a     --order订单表现
where a.type2='pass' and a.degree_type='2' and a.apply_time_src>a.apply_time2 and a.apply_time_src>a.apply_time1
group by a.order_src ;


--合并二度关联数据 
drop table overdue_cnt_2_sum_1703;

create table  lkl_card_score.overdue_cnt_2_sum_1703  as
select order_src,
       sum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt ,           
       sum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt ,   
       sum(case when title= 'black_cnt' then cnt else 0 end ) black_cnt , 
       sum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt ,    
       sum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt 
from  overdue_cnt_1_2_1703
group by order_src;


--二度关联排除自身
drop table overdue_cnt_2_2_tmp_1703

create table overdue_cnt_2_2_tmp_1703 as 
-- 二度_当前无逾期数量:该进件下二度关联通过二度当前无逾期订单数量
select c.order_src,'overdue0' title,count(distinct c.order2) cnt 
from fqz_order_related_graph_1703 c 
where c.type2='pass' and c.current_due_day2<=0 and c.degree_type='2' and c.apply_time_src>c.apply_time1 and c.apply_time_src>c.apply_time2 
group by c.order_src

union all

-- 二度_当前3+数量:该进件下二度关联通过二度当前逾期天数大于3天订单数量
select c.order_src,'overdue3' title,count(distinct c.order2) cnt 
from fqz_order_related_graph_1703 c 
where c.type2='pass' and c.current_due_day2>3 and c.degree_type='2' and c.apply_time_src>c.apply_time1 and c.apply_time_src>c.apply_time2    
group by c.order_src

union all

-- 二度_当前30+数量:该进件下二度关联通过二度当前逾期天数大于30天订单数量
select c.order_src,'overdue30' title,count(distinct c.order2) cnt 
from fqz_order_related_graph_1703 c 
where c.type2='pass' and c.current_due_day2>30 and c.degree_type='2' and c.apply_time_src>c.apply_time1 and c.apply_time_src>c.apply_time2 
group by c.order_src 

union all

-- 二度_历史无逾期数量:该进件下二度关联通过二度历史无逾期订单数量
select c.order_src,'overdue0_ls' title,count(distinct c.order2) cnt 
from fqz_order_related_graph_1703 c 
where c.type2='pass' and c.history_due_day2<=0 and c.degree_type='2' and c.apply_time_src>c.apply_time1 and c.apply_time_src>c.apply_time2    
group by c.order_src

union all

-- 二度_历史3+数量:该进件下二度关联通过二度历史逾期天数大于3天订单数量
select c.order_src,'overdue3_ls' title,count(distinct c.order2) cnt 
from fqz_order_related_graph_1703 c 
where c.type2='pass' and c.history_due_day2>3 and c.degree_type='2' and c.apply_time_src>c.apply_time1 and c.apply_time_src>c.apply_time2 
group by c.order_src

union all

-- 二度_历史30+数量:该进件下二度关联通过二度历史逾期天数大于30天订单数量
select c.order_src,'overdue30_ls' title,count(distinct c.order2) cnt 
from fqz_order_related_graph_1703 c 
where c.type2='pass' and c.history_due_day2>30 and c.degree_type='2' and c.apply_time_src>c.apply_time1 and c.apply_time_src>c.apply_time2    
group by c.order_src;
   
--合并二度关联 逾期数据
drop table  lkl_card_score.overdue_cnt_2_2_1703;

create table  lkl_card_score.overdue_cnt_2_2_1703  as
select order_src,
       sum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           
       sum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   
       sum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  
       sum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, 
       sum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,
       sum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls 
from  overdue_cnt_2_2_tmp_1703
group by order_src;

--取全量样本order_src
--一度二度订单id所有
drop table order_src_group_1703;

create table order_src_group_1703 as  
select trim(order_src) order_src   
from 
      ( 
        select c0 as order_src 
        from fqz_score_order_201703_data1  

        union all

        select c0 as order_src 
        from fqz_score_order_201703_data2
       ) a   
group by trim(order_src); 

--=====================================================================
--宽表合并
--一度二度所有订单的指标表现
drop table overdue_result_all_1703;

create table overdue_result_all_1703 as 
select 
a.order_src,
nvl(b.order_cnt_self,0) as order_cnt_self,
nvl(b.id_cnt_self,0) as id_cnt_self,
nvl(b.black_cnt_self,0) as black_cnt_self,
nvl(b.q_refuse_cnt_self,0) as q_refuse_cnt_self,
nvl(b.pass_cnt_self,0) as pass_cnt_self,
nvl(c.overdue0,0) as overdue0_self,
nvl(c.overdue3,0) as overdue3_self,
nvl(c.overdue30,0) as overdue30_self,
nvl(c.overdue0_ls,0) as overdue0_ls_self,
nvl(c.overdue3_ls,0) as overdue3_ls_self,
nvl(c.overdue30_ls,0) as overdue30_ls_self,
nvl(d.order_cnt,0) as order_cnt,
nvl(d.id_cnt,0) as id_cnt,
nvl(d.black_cnt,0) as black_cnt,
nvl(d.q_refuse_cnt,0) as q_refuse_cnt,
nvl(d.pass_cnt,0) as pass_cnt,
nvl(e.overdue0,0) as overdue0,
nvl(e.overdue3,0) as overdue3,
nvl(e.overdue30,0) as overdue30,
nvl(e.overdue0_ls,0) as overdue0_ls ,
nvl(e.overdue3_ls,0) as overdue3_ls,
nvl(e.overdue30_ls,0) as overdue30_ls, 
nvl(f.order_cnt,0) as order_cnt_2,
nvl(f.id_cnt,0)       as id_cnt_2,
nvl(f.black_cnt,0)       as black_cnt_2,
nvl(f.q_refuse_cnt,0) as q_refuse_cnt_2,
nvl(f.pass_cnt,0)    as pass_cnt_2,
nvl(g.overdue0,0)    as overdue0_2,
nvl(g.overdue3,0)    as overdue3_2,
nvl(g.overdue30,0)   as overdue30_2,
nvl(g.overdue0_ls,0) as overdue0_ls_2,
nvl(g.overdue3_ls,0) as overdue3_ls_2,
nvl(g.overdue30_ls,0) as overdue30_ls_2
from order_src_group_1703 a   --256441
left join overdue_cnt_self_sum_1703 b on a.order_src = b.order_src  -- 一度关联自身（订单数量、ID数量、黑合同数量、Q标拒绝数量 ）
left join overdue_cnt_2_self_1703 c on a.order_src = c.order_src    -- 合并一度关联 逾期数据(关联自身)
left join overdue_cnt_1_sum_1703 d on a.order_src = d.order_src     -- 一度关联排除自身（订单数量、ID数量、黑合同数量、Q标拒绝数量 ）
left join overdue_cnt_2_1703  e on a.order_src = e.order_src        -- 一度关联 逾期数据(排除自身)
left join overdue_cnt_2_sum_1703 f on a.order_src = f.order_src     -- 二度关联数据 （订单数量、ID数量、黑合同数量、Q标拒绝数量 ）
left join overdue_cnt_2_2_1703 g on a.order_src = g.order_src;      -- 二度关联 逾期数据

--数据处理，过滤全0值
--=====================================================================
--剔除指标表现全为0的数据
insert overwrite table overdue_result_all_1703 
select * from overdue_result_all_1703
where !(
  order_cnt_self         = 0 and 
  id_cnt_self          = 0 and 
  black_cnt_self       = 0 and
  q_refuse_cnt_self    = 0 and 
  pass_cnt_self        = 0 and 
  overdue0_self        = 0 and 
  overdue3_self        = 0 and 
  overdue30_self         = 0 and 
  overdue0_ls_self     = 0 and 
  overdue3_ls_self     = 0 and 
  overdue30_ls_self    = 0 and 
  order_cnt          = 0 and 
  id_cnt               = 0 and 
  black_cnt            = 0 and
  q_refuse_cnt         = 0 and 
  pass_cnt           = 0 and 
  overdue0           = 0 and 
  overdue3           = 0 and 
  overdue30          = 0 and 
  overdue0_ls          = 0 and 
  overdue3_ls          = 0 and 
  overdue30_ls         = 0 and 
  order_cnt_2          = 0 and 
  id_cnt_2           = 0 and
  black_cnt_2            = 0 and 
  q_refuse_cnt_2         = 0 and 
  pass_cnt_2           = 0 and 
  overdue0_2           = 0 and 
  overdue3_2           = 0 and 
  overdue30_2          = 0 and 
  overdue0_ls_2        = 0 and 
  overdue3_ls_2        = 0 and 
  overdue30_ls_2         = 0
);

--取图关联边数据
--==================================================================
--一度二度关联到黑订单的进件
drop table order_src_bian_tmp_1703;

create table order_src_bian_tmp_1703 as 
select a.order_src,concat(a.c1,'|',a.c3) as ljmx,1 as depth 
from fqz_order_related_graph_1703 a
where a.degree_type = '1' and a.apply_time_src>a.apply_time1 and a.label1 = 1 

union all 

select a.order_src,concat(a.c1,'|',a.c3,'|',a.c5,'|',a.c7) as ljmx,2 as depth 
from fqz_order_related_graph_1703 a
where a.degree_type = '2' and a.apply_time_src>a.apply_time1 and a.apply_time_src>a.apply_time2 and a.label2 = 1;

--===========聚合关联边
--获取这些进件的边关系
drop table order_src_bian_1703; 

create table order_src_bian_1703 as 
select c.order_src,concat_ws(',',collect_set(ljmx)) as ljmx           
from  order_src_bian_tmp_1703  c
group by c.order_src;

--边字段 关联到结果表 
--一度二度订单关联边及表现
drop table overdue_result_all_new_1703;

create table overdue_result_all_new_1703 as 
select c.label,c.apply_time,a.*,b.ljmx
from overdue_result_all_1703 a
left join order_src_bian_1703 b on a.order_src=b.order_src
join fqz_order_performance_data_new c on a.order_src = c.order_id and c.year='${year}' and c.month='${month}' and c.day='${day}';

--=========================================================================
--样本分布
select label,count(*) 
from overdue_result_all_new_1703
group by label;
--0	140806
--1	17878

--WOE处理
--统计有边数据
select count(*)  
from overdue_result_all_new_1703 
where ljmx is not null ; 

--=========================================================================
--提取边数据
--一度二度订单去除边为空及标签为灰的数据
drop table fqz_edge_data_1703;

create table fqz_edge_data_1703 as 
select order_src,label,(split(ljmx,',')) as egdes 
from overdue_result_all_new_1703 
where ljmx is not null and label <> 2;

--统计关联边、WOE处理
--将边横转为列
drop table fqz_edge_data_total_1703;

create table fqz_edge_data_total_1703 as 
select order_src,label,edge 
from fqz_edge_data_1703 fe lateral view explode(fe.egdes) adtable as edge;


--用来计算woe的分母(一度二度去除边为空(关联到黑的订单的边)去除表现为灰的订单)
select label,count(*)
from fqz_edge_data_total_1703
group by label

--woe处理
--依赖实际数据
create table fqz_edge_woe_1703 as 
select edge,nvl(ln((good_cnt/12527)/(bad_cnt/29046)),0) as woe
from 
      (
        select edge,
               count(*) as cnt,
               sum(case when label = 0 then 1 else 0 end) as good_cnt,
               sum(case when label = 1 then 1 else 0 end) as bad_cnt
        from fqz_edge_data_total_1703
        group by edge
        ) tab;

--添加关联边的深度depth
--=====================
--一度二度关联到黑订单的进件最大关联深度
create table fqz_edge_depth_1703 as
select order_src,max(depth) as depth 
from order_src_bian_tmp_1703 
group by order_src;

--统计每个订单边权重
--一度二度去除边为空去除表现为灰订单后求取woe之和,最大,最小值
create table fqz_order_edge_woe_1703 as 
select a.order_src,
       sum(b.woe) as edge_woe_sum,
       max(woe) as edge_woe_max,
       min(woe) as edge_woe_min
from fqz_edge_data_total_1703 a 
join fqz_edge_woe_1703 b on a.edge = b.edge
group by a.order_src;

--合并最总结果
--一度二度订单关联边及表现再关联黑订单最大深度,woe之和及最大,最小值
drop table overdue_result_all_new_woe_1703;

create table overdue_result_all_new_woe_1703 as
select a.*,b.edge_woe_sum,b.edge_woe_max,b.edge_woe_min,c.depth
from overdue_result_all_new_1703 a 
left join fqz_order_edge_woe_1703 b on a.order_src = b.order_src
left join fqz_edge_depth_1703 c on a.order_src = c.order_src;

--数据类型转换
drop table overdue_result_all_new_woe_1703_total;

insert overwrite table overdue_result_all_new_woe_1703_total 
select * from overdue_result_all_new_woe_1703;
--=============================================
CREATE TABLE `overdue_result_all_new_woe_1703_total`(
    `label` double, 
    `apply_time` string, 
    `order_src` string, 
    `order_cnt_self` double, 
    `id_cnt_self` double, 
    `black_cnt_self` double, 
    `q_refuse_cnt_self` double, 
    `pass_cnt_self` double, 
    `overdue0_self` double, 
    `overdue3_self` double, 
    `overdue30_self` double, 
    `overdue0_ls_self` double, 
    `overdue3_ls_self` double, 
    `overdue30_ls_self` double, 
    `order_cnt` double, 
    `id_cnt` double, 
    `black_cnt` double, 
    `q_refuse_cnt` double, 
    `pass_cnt` double, 
    `overdue0` double, 
    `overdue3` double, 
    `overdue30` double, 
    `overdue0_ls` double, 
    `overdue3_ls` double, 
    `overdue30_ls` double, 
    `order_cnt_2` double, 
    `id_cnt_2` double, 
    `black_cnt_2` double, 
    `q_refuse_cnt_2` double, 
    `pass_cnt_2` double, 
    `overdue0_2` double, 
    `overdue3_2` double, 
    `overdue30_2` double, 
    `overdue0_ls_2` double, 
    `overdue3_ls_2` double, 
    `overdue30_ls_2` double, 
    `ljmx` string, 
    `edge_woe_sum` double, 
    `edge_woe_max` double, 
    `edge_woe_min` double,
    `depth` double)
--================================================