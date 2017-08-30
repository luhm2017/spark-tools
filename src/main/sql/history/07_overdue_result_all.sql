use lkl_card_score;


drop table order_src_bian_tmp;
create table order_src_bian_tmp as 
select 
a.order_src,
concat(a.c1,'|',a.c3) as ljmx,
1 as depth 
from fqz_order_related_graph a
where a.degree_type = '1' 
and a.apply_time_src>a.apply_time1 
--一度关联进件为黑
and a.label1 = 1 
union all 
select 
a.order_src,
concat(a.c1,'|',a.c3,'|',a.c5,'|',a.c7) as ljmx,
2 as depth 
from fqz_order_related_graph a
where a.degree_type = '2' 
and a.apply_time_src>a.apply_time1 
and a.apply_time_src>a.apply_time2    
--二度关联进件为黑
and a.label2 = 1;

--===========聚合关联边
drop table order_src_bian;   
create table order_src_bian as 
select c.order_src,concat_ws(',',collect_set(ljmx)) as ljmx           
from  order_src_bian_tmp  c
group by c.order_src;

--边字段 关联到结果表 
drop table overdue_result_all_new;
create table overdue_result_all_new as 
select 
c.label,
c.apply_time,
a.*,b.ljmx
from overdue_result_all a
left join order_src_bian b on a.order_src=b.order_src
join fqz_order_performance_data_new c on a.order_src = c.order_id;
