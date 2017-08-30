use lkl_card_score;

set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks= 200;
set hive.auto.convert.join=true;


--筛选出新图
drop table fqz_order_data_inc ;
create table fqz_order_data_inc as
select a.* from fqz_order_related_graph_current a
left join fqz_order_related_graph_current_history b on a.order_src = b.order_src
where b.order_src is null 
and a.year = ${year} and a.month = ${month} and a.day = ${day};

--备份老图
insert overwrite table fqz_order_related_graph_current_history
select a.* from fqz_order_related_graph_current a 
where a.year = ${year} and a.month = ${month} and a.day = ${day};
