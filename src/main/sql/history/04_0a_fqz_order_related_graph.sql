use lkl_card_score;

--全量坏样本:
--fqz_black_order_data1
--fqz_black_order_data2
--201608--201702的正样本:
--fqz_order1608to1702_case_data1
--fqz_order1608to1702_case_data2

set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks= 200;


--一度关联
insert overwrite table fqz_order_data1 
select a.* from fqz_order1608to1702_case_data1 a
left join fqz_black_order_data1 b on a.c0 = b.c0
where b.c0 is null  --取全量白合同
union all 
select * from fqz_black_order_data1 ;

--二度关联
insert overwrite table fqz_order_data2  
select tab.c0,tab.c1,tab.c2,tab.c3,tab.c4,tab.c5,tab.c6,tab.c7,tab.c8 from (
select * from fqz_order1608to1702_case_data2
union all
select * from fqz_black_order_data2) tab
group by tab.c0,tab.c1,tab.c2,tab.c3,tab.c4,tab.c5,tab.c6,tab.c7,tab.c8;
