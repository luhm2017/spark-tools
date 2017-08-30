use lkl_card_score;


--��ȡ������
--drop table fqz_edge_data_instant;
--create table fqz_edge_data_instant as 
--select order_src,label,(split(ljmx,',')) as egdes 
--from overdue_result_all_new_instant where ljmx is not null
--and label <> 2;

--ͳ�ƹ����ߡ�WOE����
--drop table fqz_edge_data_total_instant;
--create table fqz_edge_data_total_instant as 
--select order_src,label,edge 
--from fqz_edge_data_instant fe lateral view explode(fe.egdes) adtable as edge;

--woe���� --ֻ��ȫ������
--drop table fqz_edge_woe_instant;
--create table fqz_edge_woe_instant as 
--select
--edge,
--nvl(ln((good_cnt/12527)/(bad_cnt/29046)),0) as woe
--from (
--select edge,
--count(*) as cnt,
--sum(case when label = 0 then 1 else 0 end) as bad_cnt,
--sum(case when label = 1 then 1 else 0 end) as good_cnt
--from fqz_edge_data_total_instant
--group by edge) tab;

--��ӹ����ߵ����depth
--=====================
drop table fqz_edge_depth_instant;
create table fqz_edge_depth_instant as
select order_src,max(depth) as depth from order_src_bian_tmp_instant 
group by order_src;

--ͳ��ÿ��������Ȩ�� , ÿ����woe����ȫ��ͳ��
drop table fqz_order_edge_woe_instant;
create table fqz_order_edge_woe_instant as 
select 
a.order_src,
sum(b.woe) as edge_woe_sum,
max(woe) as edge_woe_max,
min(woe) as edge_woe_min
from 
fqz_edge_data_total_instant a 
join fqz_edge_woe b on a.edge = b.edge   --ÿ����woeֵ����ȫ��ͳ��
group by a.order_src;

--�ϲ����ܽ��
drop table overdue_result_all_new_woe_instant;
create table overdue_result_all_new_woe_instant as
select a.*,
b.edge_woe_sum,
b.edge_woe_max,
b.edge_woe_min,
c.depth
from overdue_result_all_new_instant a 
left join fqz_order_edge_woe_instant b on a.order_src = b.order_src
left join fqz_edge_depth_instant c on a.order_src = c.order_src;
