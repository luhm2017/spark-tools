use lkl_card_score;

set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks= 200;
set hive.auto.convert.join=true;
set hive.mapjoin.smalltable.filesize=5000000;
set mapreduce.job.queuename=szbigdata;

--����ͬ�������graphx_tansported_ordernos��ȡ������ͼ
--step1.ʵʱ���ݵ�ǰ��ͼ
drop table graphx_tansported_ordernos_current;
create table graphx_tansported_ordernos_current as
select * from graphx_tansported_ordernos a
where a.year = ${year} and a.month = ${month} and a.day = ${day};

--step2.��ȡ������ͼ
drop table graphx_tansported_ordernos_inc;
create table graphx_tansported_ordernos_inc as
select a.* from graphx_tansported_ordernos_current a
left join graphx_tansported_ordernos_history b on a.c0 = b.c0
where b.c0 is null;

--steo3.���ݵ���ʷͼ
drop table graphx_tansported_ordernos_history;
create table graphx_tansported_ordernos_history as 
select * from graphx_tansported_ordernos_current;



