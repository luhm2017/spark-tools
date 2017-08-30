use lkl_card_score;

--取全量样本order_src
drop table order_src_group;
create table order_src_group as  
select trim(order_src) order_src   
from 
( select c0 as order_src from fqz_order_data1
union all
select c0 as order_src from   fqz_order_data2
) a   
group by trim(order_src); 


--=====================================================================
--宽表合并
drop table overdue_result_all;
create table overdue_result_all as 
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
from order_src_group a   --256441
left join overdue_cnt_self_sum b on a.order_src = b.order_src  -- 一度关联自身（订单数量、ID数量、黑合同数量、Q标拒绝数量 ）
left join overdue_cnt_2_self c on a.order_src = c.order_src    -- 合并一度关联 逾期数据(关联自身)
left join overdue_cnt_1_sum d on a.order_src = d.order_src     -- 一度关联排除自身（订单数量、ID数量、黑合同数量、Q标拒绝数量 ）
left join overdue_cnt_2  e on a.order_src = e.order_src        -- 一度关联 逾期数据(排除自身)
left join overdue_cnt_2_sum f on a.order_src = f.order_src     -- 二度关联数据 （订单数量、ID数量、黑合同数量、Q标拒绝数量 ）
left join overdue_cnt_2_2 g on a.order_src = g.order_src;      -- 二度关联 逾期数据

--数据处理，过滤全0值
--=====================================================================
insert overwrite table overdue_result_all 
select * from overdue_result_all
where !(
  order_cnt_self         = 0 and 
  id_cnt_self           = 0 and 
  black_cnt_self       = 0 and
  q_refuse_cnt_self     = 0 and 
  pass_cnt_self         = 0 and 
  overdue0_self         = 0 and 
  overdue3_self         = 0 and 
  overdue30_self         = 0 and 
  overdue0_ls_self      = 0 and 
  overdue3_ls_self      = 0 and 
  overdue30_ls_self     = 0 and 
  order_cnt           = 0 and 
  id_cnt               = 0 and 
  black_cnt            = 0 and
  q_refuse_cnt         = 0 and 
  pass_cnt           = 0 and 
  overdue0           = 0 and 
  overdue3           = 0 and 
  overdue30           = 0 and 
  overdue0_ls           = 0 and 
  overdue3_ls           = 0 and 
  overdue30_ls         = 0 and 
  order_cnt_2           = 0 and 
  id_cnt_2           = 0 and
  black_cnt_2            = 0 and 
  q_refuse_cnt_2         = 0 and 
  pass_cnt_2           = 0 and 
  overdue0_2           = 0 and 
  overdue3_2           = 0 and 
  overdue30_2           = 0 and 
  overdue0_ls_2         = 0 and 
  overdue3_ls_2         = 0 and 
  overdue30_ls_2         = 0
);
