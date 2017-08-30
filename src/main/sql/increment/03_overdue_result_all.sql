use lkl_card_score;

--ȡȫ������order_src
drop table order_src_group_instant;
create table order_src_group_instant as  
select trim(c0) order_src   
from graphx_tansported_ordernos_inc
group by c0; 

--=====================================================================
--���ϲ�
drop table overdue_result_all_instant;
create table overdue_result_all_instant as 
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
from order_src_group_instant a   --256441
left join overdue_cnt_self_sum_instant b on a.order_src = b.order_src  -- һ�ȹ�����������������ID�������ں�ͬ������Q��ܾ����� ��
left join overdue_cnt_2_self_instant c on a.order_src = c.order_src    -- �ϲ�һ�ȹ��� ��������(��������)
left join overdue_cnt_1_sum_instant d on a.order_src = d.order_src     -- һ�ȹ����ų���������������ID�������ں�ͬ������Q��ܾ����� ��
left join overdue_cnt_2_instant  e on a.order_src = e.order_src        -- һ�ȹ��� ��������(�ų�����)
left join overdue_cnt_2_sum_instant f on a.order_src = f.order_src     -- ���ȹ������� ������������ID�������ں�ͬ������Q��ܾ����� ��
left join overdue_cnt_2_2_instant g on a.order_src = g.order_src;      -- ���ȹ��� ��������

--���ݴ�������ȫ0ֵ���Ƿ����
--=====================================================================
insert overwrite table overdue_result_all_instant 
select * from overdue_result_all_instant
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
