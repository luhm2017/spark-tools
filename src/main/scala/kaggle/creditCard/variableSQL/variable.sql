1、创建变量加工表
CREATE TABLE `credit_card_client_variable`(
  `label` double COMMENT '',
  `limit_bal` double COMMENT '',
  `sex` double COMMENT '',
  `education` double COMMENT '',
  `marriage` double COMMENT '',
  `age` double COMMENT '',
  `pay_0` double COMMENT '',
  `pay_2` double COMMENT '',
  `pay_3` double COMMENT '',
  `pay_4` double COMMENT '',
  `pay_5` double COMMENT '',
  `pay_6` double COMMENT '',
  `bill_amt1` double COMMENT '',
  `bill_amt2` double COMMENT '',
  `bill_amt3` double COMMENT '',
  `bill_amt4` double COMMENT '',
  `bill_amt5` double COMMENT '',
  `bill_amt6` double COMMENT '',
  `pay_amt1` double COMMENT '',
  `pay_amt2` double COMMENT '',
  `pay_amt3` double COMMENT '',
  `pay_amt4` double COMMENT '',
  `pay_amt5` double COMMENT '',
  `pay_amt6` double COMMENT ''
 )

 2、数据处理
 insert overwrite table credit_card_client_variable
 select
 label       ,
 limit_bal	,
 sex	 	,
 case when education in ('0','5','6') then '4' else education end as education	,
 case when marriage = '0' then '3' else marriage end as marriage	,
 age		,
 pay_0		,
 pay_2		,
 pay_3		,
 pay_4		,
 pay_5		,
 pay_6		,
 bill_amt1	,
 bill_amt2	,
 bill_amt3	,
 bill_amt4	,
 bill_amt5	,
 bill_amt6	,
 pay_amt1	,
 pay_amt2	,
 pay_amt3	,
 pay_amt4	,
 pay_amt5	,
 pay_amt6
 from credit_card_client;

 3、统计离散变量 sex、education、marriage的信息值
 select label,education,count(*) from credit_card_client_variable
 group by label,education;

 ##频率分布统计，计算IV值和WOE （marriage、sex、education）
 select education,cnt0,cnt1,
 (cnt0/23364) as p_cnt0,
 (cnt1/6636) as p_cnt1,
 LN((cnt1/6636)/(cnt0/23364)) as woe,
 LN((cnt1/6636)/(cnt0/23364))*((cnt1/6636)-(cnt0/23364)) as iv
 from (
 select
 education,
 sum(case when label = 0 then cnt else 0 end) as cnt0,
 sum(case when label = 1 then cnt else 0 end) as cnt1
 from (
 select
 education,
 label,count(*) as cnt
 from credit_card_client_variable
 group by label,education
 ) tab
 group by education) tt;

