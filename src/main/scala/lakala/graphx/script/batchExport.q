USE lkl_card_score_dev;

SET mapreduce.job.queuename=szoffline;

SET hive.exec.parallel=true;

SET hive.exec.parallel.thread.number = 32;

SET mapreduce.reduce.shuffle.input.buffer.percent=0.6;

SET hive.groupby.mapaggr.checkinterval=100000;

SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

SET mapreduce.input.fileinputformat.split.maxsize=1073741824;

SET mapreduce.input.fileinputformat.split.minsize=1;

SET mapreduce.input.fileinputformat.split.minsize.per.node=536870912;

SET mapreduce.input.fileinputformat.split.minsize.per.rack=536870912;

SET hive.merge.mapredfiles=true;

SET hive.merge.mapfiles=true;

SET hive.merge.smallfiles.avgsize=268435456;

SET hive.merge.size.per.task = 1024000000;

SET hive.exec.max.dynamic.partitions=100000;

SET hive.exec.max.dynamic.partitions.pernode=100000;

SET mapred.map.tasks.speculative.execution=true;

SET mapred.reduce.tasks.speculative.execution=true;

SET hive.mapred.reduce.tasks.speculative.execution=true;

SET hive.groupby.skewindata=true;

SET hive.exec.compress.intermediate=true;

set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE lkl_card_score_dev.one_degree_apply_result_orcfile partition(dt) SELECT * FROM lkl_card_score_dev.one_degree_apply_result_orcfile where dt='${hiveco
nf:dt}';

INSERT OVERWRITE TABLE lkl_card_score_dev.two_degree_apply_result_orcfile partition(dt) SELECT * FROM lkl_card_score_dev.two_degree_apply_result_orcfile where dt='${hiveco
nf:dt}';
