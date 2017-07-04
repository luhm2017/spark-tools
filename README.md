# fs-spark-tools
此项目用来记录下关于spark的常用工具以及实践相关代码

### jar上传指令

    scp -P 50022 /root/IdeaProjects/spark-tools/target/spark-tools-1.0.jar yanghb@172.17.0.39:/home/yanghb/test
    scp /home/yanghb/test/spark-tools-1.0.jar yanghb@172.17.32.224:/home/yanghb/lib/

### 将FileSystem的数据导入hive

    $SPARK_HOME/bin/spark-submit --master yarn --num-executors 2 --class yanghb.sparktools.SparkTools /home/yanghb/lib/spark-tools-1.0.jar\
     loadFS2Hive /facishare-data/fscloud/import_data/Enterprise/172.17.1.252 webcrawler.enterprise

### 将mysql的数据导到文件系统

如果表太大，会导致OOM，就用以下这句

    $SPARK_HOME/bin/spark-submit --master local[3] --class yanghb.sparktools.SparkTools /root/yanghb/lib/spark-tools-1.0.jar \
     JDBC2FS 172.31.105.10 develop develop company_profile company_profile file:/root/yanghb/data/company_profile id 66869 851080 30

如果不会导致OOM，用以下这句

    $SPARK_HOME/bin/spark-submit --master local[3] --class yanghb.sparktools.SparkTools /root/yanghb/lib/spark-tools-1.0.jar \
     JDBC2FS 172.31.105.10 root 12345678 company_profile active_company file:/root/yanghb/data/active_company

### 将hive数据导入到FileSystem

    $SPARK_HOME/bin/spark-submit --master local[1] --class yanghb.sparktools.SparkTools /home/yanghb/lib/spark-tools-1.0.jar \
     hive2FS business_recommendation.tmp_sorted file:/home/yanghb/data/tmp_sorted

### 将FileSystem的数据导入mysql

1. 如果没有中文报错，可以直接使用以下的指令：

        $SPARK_HOME/bin/spark-submit --master local[2] --class yanghb.sparktools.SparkTools /root/yanghb/lib/spark-tools-1.0.jar \
         loadFS2JDBC 172.31.105.10 root 12345678 company_profile enterprise_sz_prediction_sorted file:/root/yanghb/data/tmp_sorted

2. 如果有中文报错类似 Incorrect string value: '\xF0\xAF\xA0\xA5...' for column 'name' 这种的，检查数据库和考虑其他编码格式入库，改编码采用如下指令：

        $SPARK_HOME/bin/spark-submit --master local[2] --class yanghb.sparktools.SparkTools /root/yanghb/lib/spark-tools-1.0.jar \
         loadFS2JDBC "jdbc:mysql://172.31.105.10:3306/company_profile_develop?user=develop&password=develop&jdbcCompliantTruncation=false&characterEncoding=gb2312" \
         customer_test file:/root/zengzc/table_data/customer_20160719
