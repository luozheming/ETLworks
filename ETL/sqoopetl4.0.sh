#!/bin/bash
#@Author  : luozheming
#@Contact : lzm931105@cmbchina.com
#@File    : sqoopETL.sh
#@Time    : 2020/2/20 15:16
##-----------------------------------------------------------------------------------------
##--参    数：tableNameOrg：源表名（分库分表情况下去除_num部分）   mode：全量或者增量  可选值：append/full
##--功    能：2.0：增加对于数据类型的判断，支持oracle的全量/增量抽取   3.0:进行了增量部分的测试，添加了空值的处理以及全量表更新前的删除操作
##--版    本：3.0
##-----------------------------------------------------------------------------------------
PATH=/bin:/sbin:/user/bin:user/sbin:user/local/bin:/user/local/sbin:~/bin:/usr/local/mysql/bin
export PATH
#异常退出机制
set -u
#输入判断
if [ $# -eq 2 ]&&[ $2 == "full" ]||[ $2 == "append" ]
then :
else
  echo "!!!LOG_INFO:参数个数或类型不正确，请查询脚本代码检查输入"
  exit 1;
fi

#表名称
tableNameOrg=$1
#全量/增量模式
mode=$2

#静态变量
#数据库配置表信息
db_ip="55.14.58.23"
db_port="3306"
db_user="cmbcckg"
db_pwd="Eccdg%2015"
db_config_db="cckg01"
db_config_table="sqoop_etl_conf"
db_log_table="sqoop_etl_logs"

#增量当前时间
append_time=`date -d "-1 day" "+%Y-%m-%d %H:%M:%S"`

#全量当前时间
full_time=`date "+%Y%m%d"`

# 从数据库获取配置信息
server_info=`mysql -h${db_ip} -P${db_port} -u${db_user} -p${db_pwd} --skip-column-names <<EOF
select concat(source,'|',conn_db_db,'|',conn_db_ip,'|',conn_db_port,'|',conn_db_user,'|',conn_db_passwd,'|',is_split,'|',src_dbname,'|',db_start_num,'|',db_end_num,'|',src_tablename,'|',table_start_num,'|',table_end_num,'|',sqoop_user,'|',sqoop_psswd,'|',hive_db_dir,'|',sqoop_fields_terminated,'|',sqoop_lines_terminated,'|',sqoop_incr_col,'|',sqoop_pt1,'|',sqoop_pt2,'|',last_update_time,'|',sid)  from ${db_config_db}.${db_config_table} where  job_name='${tableNameOrg}';
EOF`

#记录日志所用到的函数(参数：调研行数|表真实行数|sqoop并行数|成功失败标志|日志详情)
function record_logs(){
  mysql -h${db_ip} -P${db_port} -u${db_user} -p${db_pwd}<<EOF
  insert into ${db_config_db}.${db_log_table}(job_name,src_dbname,src_tablename,start_time,end_time,use_time,success_flag,mode) values('${tableNameOrg}','${src_dbname}','${src_tablename}','${1}','${2}',SEC_TO_TIME(UNIX_TIMESTAMP('${2}')-UNIX_TIMESTAMP('${1}')),'${3}','${mode}');
EOF
}

echo $server_info
# 初始化配置信息
source=`echo ${server_info} | awk -F "|" '{print $1}'`
conn_db_db=`echo ${server_info} | awk -F "|" '{print $2}'`
conn_db_ip=`echo ${server_info} | awk -F "|" '{print $3}'`
conn_db_port=`echo ${server_info} | awk -F "|" '{print $4}'`
conn_db_user=`echo ${server_info} | awk -F "|" '{print $5}'`
conn_db_passwd=`echo ${server_info} | awk -F "|" '{print $6}'`
is_split=`echo ${server_info} | awk -F "|" '{print $7}'`
src_dbname=`echo ${server_info} | awk -F "|" '{print $8}'`
db_start_num=`echo ${server_info} | awk -F "|" '{print $9}'`
db_end_num=`echo ${server_info} | awk -F "|" '{print $10}'`
src_tablename=`echo ${server_info} | awk -F "|" '{print $11}'`
table_start_num=`echo ${server_info} | awk -F "|" '{print $12}'`
table_end_num=`echo ${server_info} | awk -F "|" '{print $13}'`
sqoop_user=`echo ${server_info} | awk -F "|" '{print $14}'`
sqoop_psswd=`echo ${server_info} | awk -F "|" '{print $15}'`
hive_db_dir=`echo ${server_info} | awk -F "|" '{print $16}'`
sqoop_fields_terminated=`echo ${server_info} | awk -F "|" '{print $17}'`
sqoop_lines_terminated=`echo ${server_info} | awk -F "|" '{print $18}'`
sqoop_incr_col=`echo ${server_info} | awk -F "|" '{print $19}'`
sqoop_pt1=`echo ${server_info} | awk -F "|" '{print $20}'`
sqoop_pt2=`echo ${server_info} | awk -F "|" '{print $21}'`
last_update_time=`echo ${server_info} | awk -F "|" '{print $22}'`
sid=`echo ${server_info} | awk -F "|" '{print $23}'`




#首先判断数据库类型：
case $conn_db_db in
"mysql")
    echo "database is mysql"
    if [ "$mode" == "append" ]; then
        #append mode
        echo "append mode"
        start_time=`date "+%Y-%m-%d %H:%M:%S"`
        #计算增量的分批批次：当前小时/执行间隔
        append_hour=`date "+%H"`
        append_num=`expr $append_hour / 4`
        #判断是否分库分表,注意增量导数之后需要beeline命令修复分区信息
        if [ "$is_split" == "True" ] && [ $db_start_num == $db_end_num ];then
            dbName=$src_dbname
            for((j=$table_start_num;j<=$table_end_num;j=j+1))
            do
                tableName=$src_tablename"_"$j
                ssh root@sqoopserver <<endssh
                export HADOOP_USER_NAME=qxb_sqoop
                sqoop import \
                -D mapred.job.name="job_${tableNameOrg}_${j}_append" \
                --connect jdbc:mysql://${conn_db_ip}:${conn_db_port}/$dbName \
                --username ${conn_db_user} --password ${conn_db_passwd}  \
                --table $tableName \
                --split-by "ins_time" \
                --fields-terminated-by  "$sqoop_fields_terminated" \
                --lines-terminated-by  "$sqoop_lines_terminated" \
                --hive-drop-import-delims \
                --input-null-string '\\\N' \
                --input-null-non-string '\\\N' \
                --target-dir ${hive_db_dir}/$tableNameOrg/$sqoop_pt1$j/$sqoop_pt2$full_time"_"$append_num \
                --incremental lastmodified \
                --check-column ${sqoop_incr_col} \
                --last-value "$append_time"
                beeline -u jdbc:hive2://localhost:10000 -n qxb_sqoop -p qxb_sqoop -e "msck repair table stg.$tableNameOrg"
                exit
endssh
            done
        elif [ "$is_split" == "True" ];then
            for((i=$db_start_num;i<=$db_end_num;i=i+1))
            do
                dbName=$src_dbname"_"$i
                for((j=$table_start_num;j<=$table_end_num;j=j+1))
                do
                    tableName=$src_tablename"_"$j
                    ssh root@sqoopserver <<endssh
                    export HADOOP_USER_NAME=qxb_sqoop
                    sqoop import \
                    -D mapred.job.name="job_${tableNameOrg}_${i}${j}_append" \
                    --connect jdbc:mysql://${conn_db_ip}:${conn_db_port}/$dbName \
                    --username ${conn_db_user} --password ${conn_db_passwd}  \
                    --table $tableName \
                    --split-by "ins_time" \
                    --fields-terminated-by  "$sqoop_fields_terminated" \
                    --lines-terminated-by  "$sqoop_lines_terminated" \
                    --hive-drop-import-delims \
                    --input-null-string '\\\N' \
                    --input-null-non-string '\\\N' \
                    --target-dir ${hive_db_dir}/$tableNameOrg/$sqoop_pt1$i$j/$sqoop_pt2$full_time"_"$append_num \
                    --incremental lastmodified \
                    --check-column ${sqoop_incr_col} \
                    --last-value "$append_time"
                    beeline -u jdbc:hive2://localhost:10000 -n qxb_sqoop -p qxb_sqoop -e "msck repair table stg.$tableNameOrg"
                    exit
endssh
                done
            done
        #非分库分表的情况,此时分区字段只有partition_time
        else
            ssh root@sqoopserver <<endssh
            export HADOOP_USER_NAME=qxb_sqoop
            sqoop import \
            -D mapred.job.name="job_${tableNameOrg}_append" \
            --connect jdbc:mysql://${conn_db_ip}:${conn_db_port}/$src_dbname \
            --username ${conn_db_user} \
            --password ${conn_db_passwd}  \
            --table $src_tablename \
            --split-by "ins_time" \
            --fields-terminated-by  "$sqoop_fields_terminated" \
            --lines-terminated-by  "$sqoop_lines_terminated" \
            --input-null-string '\\\N' \
            --input-null-non-string '\\\N' \
            --hive-drop-import-delims \
            --target-dir ${hive_db_dir}/$tableNameOrg/$sqoop_pt1$full_time"_"$append_num \
            --incremental lastmodified \
            --check-column ${sqoop_incr_col} \
            --last-value "$append_time"
            beeline -u jdbc:hive2://localhost:10000 -n qxb_sqoop -p qxb_sqoop -e "msck repair table stg.$tableNameOrg"
            exit
endssh
        success_flag=$?
        end_time=`date "+%Y-%m-%d %H:%M:%S"`
        record_logs "$start_time" "$end_time" "$success_flag"
        fi
    else
        # full mode
        echo "full mode"
        start_time=`date "+%Y-%m-%d %H:%M:%S"`
        # 全量更新前，删除对应所有的数据文件,需要先检查文件夹下是否存在目录（针对第一次执行）
        dataFile=`ssh root@sqoopserver "export HADOOP_USER_NAME=qxb_sqoop;hadoop fs -ls ${hive_db_dir}/$tableNameOrg/"`
        if [ "$dataFile" != "" ];then
            ssh root@sqoopserver <<endssh
            export HADOOP_USER_NAME=qxb_sqoop
            hadoop fs -rm -r ${hive_db_dir}/$tableNameOrg/*
endssh
        fi
        # 判断是否分库分表
        if [ "$is_split" == "True" ] && [ $db_start_num == $db_end_num ];then
            dbName=$src_dbname
            for((j=$table_start_num;j<=$table_end_num;j=j+1))
            do
                tableName=$src_tablename"_"$j
                ssh root@sqoopserver <<endssh
                export HADOOP_USER_NAME=qxb_sqoop
                sqoop import \
                -D mapred.job.name="job_${tableNameOrg}_${j}_full" \
                --connect jdbc:mysql://${conn_db_ip}:${conn_db_port}/$dbName --username ${conn_db_user} --password ${conn_db_passwd} \
                --table $tableName \
                --split-by "ins_time" \
                --fields-terminated-by  "$sqoop_fields_terminated" \
                --lines-terminated-by  "$sqoop_lines_terminated" \
                --input-null-string '\\\N' \
                --input-null-non-string '\\\N' \
                --hive-drop-import-delims \
                --target-dir ${hive_db_dir}/$tableNameOrg/$sqoop_pt1$j/$sqoop_pt2$full_time
                beeline -u jdbc:hive2://localhost:10000 -n qxb_sqoop -p qxb_sqoop -e "msck repair table stg.$tableNameOrg"
                exit
endssh
            done
        elif [ "$is_split" == "True" ];then
            for((i=$db_start_num;i<=$db_end_num;i=i+1))
            do
                dbName=$src_dbname"_"$i
                for((j=$table_start_num;j<=$table_end_num;j=j+1))
                do
                    tableName=$src_tablename"_"$j
                    ssh root@sqoopserver <<endssh
                    export HADOOP_USER_NAME=qxb_sqoop
                    sqoop import \
                    -D mapred.job.name="job_${tableNameOrg}_${i}${j}_full" \
                    --connect jdbc:mysql://${conn_db_ip}:${conn_db_port}/$dbName --username ${conn_db_user} --password ${conn_db_passwd} \
                    --table $tableName \
                    --split-by "ins_time" \
                    --fields-terminated-by  "$sqoop_fields_terminated" \
                    --lines-terminated-by  "$sqoop_lines_terminated" \
                    --input-null-string '\\\N' \
                    --input-null-non-string '\\\N' \
                    --hive-drop-import-delims \
                    --target-dir ${hive_db_dir}/$tableNameOrg/$sqoop_pt1$i$j/$sqoop_pt2$full_time
                    beeline -u jdbc:hive2://localhost:10000 -n qxb_sqoop -p qxb_sqoop -e "msck repair table stg.$tableNameOrg"
                    exit
endssh
                done
            done
        #非分库分表的情况,此时分区字段只有partition_time
        else
            ssh root@sqoopserver <<endssh
            export HADOOP_USER_NAME=qxb_sqoop
            sqoop import \
            -D mapred.job.name="job_${tableNameOrg}_full" \
            --connect jdbc:mysql://${conn_db_ip}:${conn_db_port}/$src_dbname \
            --username ${conn_db_user} --password ${conn_db_passwd}  \
            --table $src_tablename \
            --fields-terminated-by  "$sqoop_fields_terminated" \
            --split-by "ins_time" \
            --lines-terminated-by  "$sqoop_lines_terminated" \
            --hive-drop-import-delims \
            --input-null-string '\\\N' \
            --input-null-non-string '\\\N' \
            --target-dir ${hive_db_dir}/$tableNameOrg/$sqoop_pt1$full_time
            beeline -u jdbc:hive2://localhost:10000 -n qxb_sqoop -p qxb_sqoop -e "msck repair table stg.$tableNameOrg"
            exit
endssh
        fi
        success_flag=$?
        end_time=`date "+%Y-%m-%d %H:%M:%S"`
        record_logs "$start_time" "$end_time" "$success_flag"
    fi;;
#注意oracle不存在分库分表
"oracle")
    echo "database is oracle"
    if [ "$mode" == "append" ]; then
        echo "mode append"
        start_time=`date "+%Y-%m-%d %H:%M:%S"`
        #计算增量的分批批次：当前小时/执行间隔
        append_hour=`date "+%H"`
        append_num=`expr $append_hour / 4`
        ssh root@sqoopserver <<endssh
        export HADOOP_USER_NAME=qxb_sqoop
        sqoop import \
        -D mapred.job.name="job_${tableNameOrg}_append" \
        --connect jdbc:oracle:thin:@${conn_db_ip}:${conn_db_port}:${sid} \
        --username ${conn_db_user} \
        --password ${conn_db_passwd}  \
        --table $src_dbname.$src_tablename \
        --split-by "ins_time" \
        --fields-terminated-by  "$sqoop_fields_terminated" \
        --lines-terminated-by  "$sqoop_lines_terminated" \
        --input-null-string '\\\N' \
        --input-null-non-string '\\\N' \
        --incremental lastmodified \
        --check-column ${sqoop_incr_col} \
        --last-value "$append_time" \
        --hive-drop-import-delims \
        --target-dir ${hive_db_dir}/$tableNameOrg/$sqoop_pt1$full_time"_"$append_num
        beeline -u jdbc:hive2://localhost:10000 -n qxb_sqoop -p qxb_sqoop -e "msck repair table stg.$tableNameOrg"
endssh
        success_flag=$?
        end_time=`date "+%Y-%m-%d %H:%M:%S"`
        record_logs "$start_time" "$end_time" "$success_flag"
    else
        # 全量更新前，删除对应所有的数据文件
        echo "full mode"
        start_time=`date "+%Y-%m-%d %H:%M:%S"`
        dataFile=`ssh root@sqoopserver "export HADOOP_USER_NAME=qxb_sqoop;hadoop fs -ls ${hive_db_dir}/$tableNameOrg/"`
        if [ "$dataFile" != "" ];then
            ssh root@sqoopserver <<endssh
            export HADOOP_USER_NAME=qxb_sqoop
            hadoop fs -rm -r ${hive_db_dir}/$tableNameOrg/*
endssh
        fi
        ssh root@sqoopserver <<endssh
        export HADOOP_USER_NAME=qxb_sqoop
        sqoop import \
        -D mapred.job.name="job_${tableNameOrg}_full" \
        --connect jdbc:oracle:thin:@${conn_db_ip}:${conn_db_port}:${sid} \
        --username ${conn_db_user} \
        --password ${conn_db_passwd}  \
        --table $src_dbname.$src_tablename \
        --split-by "ins_time" \
        --fields-terminated-by  "$sqoop_fields_terminated" \
        --lines-terminated-by  "$sqoop_lines_terminated"  \
        --hive-drop-import-delims \
        --input-null-string '\\\N' \
        --input-null-non-string '\\\N' \
        --target-dir ${hive_db_dir}/$tableNameOrg/$sqoop_pt1$full_time
        beeline -u jdbc:hive2://localhost:10000 -n qxb_sqoop -p qxb_sqoop -e "msck repair table stg.$tableNameOrg"
        exit
endssh
        success_flag=$?
        end_time=`date "+%Y-%m-%d %H:%M:%S"`
        record_logs "$start_time" "$end_time" "$success_flag"
    fi;;
esac

