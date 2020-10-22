#!/bin/bash
#@Author  : luozheming
#@Contact :
#@File    : sqoopETL.sh
#@Time    : 2020/2/20 15:16
##-----------------------------------------------------------------------------------------
##--参    数：tableNameOrg：源表名（分库分表情况下去除_num部分）   mode：全量或者增量  可选值：append/full
##--功    能：2.0：增加对于数据类型的判断，支持oracle的全量/增量抽取   3.0:进行了增量部分的测试，添加了空值的处理以及全量表更新前的删除操作
## 5.0:1.分库分表或不分库分表写法改成一次抽一个库，对应分库表16个库分区
#2.全量抽取也需要增加时间限制，时间小于当天00：00：00
#3.数量统计增加时间限制，时间小于当天00：00：00
#4.增加任务提交的hiveserver的参数化配置
#5.时间分区写成前一天，T-1
##--版    本：5.0
##-----------------------------------------------------------------------------------------
PATH=/bin:/sbin:/user/bin:user/sbin:user/local/bin:/user/local/sbin:~/bin:/usr/local/mysql/bin:/home/oracle/app/oracle/product/11.2.0/dbhome_1/bin/
export PATH
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
db_ip=""
db_port=""
db_user=""
db_pwd=""
db_config_db=""
db_config_table=""
db_log_table=""

#增量当前时间
append_time=`date -d "-1 day" "+%Y-%m-%d 00:00:00"`
append_time_now=`date "+%Y-%m-%d 00:00:00"`

#全量当前时间(版本5.0修改为前一天)
full_time=`date -d "-1 day" "+%Y%m%d"`

# 从数据库获取配置信息
server_info=`mysql -h${db_ip} -P${db_port} -u${db_user} -p${db_pwd} --skip-column-names <<EOF
select concat(source,'|',conn_db_db,'|',conn_db_ip,'|',conn_db_port,'|',conn_db_user,'|',conn_db_passwd,'|',is_split,'|',src_dbname,'|',db_start_num,'|',db_end_num,'|',src_tablename,'|',table_start_num,'|',table_end_num,'|',sqoop_user,'|',sqoop_psswd,'|',hive_db_dir,'|',sqoop_fields_terminated,'|',sqoop_lines_terminated,'|',sqoop_incr_col,'|',sqoop_pt1,'|',sqoop_pt2,'|',last_update_time,'|',sid,'|',hive_server)  from ${db_config_db}.${db_config_table} where  job_name='${tableNameOrg}';
EOF`

#记录日志所用到的函数(参数：调研行数|表真实行数|sqoop并行数|成功失败标志|日志详情)
function record_logs(){
  mysql -h${db_ip} -P${db_port} -u${db_user} -p${db_pwd}<<EOF
  insert into ${db_config_db}.${db_log_table}(job_name,src_dbname,src_tablename,start_time,end_time,use_time,success_flag,mode,check_sum) values('${tableNameOrg}','${src_dbname}','${src_tablename}','${1}','${2}',SEC_TO_TIME(UNIX_TIMESTAMP('${2}')-UNIX_TIMESTAMP('${1}')),'${3}','${mode}','${4}');
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
hive_server=`echo ${server_info} | awk -F "|" '{print $24}'`


#检查hive/mysql/Oracle的数据总数核对
function check_sum(){
hive_count=`ssh root@${hive_server} "beeline -u jdbc:hive2://localhost:10000 -n qxb_sqoop -p qxb_sqoop --outputformat=tsv2 --silent=true --showHeader=false --showNestedErrs=false -e 'select count(*) from stg.${tableNameOrg}'"`
case $conn_db_db in
"mysql")
    if [ "$is_split" == "True" ] && [ $db_start_num == $db_end_num ];then
        dbName=$src_dbname
        count_sum=0
        for((j=$table_start_num;j<=$table_end_num;j=j+1))
        do
          tableName=$src_tablename"_"$j
          temp_sum=`mysql -h${conn_db_ip} -P${conn_db_port} -u${conn_db_user} -p${conn_db_passwd} --skip-column-names<<EOF
          select count(1) from ${dbName}.${tableName}
EOF`
        count_sum=$[$count_sum+$temp_sum]
        done
    elif [ "$is_split" == "True" ];then
        count_sum=0
        for((i=$db_start_num;i<=$db_end_num;i=i+1))
        do
            dbName=$src_dbname"_"$i
            for((j=$table_start_num;j<=$table_end_num;j=j+1))
            do
                tableName=$src_tablename"_"$j
                temp_sum=`mysql -h${conn_db_ip} -P${conn_db_port} -u${conn_db_user} -p${conn_db_passwd} --skip-column-names<<EOF
                select count(1) from ${dbName}.${tableName}
EOF`
                count_sum=$[$count_sum+$temp_sum]
            done
        done
    else
        dbName=$src_dbname
        count_sum=0
        temp_sum=`mysql -h${conn_db_ip} -P${conn_db_port} -u${conn_db_user} -p${conn_db_passwd} --skip-column-names<<EOF
                select count(1) from ${dbName}.${src_tablename}
EOF`
        count_sum=$[$count_sum+$temp_sum]
    fi;;
"oracle")
    count_sum=`sqlplus -S ${conn_db_user}/${conn_db_passwd}@//${conn_db_ip}:${conn_db_port}/${sid} <<EOF
    set heading off;
    set trimspool on;
    set trimout on;
    set feedback off;
    select count(1) from ${src_dbname}.${src_tablename};
EOF`
;;
esac
#判断数据库总行数和hive行数是否相等
if [ $count_sum == $hive_count ];then
    echo "true"
else
    echo "false"
fi
}



function generateallmodeSQL(){
    outputsql=""
    case ${mode} in
    "full")
        condition='where \$CONDITIONS'" and ${sqoop_incr_col}<='${append_time}'";;
    "append")
        condition='where \$CONDITIONS'" and ${sqoop_incr_col}<='${append_time_now}' and ${sqoop_incr_col}>='${append_time}'";;
    esac
    #分库/分表的情况
    if [ "$is_split" == "True" ];then
        for((j=$table_start_num;j<=$table_end_num;j=j+1))
        do
            tableName=$src_tablename"_"$j
            outputsql+="select * from ${1}.${tableName} ${condition}"
            #使用union all将分表接连
            if [ $j != $table_end_num ];then
                outputsql+=" union all "
            fi
        done
    #非分库分表的情况
    else
        outputsql+="select * from ${src_tablename}.${src_tablename} ${condition}"
    fi
    echo "$outputsql"
}



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
            sql=`generateallmodeSQL "${dbName}"`
            ssh root@${hive_server} <<endssh
            export HADOOP_USER_NAME=qxb_sqoop
            sqoop import \
            -D mapred.job.name="job_${tableNameOrg}_append" \
            --connect jdbc:mysql://${conn_db_ip}:${conn_db_port}/$dbName?zeroDateTimeBehavior=convertToNull \
            --username ${conn_db_user} --password ${conn_db_passwd}  \
            --fields-terminated-by  "$sqoop_fields_terminated" \
            --lines-terminated-by  "$sqoop_lines_terminated" \
            --split-by "ins_time" \
            --hive-drop-import-delims \
            --input-null-string '\\\N' \
            --input-null-non-string '\\\N' \
            --target-dir ${hive_db_dir}/$tableNameOrg/$sqoop_pt2$full_time"_"$append_num \
            --query "${sql}"
            exit
endssh
        #分库分表
        elif [ "$is_split" == "True" ];then
            for((i=$db_start_num;i<=$db_end_num;i=i+1))
            do
                dbName=$src_dbname"_"$i
                sql=`generateallmodeSQL "${dbName}"`
                ssh root@${hive_server} <<endssh
                export HADOOP_USER_NAME=qxb_sqoop
                sqoop import \
                -D mapred.job.name="job_${tableNameOrg}_${i}_append" \
                --connect jdbc:mysql://${conn_db_ip}:${conn_db_port}/$dbName?zeroDateTimeBehavior=convertToNull \
                --username ${conn_db_user} --password ${conn_db_passwd}  \
                --fields-terminated-by  "$sqoop_fields_terminated" \
                --lines-terminated-by  "$sqoop_lines_terminated" \
                --split-by "ins_time" \
                --hive-drop-import-delims \
                --input-null-string '\\\N' \
                --input-null-non-string '\\\N' \
                --target-dir ${hive_db_dir}/$tableNameOrg/$sqoop_pt1${i}/$sqoop_pt2$full_time"_"$append_num \
                --query "${sql}"
                exit
endssh
                done
        #非分库分表的情况,此时分区字段只有partition_time,注意此处不采用函数生成sql
        else
            ssh root@${hive_server} <<endssh
            export HADOOP_USER_NAME=qxb_sqoop
            sqoop import \
            -D mapred.job.name="job_${tableNameOrg}_append" \
            --connect jdbc:mysql://${conn_db_ip}:${conn_db_port}/$src_dbname \
            --username ${conn_db_user} \
            --password ${conn_db_passwd}  \
            --table $src_tablename \
            --fields-terminated-by  "$sqoop_fields_terminated" \
            --lines-terminated-by  "$sqoop_lines_terminated" \
            --split-by "ins_time" \
            --input-null-string '\\\N' \
            --input-null-non-string '\\\N' \
            --hive-drop-import-delims \
            --target-dir ${hive_db_dir}/$tableNameOrg/$sqoop_pt1$full_time"_"$append_num \
            --where '${sqoop_incr_col}>="${append_time}" and ${sqoop_incr_col}<="${append_time_now}"'
            exit
endssh
#判断mysql-append的情况是否成功
        fi
        success_flag=$?
    else
        # full mode
        echo "full mode"
        start_time=`date "+%Y-%m-%d %H:%M:%S"`
        # 全量更新前，删除对应所有的数据文件,需要先检查文件夹下是否存在目录（针对第一次执行）
        dataFile=`ssh root@${hive_server} "export HADOOP_USER_NAME=qxb_sqoop;hadoop fs -ls ${hive_db_dir}/$tableNameOrg/"`
        if [ "$dataFile" != "" ];then
            ssh root@sqoopserver <<endssh
            export HADOOP_USER_NAME=qxb_sqoop
            hadoop fs -rm -r ${hive_db_dir}/$tableNameOrg/*
endssh
        fi
        # 判断是否分库分表
        if [ "$is_split" == "True" ] && [ $db_start_num == $db_end_num ];then
            dbName=$src_dbname
            tableName=$src_tablename
            sql=`generateallmodeSQL "${dbName}"`
            ssh root@${hive_server} <<endssh
            export HADOOP_USER_NAME=qxb_sqoop
            sqoop import \
            -D mapred.job.name="job_${tableNameOrg}_full" \
            --connect jdbc:mysql://${conn_db_ip}:${conn_db_port}/$dbName?zeroDateTimeBehavior=convertToNull  --username ${conn_db_user} --password ${conn_db_passwd} \
            --fields-terminated-by  "$sqoop_fields_terminated" \
            --lines-terminated-by  "$sqoop_lines_terminated" \
            --split-by "ins_time" \
            --input-null-string '\\\N' \
            --input-null-non-string '\\\N' \
            --hive-drop-import-delims \
            --target-dir ${hive_db_dir}/$tableNameOrg/$sqoop_pt2$full_time \
            --query "${sql}"
            exit
endssh

        elif [ "$is_split" == "True" ];then
            for((i=$db_start_num;i<=$db_end_num;i=i+1))
            do
                dbName=$src_dbname"_"$i
                sql=`generateallmodeSQL ${dbName}`
                ssh root@${hive_server} <<endssh
                export HADOOP_USER_NAME=qxb_sqoop
                sqoop import \
                -D mapred.job.name="job_${tableNameOrg}_${i}_full" \
                --connect jdbc:mysql://${conn_db_ip}:${conn_db_port}/$dbName?zeroDateTimeBehavior=convertToNull --username ${conn_db_user} --password ${conn_db_passwd} \
                --fields-terminated-by  "$sqoop_fields_terminated" \
                --lines-terminated-by  "$sqoop_lines_terminated" \
                --split-by "ins_time" \
                --input-null-string '\\\N' \
                --input-null-non-string '\\\N' \
                --hive-drop-import-delims \
                --target-dir ${hive_db_dir}/$tableNameOrg/$sqoop_pt1${i}/$sqoop_pt2$full_time \
                --query "${sql}"
                exit
endssh
                done
        #非分库分表的情况,此时分区字段只有partition_time,注意此处不采用sql
        else
            ssh root@${hive_server} <<endssh
            export HADOOP_USER_NAME=qxb_sqoop
            sqoop import \
            -D mapred.job.name="job_${tableNameOrg}_full" \
            --connect jdbc:mysql://${conn_db_ip}:${conn_db_port}/$src_dbname \
            --username ${conn_db_user} --password ${conn_db_passwd}  \
            --table $src_tablename \
            --fields-terminated-by  "$sqoop_fields_terminated" \
            --lines-terminated-by  "$sqoop_lines_terminated" \
            --split-by "ins_time" \
            --hive-drop-import-delims \
            --input-null-string '\\\N' \
            --input-null-non-string '\\\N' \
            --target-dir ${hive_db_dir}/$tableNameOrg/$sqoop_pt1$full_time \
            --where '${sqoop_incr_col}<="${append_time_now}"'
            exit
endssh
        fi
#判断mysql-full的情况是否成功
        success_flag=$?
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
        ssh root@${hive_server} <<endssh
        export HADOOP_USER_NAME=qxb_sqoop
        sqoop import \
        -D mapred.job.name="job_${tableNameOrg}_append" \
        --connect jdbc:oracle:thin:@${conn_db_ip}:${conn_db_port}:${sid} \
        --username ${conn_db_user} \
        --password ${conn_db_passwd}  \
        --table $src_dbname.$src_tablename \
        --fields-terminated-by  "$sqoop_fields_terminated" \
        --lines-terminated-by  "$sqoop_lines_terminated" \
        --input-null-string '\\\N' \
        --input-null-non-string '\\\N' \
        --where "${sqoop_incr_col}>=to_date('${append_time}','yyyy-mm-dd hh24:mi:ss') and ${sqoop_incr_col}<=to_date('${append_time_now}','yyyy-mm-dd hh24:mi:ss')" \
        --hive-drop-import-delims \
        --target-dir ${hive_db_dir}/$tableNameOrg/$sqoop_pt1$full_time"_"$append_num
endssh
        success_flag=$?
    else
        # 全量更新前，删除对应所有的数据文件
        echo "full mode"
        start_time=`date "+%Y-%m-%d %H:%M:%S"`
        dataFile=`ssh root@sqoopserver "export HADOOP_USER_NAME=qxb_sqoop;hadoop fs -ls ${hive_db_dir}/$tableNameOrg/"`
        if [ "$dataFile" != "" ];then
            ssh root@${hive_server} <<endssh
            export HADOOP_USER_NAME=qxb_sqoop
            hadoop fs -rm -r ${hive_db_dir}/$tableNameOrg/*
endssh
        fi
        ssh root@${hive_server} <<endssh
        export HADOOP_USER_NAME=qxb_sqoop
        sqoop import \
        -D mapred.job.name="job_${tableNameOrg}_full" \
        --connect jdbc:oracle:thin:@${conn_db_ip}:${conn_db_port}:${sid} \
        --username ${conn_db_user} \
        --password ${conn_db_passwd}  \
        --table $src_dbname.$src_tablename \
        --fields-terminated-by  "$sqoop_fields_terminated" \
        --lines-terminated-by  "$sqoop_lines_terminated"  \
        --hive-drop-import-delims \
        --input-null-string '\\\N' \
        --input-null-non-string '\\\N' \
        --target-dir ${hive_db_dir}/$tableNameOrg/$sqoop_pt1$full_time
        exit
endssh
        success_flag=$?
    fi;;
esac

#不论成功与否，修复分区
ssh root@${hive_server} <<endssh
export HADOOP_USER_NAME=qxb_sqoop
beeline -u jdbc:hive2://localhost:10000 -n qxb_sqoop -p qxb_sqoop -e "msck repair table stg.$tableNameOrg"
exit
endssh
end_time=`date "+%Y-%m-%d %H:%M:%S"`
record_logs "$start_time" "$end_time" "$success_flag" "$(check_sum)"
if [ "$success_flag" == "1" ];then
  exit 1
fi
