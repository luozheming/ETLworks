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
PATH=/bin:/sbin:/user/bin:user/sbin:user/local/bin:/user/local/sbin:~/bin:/usr/local/mysql/bin:/home/oracle/app/oracle/product/11.2.0/dbhome_1/bin/
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
full_time=`date "+%Y-%m-%d"`

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

#检查hive/mysql/Oracle的数据总数核对
function check_sum(){
hive_count=`ssh root@sqoopserver "beeline -u jdbc:hive2://localhost:10000 -n qxb_sqoop -p qxb_sqoop --outputformat=tsv2 --silent=true --showHeader=false --showNestedErrs=false -e 'select count(*) from stg.${tableNameOrg}'"`
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

result=$(check_sum)
echo $result
