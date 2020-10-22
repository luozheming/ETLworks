#!/bin/bash
#@Author  : luozheming
#@Contact : lzm931105@cmbchina.com
#@File    : huaweiETL.sh
#@Time    : 2020/3/23 10:16
##-----------------------------------------------------------------------------------------
##--参    数：tableNameOrg：源表名（分库分表情况下去除_num部分)
##--功    能：1.0：用于抽取华为Fushion Insight大数据集群的数据到CDH
##--版    本：1.0
##-----------------------------------------------------------------------------------------
PATH=/bin:/sbin:/user/bin:user/sbin:user/local/bin:/user/local/sbin:~/bin:/usr/local/mysql/bin:/home/oracle/app/oracle/product/11.2.0/dbhome_1/bin/:/opt/hadoop_client/KrbClient/kerberos/bin/:/opt/hadoop_client/HDFS/hadoop/bin/
export PATH

#输入判断
if [ $# -eq 1 ]
then :
else
  echo "!!!LOG_INFO:参数个数或类型不正确，请查询脚本代码检查输入"
  exit 1;
fi

#表名称
tableNameOrg=$1

#静态变量
#数据库配置表信息
db_ip="55.14.58.23"
db_port="3306"
db_user="cmbcckg"
db_pwd="Eccdg%2015"
db_config_db="cckg01"
db_config_table="hdfs_etl_conf"
db_log_table="sqoop_etl_logs"
#抽取开始时间
start_time=`date "+%Y-%m-%d %H:%M:%S"`
#全量当前时间
full_time=`date -d "-1 day" "+%Y%m%d"`
#从SFTP暂且保存在本地的路径
tmp_save_path="/tmp"


#记录日志所用到的函数(参数：调研行数|表真实行数|sqoop并行数|成功失败标志|日志详情)
function record_logs(){
  mysql -h${db_ip} -P${db_port} -u${db_user} -p${db_pwd}<<EOF
  insert into ${db_config_db}.${db_log_table}(job_name,src_dbname,src_tablename,start_time,end_time,use_time,success_flag,mode) values('${tableNameOrg}','HDFS','${tableNameOrg}','${1}','${2}',SEC_TO_TIME(UNIX_TIMESTAMP('${2}')-UNIX_TIMESTAMP('${1}')),'${3}','HDFS');
EOF
}

server_info=`mysql -h${db_ip} -P${db_port} -u${db_user} -p${db_pwd} --skip-column-names <<EOF
select concat(source,'|',job_name,'|',fstp_ip,'|',fstp_user,'|',fstp_passwd,'|',hdfs_user,'|',hdfs_passwd,'|',hdfs_dir,'|',data_filename,'|',ctl_filename,'|',hive_db_dir,'|',file_format,'|',fields_terminated,'|',vld_ind,'|',refresh_delta,'|',last_update_time,'|',sqoop_pt1,'|',para1) from ${db_config_db}.${db_config_table} where job_name='${tableNameOrg}';
EOF`

echo $server_info
source=`echo ${server_info} | awk -F "|" '{print $1}'`
job_name=`echo ${server_info} | awk -F "|" '{print $2}'`
fstp_ip=`echo ${server_info} | awk -F "|" '{print $3}'`
fstp_user=`echo ${server_info} | awk -F "|" '{print $4}'`
fstp_passwd=`echo ${server_info} | awk -F "|" '{print $5}'`
hdfs_user=`echo ${server_info} | awk -F "|" '{print $6}'`
hdfs_passwd=`echo ${server_info} | awk -F "|" '{print $7}'`
hdfs_dir=`echo ${server_info} | awk -F "|" '{print $8}'`
data_filename=`echo ${server_info} | awk -F "|" '{print $9}'`
ctl_filename=`echo ${server_info} | awk -F "|" '{print $10}'`
hive_db_dir=`echo ${server_info} | awk -F "|" '{print $11}'`
file_format=`echo ${server_info} | awk -F "|" '{print $12}'`
fields_terminated=`echo ${server_info} | awk -F "|" '{print $13}'`
vld_ind=`echo ${server_info} | awk -F "|" '{print $14}'`
refresh_delta=`echo ${server_info} | awk -F "|" '{print $15}'`
last_update_time=`echo ${server_info} | awk -F "|" '{print $16}'`
sqoop_pt1=`echo ${server_info} | awk -F "|" '{print $17}'`
hive_table_name=`echo ${server_info} | awk -F "|" '{print $18}'`


#查看目标文件是否存在
success_flag=1
todaytagetfile=`ssh root@sqoopserver "export HADOOP_USER_NAME=qxb_sqoop;hadoop fs -test -e ${hive_db_dir}/${sqoop_pt1}${full_time}/${data_filename}"`

#进入到交互模式
if [ $? -eq 1 ];then
    #大数据集群环境变量初始化操作
    kinit -kt ${hdfs_passwd} ${hdfs_user}
    #查看数仓文件是否存在
    hadoop fs -test -e ${hdfs_dir}/${full_time}/${data_filename}
    if [ $? -eq 0 ];then
        echo "download file from huawei FI"
        hdfs dfs -get ${hdfs_dir}/${full_time}/${data_filename} ${tmp_save_path}
        ssh root@sqoopserver<<endssh
        export HADOOP_USER_NAME=qxb_sqoop
        expect<<EOF
        spawn sftp ${fstp_user}@${fstp_ip}
        expect {
            -re "(.?)assword:"  { send "${fstp_passwd}\r"; exp_continue}
            -re "sftp" { send "get -r ${tmp_save_path}/${data_filename} ${tmp_save_path}\r";}
            eof{
                send_error "EOF...exit";
                exit
            } timeout 10 {
                send_user "Timeout...exit";
                exit
            }
        }
        expect -re  "sftp" { send "fstp file download to local path sussfully!\n exit\r";}
        expect eof
EOF
        if [ -f "${tmp_save_path}/${data_filename}" ];then
            hdfs dfs -mkdir -p ${hive_db_dir}/${sqoop_pt1}${full_time}/
            hdfs dfs -put ${tmp_save_path}/${data_filename} ${hive_db_dir}/${sqoop_pt1}${full_time}/
            beeline -u jdbc:hive2://localhost:10000 -n qxb_sqoop -p qxb_sqoop -e "msck repair table stg.$hive_table_name"
            echo "fstp file upload sussfully!"
            rm ${tmp_save_path}/${data_filename}
        else
            echo "fstp file download failed, exit 1"
            exit 1
        fi
endssh
        #指令执行标识位(0:成功；1：失败；2：当天已执行; 3:文件不存在)
        success_flag=$?
        #传输完成后，删除对应文件目录下的文件
        rm ${tmp_save_path}/${data_filename}
    else
        echo "file on huawei FI does not exist, program exit"
        success_flag=3
    fi
else
    echo "file is already exists, program exist"
    success_flag=2
fi

#操作结束时间
end_time=`date "+%Y-%m-%d %H:%M:%S"`
record_logs "$start_time" "$end_time" "$success_flag"
if [ "$success_flag" == "1" ];then
  exit 1
elif [ "$success_flag" == "0" ];then
  echo "job success, exist"
fi


