import time
import requests
import pandas as pd
import pymysql
import json

## 登录接口服务，获取txtAccessToken
def GetXAccessToken():
    accessKey =""
    secretKey =""
    urlNameString = ""+accessKey \
                + "="+secretKey
    responsedata = requests.get(urlNameString)
    data = eval(responsedata.text)
    if(responsedata.status_code == 200):
        return data.get("accessToken")
    else:
        return data.get("message")


#返回值：html字符串
def getmysqldata(condition):
    #表头
    columns=['job_name','src_dbname','src_tablename','mode','success_flag','check_sum']
    df_init=pd.DataFrame(columns=columns)
    conn = pymysql.Connect(host="55.14.58.23", port=3306,
                           user="cmbcckg", password="Eccdg%2015",
                           database="cckg01", charset="utf8")
    cur = conn.cursor()
    sql = "select job_name,src_dbname,src_tablename,mode,success_flag,check_sum from sqoop_etl_logs %s"%condition
    cur.execute(sql)
    start_num = 0
    for i in cur.fetchall():
        start_num+=1
        df_init.loc[start_num]=i
    cur.close()
    conn.close()
    return df_init.to_html(index=False)


if __name__ == '__main__':
    txtAccessToken = GetXAccessToken()
    #st环境邮件发送地址
    requestUrl = "http://yst-services-gateway-st.paas.cmbchina.cn/email"
    #查询条件
    todaytime=time.strftime("%Y-%m-%d", time.localtime())
    condition = "where (left(start_time,10)='%s' and success_flag=1) or (left(start_time,10)='%s' and check_sum='false')"%(todaytime,todaytime)
    #发送邮件具体参数配置
    headers={"X-Access-Token":txtAccessToken,"Content-Type":"application/json"}
    jsonParam={}
    jsonParam["destAddress"]="ligongzheng@yst.cmbchina.cn"
    jsonParam["subject"]= "小明专用邮件接口"
    jsonParam["body"]=getmysqldata(condition)
    data = requests.post(url=requestUrl,headers=headers,data=json.dumps(jsonParam))
    print(data.text)


