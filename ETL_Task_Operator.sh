#!/bin/bash
#Path: /home/e/zhangtao02/ETL_Task_Operator.sh
#         -n: 脚本名称（新增加任务的脚本名称，带后缀）
#         -t: y  / n  是否时间触发
#         -h: 时间触发小时
#         -m: 时间触发分钟
#         -r: 负责人
#         -s:依赖触发时stream所依赖的etl_job，不用带后缀
#         -d:依赖触发时dependency所依赖的etl_job，不用带后缀，多个的以逗号分隔
#dependency_parser: 用于解析脚本中所依赖的数据表

MYSQL_HOST="rdseok8evj5w2jh5uprrj.mysql.rds.aliyuncs.com"
MYSQL_USER="data" 
MYSQL_PSWD="sudEio998s8daAD0af"

ETL_JOB_TABLE="etlmetastore.etl_job"
ETL_SOURCE_TABLE="etlmetastore.etl_job_source"
ETL_TIMETRIGGER_TABLE="etlmetastore.etl_timetrigger"
ETL_STREAM_TABLE="etlmetastore.etl_job_stream"
ETL_DEPENDENCY_TABLE="etlmetastore.etl_job_dependency"

dependency_parser() {
if [[ $1 ]];then
  input_file=$1
  echo $input_file
else
  echo '===>dependency_parser needs an input parameter: type->file'
fi

in_next_line=0
sql_start=0
index=0
declare -A array
while read line
do
  until [[ $sql_start -eq 1 ]]
  do
    if [[ $line =~ "SQL" ]];then
      #echo "$line contains SQL, set sql_start = 1..."
      sql_start=1
    else
      #echo $line
      #echo "read next line..."
      continue 2
    fi
  done

  if [[ $line =~ "sqls=map" ]];then
    break
  fi

  if [[ $sql_start -eq 1 && $in_next_line -eq 1 ]];then
    in_next_line=0
    #去掉*字符
    line=${line//\*/ }
    for item in `echo "$line" | sed "s/[[:space:]]/\n/g"`
    do
      if [[ $item =~ "." ]];then
        echo $item
	echo "index = $index"
	array[$index]=$item
	echo ${array[$index]}
	let "index+=1"
      else
        continue
      fi
    done
  fi

  if [[ $line =~ "from" || $line =~ "join" ]];then
    echo "line!!!!!!!!!!!!!=====================>>>>>>>$line"
    if [[ $line =~ "." ]];then
      echo "===> $line contains point..."
      #去掉*字符
      line=${line//\*/ }
      for item in `echo "$line" | sed "s/[[:space:]]/\n/g"`
      do
        if [[ $item =~ "." ]];then
	  echo $item
	  echo "index = $index"
	  array[$index]=$item
	  echo ${array[$index]}
	  let "index+=1"
	else
	  continue
	fi
      done
    else
      echo "$line does not contain point, set in_next_line = 1..."
      in_next_line=1
      continue
    fi
  fi
done < $input_file

for item in ${input_file//\// }
do
  script_name=$item
done

echo "===>script_name = $script_name"

databases=(app db_ad db_car db_dynamic_subsidies db_email_report db_failure db_finance db_h5_acts db_log_parase db_order db_pri db_report db_stat db_tmp dc_data default dim dw_warehouse dwa dwd dwr stg tmp tmp_rjg wkd_app wkd_dwa wkd_dwd wkd_stg)
declare -A table_array
index=0
echo "======================================Finally, parse the dependency============================================="
for item in ${array[@]}
do
    in_flag=0
    echo $item
    database=`echo $item | awk -F '.' '{print $1}'`
    table_name=`echo $item | awk -F '.' '{print $2}'`
    #若不是以ETL_SYSTEM开头的表名,需要添加ETL_SYSTEM相关信息,作为在ETL_JOB中识别该JOB
    if [[ ${table_name//$database/} = ${table_name} ]];then
      table_name=$database'_'$table_name
    fi
    table_name=${table_name//)/}
    echo "database = $database"
    echo "table_name = $table_name"
    for item in ${databases[@]}
    do
      #echo "###item = $item, database = $database###"
      if [[ "$database" = "$item" ]];then
        in_flag=1
	break
      fi
    done

    if [[ $in_flag -eq 1 ]];then
      echo "===>database $database in databases."
      #排除自身的依赖
      table_name_bak=`echo $table_name | tr "[:lower:]" "[:upper:]"`
      if [[ "$table_name_bak" = "$ETL_JOB" ]]; then
	continue
      fi
      exist_flag=0
      for item in ${table_array[@]}
      do
        if [[ $table_name = $item ]];then
	  exist_flag=1
	  break
        else
          continue
        fi
      done
      if [[ exist_flag -eq 0 ]];then
	table_array[$index]=$table_name
	let "index+=1"
      fi
    else
      echo "===>database $database not in databases, please check..."
    fi
done

flag=0
echo "======================================Finally, output the dependent table_name============================================="
for item in ${table_array[@]}
do
  if [[ $flag -eq 0 ]];then
    stream=$item
    let "flag+=1"
  else
    if [[ $flag -eq 1 ]];then
      dependency=$item
      flag=$flag+1
    else
      dependency=$dependency','$item
    fi
  fi
done

echo "stream = $stream"
echo "dependency = $dependency"
echo "======================================Finally, output the dependent table_name============================================="
}

while getopts ":an:t:h:m:r:s:d:" optname
do
  case "$optname" in
    "a")
      operate_method="add"
      echo "operate_method: "$operate_method
      ;;
    "n")
      script_name=`echo $OPTARG | tr "[:upper:]" "[:lower:]"`
      echo "script_name: "$script_name
      ;;
    "t")
      time_trigger=`echo $OPTARG | tr "[:lower:]" "[:upper:]" `
      echo "time_trigger: "$time_trigger
      ;;
    "h")
      hour=$OPTARG
      echo "hour: "$hour
      ;;
    "m")
      minute=$OPTARG
      echo "minute: "$minute
      ;;
    "r")
      responsor=$OPTARG
      echo "responsor: "$responsor 
      ;;
    "s")
      stream=$OPTARG
      echo "stream: "$stream
      ;;
    "d")
      dependency=$OPTARG
      echo "dependency: "$dependency
      ;;
    *)
      echo "Unknown options"
      ;;
  esac
done

if [ -z $script_name ];then
  echo "===>script name could not be empty!!!"
  exit 1
fi

ETL_SYSTEM=`echo ${script_name:0:3} | tr "[:lower:]" "[:upper:]"`
ETL_JOB=`echo $script_name | awk -F "." '{print $1}' | tr "[:lower:]" "[:upper:]"`
ETL_SOURCE=`echo $ETL_JOB | tr "[:upper:]" "[:lower:]"`
ETL_SERVER="ETL_JOB01"
CHECK_FLAG="Y"
AUTO_OFF="N"
CALENDAR_BU="BASE"
SCRIPT_PATH_PREFIX="/home/e/dwetl/APP/"
SCRIPT_PATH=$SCRIPT_PATH_PREFIX$ETL_SYSTEM'/bin/'$script_name
CHECK_LAST_STATUS="Y"
JOB_PRIORITY=1
JOB_THEME=`echo $ETL_SYSTEM | tr "[:upper:]" "[:lower:]"`
EXPAND_PATH="home"
RETRY_COUNT=0
FLAG="Y"
echo $ETL_SYSTEM
echo $ETL_JOB
echo $SCRIPT_PATH
echo $JOB_THEME

if [[ "$time_trigger" != "Y" ]];then
  dependency_parser $SCRIPT_PATH
fi

#Update ETL_JOB表
echo "update table etl_job, ETL_SYSTEM=$ETL_SYSTEM, ETL_JOB=$ETL_JOB, TIME_TRIGGER=$time_trigger, RESPONSOR=$responsor"
#mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PSWD -e "insert into $ETL_JOB_TABLE values ('$ETL_SYSTEM', '$ETL_JOB', '$ETL_SERVER', null, 0, 'D', 1, null, null, null, null, null, null, null, '$CHECK_FLAG', '$AUTO_OFF', null, '$CALENDAR_BU', '$SCRIPT_PATH', null, null, '$CHECK_LAST_STATUS', '$time_trigger', $JOB_PRIORITY, '$JOB_THEME', null, null, '$EXPAND_PATH', '$responsor', $RETRY_COUNT)"
if [ $? -eq 0 ];then
  echo '===>update table etl_job succeed'
else
  echo '===>update table etl_job failed, please check your sql'
fi

#Update ETL_SOURCE表
echo "update table etl_job_source, ETL_SOURCE=$ETL_SOURCE, ETL_SYSTEM=$ETL_SYSTEM, ETL_JOB=$ETL_JOB"
#mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PSWD -e "insert into $ETL_SOURCE_TABLE values ('$ETL_SOURCE', '$ETL_SYSTEM', '$ETL_JOB', '$ETL_JOB', 0, 0, 0, 0, 0, 0)"

if [ $? -eq 0 ];then
  echo '===>update table etl_job_source succeed'
else
  echo '===>update table etl_job_source failed, please check your sql'
fi

#根据是否为time_trigger来确定需要更新的表
if [ "$time_trigger" = "$FLAG" ];then
  echo "===>time_trigger=$time_trigger, 需要更新etl_timetrigger表"
  echo "update table etl_timetrigger, ETL_SYSTEM=$ETL_SYSTEM, ETL_JOB=$ETL_JOB, START_HOUR=$hour, START_MINUTE=$minute"
#mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PSWD -e "insert into $ETL_TIMETRIGGER_TABLE values ('$ETL_SYSTEM', '$ETL_JOB', 'D', $hour, $minute, 1, null, null)"
  if [ $? -eq 0 ];then
    echo '===>update table etl_timetrigger succeed'
  else
    echo '===>update table etl_timetrigger failed, please check your sql'
  fi
else
  echo "===>time_trigger=$time_trigger, 需要更新etl_job_stream/etl_job_dependency表"
  STREAM_ETL_SYSTEM=`echo ${stream:0:3} | tr "[:lower:]" "[:upper:]"`
  STREAM_ETL_JOB=`echo $stream | awk -F "." '{print $1}' | tr "[:lower:]" "[:upper:]"`
  echo "update table etl_job_stream, STREAM_ETL_SYSTEM=$STREAM_ETL_SYSTEM, STREAM_ETL_JOB=$STREAM_ETL_JOB"
#mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PSWD -e "insert into $ETL_STREAM_TABLE values ('$STREAM_ETL_SYSTEM', '$STREAM_ETL_JOB', '$ETL_SYSTEM', '$ETL_JOB', null, 1)"

  if [ $? -eq 0 ];then
    echo '===>update table etl_job_stream succeed'
  else
    echo '===>update table etl_job_stream failed, please check your sql'
  fi

  for dependency_item in `echo $dependency | sed 's/,/\n/g'`
  do
    DEPEND_ETL_SYSTEM=`echo ${dependency_item:0:3} | tr "[:lower:]" "[:upper:]"`
    DEPEND_ETL_JOB=`echo $dependency_item | awk -F "." '{print $1}' | tr "[:lower:]" "[:upper:]"`
    echo "update table etl_job_dependency, DEPEND_ETL_SYSTEM=$DEPEND_ETL_SYSTEM, DEPEND_ETL_JOB=$DEPEND_ETL_JOB"
    #mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PSWD -e "insert into $ETL_DEPENDENCY_TABLE values ('$ETL_SYSTEM', '$ETL_JOB', '$DEPEND_ETL_SYSTEM', '$DEPEND_ETL_JOB', null, 1)"
    if [ $? -eq 0 ];then
    echo '===>update table etl_job_dependency succeed'
  else
    echo '===>update table etl_job_dependency failed, please check your sql'
  fi
  done
fi

