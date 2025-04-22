#!/bin/bash

current_path=`pwd`
case "`uname`" in
    Linux)
                bin_abs_path=$(readlink -f $(dirname $0))
                ;;
        *)
                bin_abs_path=`cd $(dirname $0); pwd`
                ;;
esac
base=${bin_abs_path}/..
export LANG=en_US.UTF-8
export BASE=$base

if [ -f $base/bin/adapter.pid ] ; then
        echo "found adapter.pid , Please run stop.sh first ,then startup.sh" 2>&2
    exit 1
fi

if [ ! -d $base/logs ] ; then
        mkdir -p $base/logs
fi

## set java path
if [ -z "$JAVA" ] ; then
  JAVA=$(which java)
fi

ALIBABA_JAVA="/usr/alibaba/java/bin/java"
TAOBAO_JAVA="/opt/taobao/java/bin/java"
if [ -z "$JAVA" ]; then
  if [ -f $ALIBABA_JAVA ] ; then
          JAVA=$ALIBABA_JAVA
  elif [ -f $TAOBAO_JAVA ] ; then
          JAVA=$TAOBAO_JAVA
  else
          echo "Cannot find a Java JDK. Please set either set JAVA or put java (>=1.5) in your PATH." 2>&2
    exit 1
  fi
fi

case "$#"
in
0 )
  ;;
2 )
  if [ "$1" = "debug" ]; then
    DEBUG_PORT=$2
    DEBUG_SUSPEND="n"
    JAVA_DEBUG_OPT="-agentlib:jdwp=transport=dt_socket,server=y,suspend=$DEBUG_SUSPEND,address=*:$DEBUG_PORT"
  fi
  ;;
* )
  echo "THE PARAMETERS MUST BE TWO OR LESS.PLEASE CHECK AGAIN."
  exit;;
esac

JavaVersion=`$JAVA -version 2>&1 |awk 'NR==1{ gsub(/"/,""); print $3 }' | awk  -F '.' '{print $1}'`
str=`file -L $JAVA | grep 64-bit`
# 移除不兼容的选项
JAVA_OPTS="$JAVA_OPTS -Xss1m -XX:-OmitStackTraceInFastThrow -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$base/logs"

if [ $JavaVersion -ge 11 ] ; then
  # Java 11+的GC日志选项
  #JAVA_OPTS="$JAVA_OPTS -Xlog:gc*:file=$base/logs/gc.log:time,uptime,level,tags"
  JAVA_OPTS="$JAVA_OPTS"
else
  #JAVA_OPTS="$JAVA_OPTS -Xloggc:$base/logs/canal/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime"
  JAVA_OPTS="$JAVA_OPTS -XX:+UseFastAccessorMethods -XX:+PrintAdaptiveSizePolicy -XX:+PrintTenuringDistribution"
fi

if [ -n "$str" ]; then
  if [ $JavaVersion -ge 11 ] ; then
    # 简化G1GC设置，移除不兼容的选项
    JAVA_OPTS="-server -Xms2g -Xmx3g -XX:+UseG1GC -XX:MaxGCPauseMillis=250 -XX:+UseGCOverheadLimit $JAVA_OPTS"
  else
    # 旧版本Java的参数
    JAVA_OPTS="-server -Xms2g -Xmx3g -Xmn1g -XX:SurvivorRatio=2 -XX:MaxTenuringThreshold=15 -XX:+DisableExplicitGC $JAVA_OPTS"
  fi
else
  # 移除PermSize参数，这些在Java 8+已不再存在
  JAVA_OPTS="-server -Xms1024m -Xmx1024m -XX:NewSize=256m -XX:MaxNewSize=256m $JAVA_OPTS"
fi
JAVA_OPTS=" $JAVA_OPTS -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dfile.encoding=UTF-8"
ADAPTER_OPTS="-DappName=canal-adapter"

for i in $base/lib/*;
    do CLASSPATH=$i:"$CLASSPATH";
done

CLASSPATH="$base/conf:$CLASSPATH";

echo "cd to $bin_abs_path for workaround relative path"
cd $bin_abs_path

echo CLASSPATH :$CLASSPATH
$JAVA $JAVA_OPTS $JAVA_DEBUG_OPT $ADAPTER_OPTS -classpath .:$CLASSPATH com.alibaba.otter.canal.adapter.launcher.CanalAdapterApplication > $base/logs/adapter_stdout.log 2> $base/logs/adapter_stderr.log &
echo $! > $base/bin/adapter.pid

echo "cd to $current_path for continue"
cd $current_path