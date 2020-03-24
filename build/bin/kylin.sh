#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# set verbose=true to print more logs during start up




source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh $@
if [ "$verbose" = true ]; then
    shift
fi

mkdir -p ${KYLIN_HOME}/logs
mkdir -p ${KYLIN_HOME}/ext

source ${dir}/set-java-home.sh

function retrieveDependency() {
    #retrive $hive_dependency and $hbase_dependency
    if [[ -z $reload_dependency && `ls -1 ${dir}/cached-* 2>/dev/null | wc -l` -eq 5 ]]
    then
        echo "Using cached dependency..."
        source ${dir}/cached-hive-dependency.sh
        #retrive $hbase_dependency
        metadataUrl=`${dir}/get-properties.sh kylin.metadata.url`
        if [[ "${metadataUrl##*@}" == "hbase" ]]
        then
            source ${dir}/cached-hbase-dependency.sh
        fi
        source ${dir}/cached-hadoop-conf-dir.sh
        source ${dir}/cached-kafka-dependency.sh
        source ${dir}/cached-spark-dependency.sh
    else
        source ${dir}/find-hive-dependency.sh
        #retrive $hbase_dependency
        metadataUrl=`${dir}/get-properties.sh kylin.metadata.url`
        if [[ "${metadataUrl##*@}" == "hbase" ]]
        then
            source ${dir}/find-hbase-dependency.sh
        fi
        source ${dir}/find-hadoop-conf-dir.sh
        source ${dir}/find-kafka-dependency.sh
        source ${dir}/find-spark-dependency.sh
    fi

    # get hdp_version
    if [ -z "${hdp_version}" ]; then
        hdp_version=`/bin/bash -x hadoop 2>&1 | sed -n "s/\(.*\)export HDP_VERSION=\(.*\)/\2/"p`
        verbose "hdp_version is ${hdp_version}"
    fi

    tomcat_root=${dir}/../tomcat
    export tomcat_root

    # get KYLIN_REST_ADDRESS
    if [ -z "$KYLIN_REST_ADDRESS" ]
    then
        KYLIN_REST_ADDRESS=`hostname -f`":"`grep "<Connector port=" ${tomcat_root}/conf/server.xml |grep protocol=\"HTTP/1.1\" | cut -d '=' -f 2 | cut -d \" -f 2`
        export KYLIN_REST_ADDRESS
        verbose "KYLIN_REST_ADDRESS is ${KYLIN_REST_ADDRESS}"
    fi

    # compose hadoop_dependencies
    hadoop_dependencies=${hadoop_dependencies}:`hadoop classpath`
    if [ -n "${hbase_dependency}" ]; then
        hadoop_dependencies=${hadoop_dependencies}:${hbase_dependency}
    fi
    if [ -n "${hive_dependency}" ]; then
        hadoop_dependencies=${hadoop_dependencies}:${hive_dependency}
    fi
    if [ -n "${kafka_dependency}" ]; then
        hadoop_dependencies=${hadoop_dependencies}:${kafka_dependency}
    fi
    if [ -n "${spark_dependency}" ]; then
        hadoop_dependencies=${hadoop_dependencies}:${spark_dependency}
    fi

    # compose KYLIN_TOMCAT_CLASSPATH
    tomcat_classpath=${tomcat_root}/bin/bootstrap.jar:${tomcat_root}/bin/tomcat-juli.jar:${tomcat_root}/lib/*
    export KYLIN_TOMCAT_CLASSPATH=${tomcat_classpath}:${KYLIN_HOME}/conf:${KYLIN_HOME}/lib/*:${KYLIN_HOME}/ext/*:${hadoop_dependencies}

    # compose KYLIN_TOOL_CLASSPATH
    export KYLIN_TOOL_CLASSPATH=${KYLIN_HOME}/conf:${KYLIN_HOME}/tool/*:${KYLIN_HOME}/ext/*:${hadoop_dependencies}

    # compose kylin_common_opts
    kylin_common_opts="-Dkylin.hive.dependency=${hive_dependency} \
    -Dkylin.kafka.dependency=${kafka_dependency} \
    -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} \
    -Dkylin.server.host-address=${KYLIN_REST_ADDRESS} \
    -Dspring.profiles.active=${spring_profile} \
    -Dhdp.version=${hdp_version}"

    # compose KYLIN_TOMCAT_OPTS
    KYLIN_TOMCAT_OPTS="-Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-server-log4j.properties \
    -Djava.util.logging.manager=org.apache.juli.ClassLoaderLogManager \
    -Dorg.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH=true \
    -Dorg.apache.catalina.connector.CoyoteAdapter.ALLOW_BACKSLASH=true \
    -Djava.endorsed.dirs=${tomcat_root}/endorsed  \
    -Dcatalina.base=${tomcat_root} \
    -Dcatalina.home=${tomcat_root} \
    -Djava.io.tmpdir=${tomcat_root}/temp ${kylin_common_opts}"
    export KYLIN_TOMCAT_OPTS

    # compose KYLIN_TOOL_OPTS
    KYLIN_TOOL_OPTS="-Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-tools-log4j.properties ${kylin_common_opts}"
    export KYLIN_TOOL_OPTS
}

function checkBasicKylinProps() {
    spring_profile=`${dir}/get-properties.sh kylin.security.profile`
    if [ -z "$spring_profile" ]
    then
        quit 'Please set kylin.security.profile in kylin.properties, options are: testing, ldap, saml.'
    else
        verbose "kylin.security.profile is $spring_profile"
    fi
}

function checkRestPort() {
    kylin_rest_address_arr=(${KYLIN_REST_ADDRESS//:/ })
    inuse=`netstat -tlpn | grep "\b${kylin_rest_address_arr[1]}\b"`
    [[ -z ${inuse} ]] || quit "Port ${kylin_rest_address_arr[1]} is not available. Another kylin server is running?"
}


function classpathDebug() {
    if [ "${KYLIN_CLASSPATH_DEBUG}" != "" ]; then
        echo "Finding ${KYLIN_CLASSPATH_DEBUG} on classpath" $@
        $JAVA -classpath $@ org.apache.kylin.common.util.ClasspathScanner ${KYLIN_CLASSPATH_DEBUG}
    fi
}

function runTool() {

    retrieveDependency

    # get KYLIN_EXTRA_START_OPTS
    if [ -f "${KYLIN_HOME}/conf/setenv-tool.sh" ]; then
        source ${KYLIN_HOME}/conf/setenv-tool.sh
    fi

    verbose "java opts for tool is ${KYLIN_EXTRA_START_OPTS} ${KYLIN_TOOL_OPTS}"
    verbose "java classpath for tool is ${KYLIN_TOOL_CLASSPATH}"
    classpathDebug ${KYLIN_TOOL_CLASSPATH}

    exec $JAVA ${KYLIN_EXTRA_START_OPTS} ${KYLIN_TOOL_OPTS} -classpath ${KYLIN_TOOL_CLASSPATH}  "$@"
}

if [ "$2" == "--reload-dependency" ]
then
    reload_dependency=1
fi

# start command
if [ "$1" == "start" ]
then
    if [ -f "${KYLIN_HOME}/pid" ]
    then
        PID=`cat $KYLIN_HOME/pid`
        if ps -p $PID > /dev/null
        then
          quit "Kylin is running, stop it first"
        fi
    fi

    checkBasicKylinProps

    source ${dir}/check-env.sh

    retrieveDependency

    checkRestPort

    ${KYLIN_HOME}/bin/check-migration-acl.sh || { exit 1; }

    # get KYLIN_EXTRA_START_OPTS
    if [ -f "${KYLIN_HOME}/conf/setenv.sh" ]; then
        source ${KYLIN_HOME}/conf/setenv.sh
    fi

    verbose "java opts is ${KYLIN_EXTRA_START_OPTS} ${KYLIN_TOMCAT_OPTS}"
    verbose "java classpath is ${KYLIN_TOMCAT_CLASSPATH}"
    classpathDebug ${KYLIN_TOMCAT_CLASSPATH}
    $JAVA ${KYLIN_EXTRA_START_OPTS} ${KYLIN_TOMCAT_OPTS} -classpath ${KYLIN_TOMCAT_CLASSPATH}  org.apache.catalina.startup.Bootstrap start >> ${KYLIN_HOME}/logs/kylin.out 2>&1 & echo $! > ${KYLIN_HOME}/pid &

    echo ""
    echo "A new Kylin instance is started by $USER. To stop it, run 'kylin.sh stop'"
    echo "Check the log at ${KYLIN_HOME}/logs/kylin.log"
    echo "Web UI is at http://${KYLIN_REST_ADDRESS}/kylin"
    exit 0
    
# run command
elif [ "$1" == "run" ]
then
    retrieveStartCommand
    ${start_command}

# stop command
elif [ "$1" == "stop" ]
then
    if [ -f "${KYLIN_HOME}/pid" ]
    then
        PID=`cat $KYLIN_HOME/pid`
        WAIT_TIME=2
        LOOP_COUNTER=10
        if ps -p $PID > /dev/null
        then
            echo "Stopping Kylin: $PID"
            kill $PID

            for ((i=0; i<$LOOP_COUNTER; i++))
            do
                # wait to process stopped 
                sleep $WAIT_TIME
                if ps -p $PID > /dev/null ; then
                    echo "Stopping in progress. Will check after $WAIT_TIME secs again..."
                    continue;
                else
                    break;
                fi
            done

            # if process is still around, use kill -9
            if ps -p $PID > /dev/null
            then
                echo "Initial kill failed, getting serious now..."
                kill -9 $PID
                sleep 1 #give kill -9  sometime to "kill"
                if ps -p $PID > /dev/null
                then
                   quit "Warning, even kill -9 failed, giving up! Sorry..."
                fi
            fi

            # process is killed , remove pid file		
            rm -rf ${KYLIN_HOME}/pid
            echo "Kylin with pid ${PID} has been stopped."
            exit 0
        else
           quit "Kylin with pid ${PID} is not running"
        fi
    else
        quit "Kylin is not running"
    fi

# streaming command
elif [ "$1" == "streaming" ]
then
    if [ $# -lt 2 ]
    then
        echo "invalid input args $@"
        exit -1
    fi
    if [ "$2" == "start" ]
    then
        if [ -f "${KYLIN_HOME}/streaming_receiver_pid" ]
        then
            PID=`cat $KYLIN_HOME/streaming_receiver_pid`
            if ps -p $PID > /dev/null
            then
              echo "Kylin is running, stop it first"
            exit 1
            fi
        fi
        #retrive $hbase_dependency
        metadataUrl=`${dir}/get-properties.sh kylin.metadata.url`
        if [[ "${metadataUrl##*@}" == "hbase" ]]
        then
            source ${dir}/find-hbase-dependency.sh
        fi
        #retrive $KYLIN_EXTRA_START_OPTS
        if [ -f "${KYLIN_HOME}/conf/setenv.sh" ]
            then source ${KYLIN_HOME}/conf/setenv.sh
        fi

        mkdir -p ${KYLIN_HOME}/ext
        HBASE_CLASSPATH=`hbase classpath`
        #echo "hbase class path:"$HBASE_CLASSPATH
        STREAM_CLASSPATH=${KYLIN_HOME}/lib/streaming/*:${KYLIN_HOME}/ext/*:${HBASE_CLASSPATH}

        # KYLIN_EXTRA_START_OPTS is for customized settings, checkout bin/setenv.sh
        $JAVA -cp $STREAM_CLASSPATH ${KYLIN_EXTRA_START_OPTS} \
        -Dlog4j.configuration=stream-receiver-log4j.properties\
        -DKYLIN_HOME=${KYLIN_HOME}\
        -Dkylin.hbase.dependency=${hbase_dependency} \
        org.apache.kylin.stream.server.StreamingReceiver $@ > ${KYLIN_HOME}/logs/streaming_receiver.out 2>&1 & echo $! > ${KYLIN_HOME}/streaming_receiver_pid &
        exit 0
    elif [ "$2" == "stop" ]
    then
        if [ ! -f "${KYLIN_HOME}/streaming_receiver_pid" ]
        then
            echo "streaming is not running, please check"
            exit 1
        fi
        PID=`cat ${KYLIN_HOME}/streaming_receiver_pid`
        if [ "$PID" = "" ]
        then
            echo "streaming is not running, please check"
            exit 1
        else
            echo "stopping streaming:$PID"
            WAIT_TIME=2
            LOOP_COUNTER=20
            if ps -p $PID > /dev/null
            then
                echo "Stopping Kylin: $PID"
                kill $PID

                for ((i=0; i<$LOOP_COUNTER; i++))
                do
                    # wait to process stopped
                    sleep $WAIT_TIME
                    if ps -p $PID > /dev/null ; then
                        echo "Stopping in progress. Will check after $WAIT_TIME secs again..."
                        continue;
                    else
                        break;
                    fi
                done

                # if process is still around, use kill -9
                if ps -p $PID > /dev/null
                then
                    echo "Initial kill failed, getting serious now..."
                    kill -9 $PID
                    sleep 1 #give kill -9  sometime to "kill"
                    if ps -p $PID > /dev/null
                    then
                       quit "Warning, even kill -9 failed, giving up! Sorry..."
                    fi
                fi

                # process is killed , remove pid file
                rm -rf ${KYLIN_HOME}/streaming_receiver_pid
                echo "Kylin with pid ${PID} has been stopped."
                exit 0
            else
               quit "Kylin with pid ${PID} is not running"
            fi
        fi
    elif [[ "$2" = org.apache.kylin.* ]]
    then
        source ${KYLIN_HOME}/conf/setenv.sh
        HBASE_CLASSPATH=`hbase classpath`
        #echo "hbase class path:"$HBASE_CLASSPATH
        STREAM_CLASSPATH=${KYLIN_HOME}/lib/streaming/*:${KYLIN_HOME}/ext/*:${HBASE_CLASSPATH}

        shift
        # KYLIN_EXTRA_START_OPTS is for customized settings, checkout bin/setenv.sh
        $JAVA -cp $STREAM_CLASSPATH ${KYLIN_EXTRA_START_OPTS} \
        -Dlog4j.configuration=stream-receiver-log4j.properties\
        -DKYLIN_HOME=${KYLIN_HOME}\
        -Dkylin.hbase.dependency=${hbase_dependency} \
        "$@"
        exit 0
    fi

elif [ "$1" = "version" ]
then
    runTool org.apache.kylin.common.KylinVersion

elif [ "$1" = "diag" ]
then
    echo "'kylin.sh diag' no longer supported, use diag.sh instead"
    exit 0

# tool command
elif [[ "$1" = org.apache.kylin.* ]]
then
    runTool "$@"
else
    quit "Usage: 'kylin.sh [-v] start' or 'kylin.sh [-v] stop'"
fi
