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

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo Retrieving Alluxio dependency...

alluxio_home=

if [ -n "$ALLUXIO_HOME" ]
then
    verbose "ALLUXIO_HOME is set to: $ALLUXIO_HOME, use it to locate Alluxio dependencies."
    alluxio_home=$ALLUXIO_HOME
fi

if [ -z "$ALLUXIO_HOME" ]
then
    verbose "ALLUXIO_HOME wasn't set, use $KYLIN_HOME/alluxio"
    alluxio_home=$KYLIN_HOME/alluxio
fi

if [ ! -d "$alluxio_home/lib" ]
then
    echo "Optional dependency alluxio not found, if you need this; please set ALLUXIO_HOME"
    echo "echo 'skip alluxio_dependency'" > ${dir}/cached-alluxio-dependency.sh
else
    alluxio_dependency=`find -L $alluxio_home/client -name 'alluxio-*-client.jar' ! -name '*doc*' ! -name '*test*' ! -name '*sources*' ''-printf '%p:' | sed 's/:$//'`
    if [ -z "$alluxio_dependency" ]
    then
        quit "alluxio jars not found"
    else
        verbose "alluxio dependency: $alluxio_dependency"
        export alluxio_dependency
    fi
    echo "export alluxio_dependency=$alluxio_dependency" > ${dir}/cached-alluxio-dependency.sh
fi
