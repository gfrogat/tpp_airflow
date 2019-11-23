#!/bin/bash

bash ${SPARK_HOME}/sbin/start-master.sh
bash ${SPARK_HOME}/sbin/start-slave.sh "spark://$(hostname):7077"