#!/bin/bash

# Params:
# 1. Ip address
# 2. Port

java -classpath "lib/*" es.bernal.thriftclient.JDBCExample $1 $2