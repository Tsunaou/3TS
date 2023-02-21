#!/bin/bash
DBTEST_DIR="/root/github-projects/3TS-Coo/src/dbtest"
PROGRAM="3ts_dbtest_v2"
BUILD_DIR=$DBTEST_DIR"/build/"
CASE_DIR=$DBTEST_DIR"/t/test_case_v2"
DB_DIR=$DBTEST_DIR"/databases/mariadb"
LIST_DIR=$DB_DIR"/do_test_list_mariadb.txt"
OUTPUT_DIR=$DB_DIR
RUNNER=$BUILD_DIR$PROGRAM

$RUNNER \
    -isolation=repeatable-read \
    -db_type="mariadb" \
    -user="root" -passwd="123456" \
    -conn_pool_size=4 \
    -timeout=1 \
    -case_dir=$CASE_DIR \
    -out_dir=$OUTPUT_DIR \
    -do_test_list=$LIST_DIR