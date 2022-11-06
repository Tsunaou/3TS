#!/bin/bash
./3ts_dbtest_v2 -isolation=serializable -db_type="crdb" -user="" -passwd="" -case_dir="crdb" -do_test_list="./do_test_list_crdb.txt" -case_dir="test_case_v2"