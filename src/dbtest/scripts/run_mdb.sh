#!/bin/bash
# python3 ./src/random_do_list.py 2 3
# python3 ./src/mda_generate.py mysql single 
./3ts_dbtest_v2 -isolation=serializable -db_type="mysql" -user="myuser" -passwd="mypass" -case_dir="mysql"
python ./src/mda_detect.py