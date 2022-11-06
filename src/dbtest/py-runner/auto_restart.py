import subprocess
import os
import sys

base = '/root/github-projects/3TS-Coo/src/dbtest' 
do_test_list_base = os.path.join(base, "do_test_list_crdb.txt") # 所有需要测试的样例列表

def get_left_number(do_test_list):
    with open(file=do_test_list, mode='r') as f:
        testcases = f.readlines()  
        count = 0
        for case in testcases:
            if len(case.strip('\n')) > 1:
                count = count + 1
        print("{} constains {} lines".format(do_test_list, len(testcases)))
        print("{} constains {} cases".format(do_test_list,count))        
        return count 
    

if __name__ == '__main__':    
    # 当 do_test_list 的数量 > 1时
    p_run = subprocess.Popen(['./update_crdb_list.sh'], cwd=base)
    p_run.wait()
    
    while get_left_number(do_test_list_base) > 1:
        p_run = subprocess.Popen(['./run_crdb.sh'], cwd=base)
        p_run.wait()
        p_run = subprocess.Popen(['./update_crdb_list.sh'], cwd=base)
        p_run.wait()
