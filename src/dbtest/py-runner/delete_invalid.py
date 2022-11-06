import os
import argparse

base = '/root/github-projects/3TS-Coo/src/dbtest' 
do_test_list_base = os.path.join(base, "do_test_list_crdb.txt") # 所有需要测试的样例列表

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--result_dir", default="crdb/serializable", help="目前已有的测试样例结果列表")
    args = parser.parse_args()

    result_dir = os.path.join(base, args.result_dir) 
    
    # 从结果文件中获取已有结果的集合
    results = os.listdir(result_dir)

    to_delete = []
    for res_file in results:
        res_file_path = os.path.join(result_dir, res_file)
        try:
            with open(res_file_path, 'r') as f:
                content = "".join(f.readlines())
                if content.__contains__('SQL_INVALID_HANDLE'):
                    print(res_file_path)
                    to_delete.append(res_file_path)
        except UnicodeDecodeError as e:
            to_delete.append(res_file_path)
                
    print("{} files to be delete".format(len(to_delete)))
    
    for filepath in to_delete:
        print("remove", filepath)
        os.remove(filepath)
