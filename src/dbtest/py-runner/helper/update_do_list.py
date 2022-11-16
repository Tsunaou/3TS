import os
import argparse

base = '/root/github-projects/3TS-Coo/src/dbtest' 
do_test_list_base = os.path.join(base, "do_test_list_base.txt") # 所有需要测试的样例列表

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--do_test_list", default="do_test_list_my.txt", help="还需测试的样例列表")
    parser.add_argument("-d", "--result_dir", default="mysql/serializable", help="目前已有的测试样例结果列表")
    args = parser.parse_args()

    do_test_list = os.path.join(base, args.do_test_list) 
    result_dir = os.path.join(base, args.result_dir) 
    
    # 获取所有列表的集合
    with open(do_test_list_base, "r") as f:
        todos = f.readlines()
    todo_set = set([todo.strip('\n') for todo in todos])

    # 从结果文件中获取已有结果的集合
    results = os.listdir(result_dir)
    # 删除创建时间最新的那个文件（不完整）
    latest = None
    create_time = None
    
    for res_file in results:
        res_file_path = os.path.join(result_dir, res_file)
        ct = os.path.getctime(res_file_path)
        
        if latest is None or ct > create_time:
            latest = res_file_path
            create_time = ct
    
    print("The latest file is {}, created at {}. It's incomplete so we delete it.".format(latest, create_time))
    os.remove(latest)
    
    available_results = set([res.replace('.txt', '') for res in results])

    # 去掉已有结果的内容
    todos = todo_set-available_results
    todos = list(todos)
    todos.sort()
    with open(do_test_list, 'w') as f:
        for todo in todos:
            f.write(todo)
            f.write('\n')
