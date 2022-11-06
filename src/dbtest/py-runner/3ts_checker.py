# /*
#  * Tencent is pleased to support the open source community by making 3TS available.
#  *
#  * Copyright (C) 2022 THL A29 Limited, a Tencent company.  All rights reserved. The below software
#  * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
#  * Tencent Modifications are Copyright (C) THL A29 Limited.
#  *
#  * Author:  xenitchen axingguchen tsunaouyang (xenitchen,axingguchen,tsunaouyang@tencent.com)
#  *
#  */


import queue
import os
import time
import re

WTypes = {'W', 'D', 'I'}  # 写操作的集合
RTypes = {'R', 'P'}  # 读操作的集合

debug_flag = False  # 如果为真，结果不输出到文件，而是打印到屏幕


class Edge:
    def __init__(self, type, out):
        self.type = type
        self.out = out

    def __str__(self):
        return "-{}->{}".format(self.type, self.out)


class OP:
    def __init__(self, op_type, tid, qid, key, value=None, finish=None) -> None:
        super().__init__()
        self.op_type = op_type
        self.tid = tid
        self.qid = qid
        self.key = key
        self.value = value
        self.finish = finish
        self.ver_index = None  # 操作了 key 上的第几个 version 的数据


class Txn:
    def __init__(self):
        self.tid = None
        self.begin_qid = None  # 第一个有效操作（读或者写）的 Qid
        self.end_qid = None
        self.begin_ts = -1
        self.end_ts = 99999999999999999999
        self.ops = []
        self.failed_reason = None
        self.committed = False

    def add(self, op: OP):
        self.ops.append(op)

    def __str__(self):
        return "[begin: {}, end: {}]".format(self.begin_ts, self.end_ts)


# find total variable number
def get_total(lines):
    ''' 返回执行历史中 Q0-T1 中 insert 的最大变量，例如 insert (0,0) (2,2) (4,4) (6,6) 结果就是 6 '''
    num = 0
    for query in lines:
        query = query.replace("\n", "")
        query = query.replace(" ", "")
        if query[0:2] == "Q0" and query.find("INSERT") != -1:
            tmp = find_data(query, "(")
            num = max(num, tmp)
        elif query[0:2] == "Q1":
            break
    return num


# extract the data we need in query
def find_data(query, target):
    pos = query.find(target)
    if pos == -1:
        return pos
    pos += len(target)
    data_value = ""
    for i in range(pos, len(query)):
        if query[i].isdigit():
            data_value += query[i]
        else:
            break
    if data_value == "":
        return -1
    data_value = int(data_value)
    return data_value


def redirect_output(func, content):
    if debug_flag:
        print(content, end='')
    else:
        func(content)


def print_path(result_folder, ts_now, edge):
    with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
        flag = 0
        for i in range(len(edge)):
            for v in edge[i]:
                if flag == 0:
                    flag = 1
                else:
                    redirect_output(f.write, ", ")
                redirect_output(f.write, str(i) + "->" + v.type + "->" + str(v.out))
        redirect_output(f.write, "\n\n")


def output_result(file, result_folder, ts_now, IsCyclic):
    with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
        redirect_output(f.write, file + ": " + IsCyclic + "\n")


def print_error(result_folder, ts_now, error_message):
    with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
        redirect_output(f.write, error_message + "\n")
        redirect_output(f.write, "\n\n")


def get_finish_time(query: str):
    pos = query.find("finishedat:")
    pos += len("finishedat:")
    data_value = ""
    tmp, tmp1 = "", ""
    for i in range(pos, len(query)):
        if query[i].isdigit():
            tmp += query[i]
        else:
            for j in range(3 - len(tmp)):
                tmp1 += "0"
            tmp = tmp1 + tmp
            data_value += tmp
            tmp, tmp1 = "", ""
    data_value = int(data_value)
    return data_value


class Checker(object):

    def __init__(self, file, result_folder, ts_now) -> None:
        super().__init__()
        self.file = file
        self.result_folder = result_folder
        self.ts_now = ts_now

        lines = self.read_file()
        if lines is None:
            print('[ERROR] file {} not exists')
            self.init = False
            return

        self.lines = list(map(lambda x: x.strip('\n').replace(' ', ''), lines))
        self.total_num = self.get_total_num()  # 在初始 INSERT 语句中出现的最大的 k

        wider_range = self.total_num + 2  # 开大一些范围，防止越界
        self.T = [Txn() for i in range(wider_range)]  # T[i] 表示事务 Ti，有效事务从 T1 开始
        self.ops_for_key = [[] for i in range(wider_range)]  # ops_for_key[k] 表示变量 k 的所有操作
        self.version_list = [[None] for i in range(wider_range)]  # version_list[k] 表示 k 上的版本序; 初始化为 None

        self.edge = [[] for i in range(wider_range)]
        self.indegree = [0] * wider_range
        self.visit = [0] * wider_range
        self.visit1 = [0] * wider_range
        self.path = []
        self.edge_type = []

        self.init = True

    def read_file(self):
        with open(self.file, 'r') as f:
            return f.readlines()

    def init_successfully(self):
        return self.init

    def get_kv_tuples(self, query):
        kvs = re.findall(r'\(\d+,\d+\)', query)
        kvs = [eval(kv) for kv in kvs]
        return kvs

    def get_query_txn_id(self, query):
        indexes = re.findall(r'Q(\d+)-T(\d+)', query)
        assert len(indexes) == 1
        qid = int(indexes[0][0])
        tid = int(indexes[0][1])
        return qid, tid

    def get_total_num(self):
        '''
        返回执行历史中 Q0-T1 中 insert 的最大变量
        例如 insert (0,0) (2,2) (4,4) (6,6) 结果就是 6
        :return:
        '''
        res = 0
        for query in self.lines:
            if query[0:2] == "Q0" and query.__contains__('INSERT'):
                kvs = self.get_kv_tuples(query)
                assert len(kvs) == 1
                res = max(res, kvs[0][0])
            elif query[0:2] == "Q1":
                break
        return res

    def parse_init(self, query: str):
        kvs = self.get_kv_tuples(query)
        assert len(kvs) == 1
        k, v = kvs[0]
        self.version_list[k].append(v)

    def parse_return(self, query: str):
        qid, tid = self.get_query_txn_id(query)
        # 如果没有读到值，
        if query.__contains__('null'):
            for op in self.T[tid].ops:
                if op.qid == qid:
                    op.value = None
                    op.ver_index = 0
        else:
            # 遍历读到的所有值
            for k, v in self.get_kv_tuples(query):
                # 如果读到了还没有写的值
                if self.version_list[k].count(v) == 0:
                    return "Read {} that does not exist".format((k, v))

                for op in self.T[tid].ops:
                    if op.qid == qid and op.key == k:
                        op.value = v
                        op.ver_index = self.version_list[k].index(v)

    def parse_finish(self, query: str):
        qid, tid = self.get_query_txn_id(query)
        finish_ts = get_finish_time(query)
        T = self.T[tid]
        if T.begin_qid == qid:
            T.begin_ts = finish_ts
        if T.end_qid == qid and T.failed_reason is None:
            T.end_ts = finish_ts
            T.committed = True

        for op in self.T[tid].ops:
            if op.qid == qid:
                op.finish = finish_ts
                if op.op_type == 'W' or op.op_type == 'I':
                    self.version_list[op.key].append(op.value)
                    op.ver_index = len(self.version_list[op.key]) - 1
                elif op.op_type == 'D':
                    return "Delete is not considered now"

    def parse_failed(self, query: str):
        qid, tid = self.get_query_txn_id(query)
        failed_reason = re.findall(r'failedreason:(.+)', query)
        if failed_reason:
            self.T[tid].failed_reason = failed_reason

    def parse_first_op(self, tid, qid):
        """ 如果是当前事务的第一个读写操作，记录下当前事务的 tid 和 begin_qid """
        T = self.T[tid]  # 引用，可以直接修改
        if T.tid is None:
            T.tid = tid
            T.begin_qid = qid

    def parse_select(self, query: str):
        qid, tid = self.get_query_txn_id(query)
        self.parse_first_op(tid, qid)
        # for some distributed cases which have 4 param, write part is same
        if query.__contains__('value1='):
            # TODO: 分布式的情况
            return "Distributed select is not considered now"
        # for normal cases
        elif query.__contains__('k='):
            k = find_data(query, 'k=')
            op = OP('R', tid, qid, k)
            self.ops_for_key[k].append(op)
            self.T[tid].add(op)
        # for predicate cases
        elif query.__contains__('k>'):
            left = find_data(query, "k>") + 1
            right = find_data(query, "k<")
            for k in range(left, right):
                op = OP('P', tid, qid, k)
                self.ops_for_key[k].append(op)
                self.T[tid].add(op)
        elif query.find("value1>") != -1:
            # TODO: 分布式的情况
            return "Distributed select is not considered now"
        else:
            # it means select all rows in table
            for k in range(self.total_num + 1):
                op = OP('R', tid, qid, k)
                self.ops_for_key[k].append(op)
                self.T[tid].add(op)

    def parse_update(self, query: str):
        qid, tid = self.get_query_txn_id(query)
        self.parse_first_op(tid, qid)
        if query.__contains__('value1='):
            return "Distributed update is not considered now"
        elif query.__contains__('k='):
            k = find_data(query, 'k=')
            v = find_data(query, 'v=')
            op = OP('W', tid, qid, k, v)
            self.ops_for_key[k].append(op)
            self.T[tid].add(op)

    def parse_delete(self, query: str):
        qid, tid = self.get_query_txn_id(query)
        self.parse_first_op(tid, qid)
        if query.__contains__('value1='):
            return "Distributed delete is not considered now"
        elif query.__contains__('k='):
            return "Delete is not considered now"

    def parse_insert(self, query: str):
        qid, tid = self.get_query_txn_id(query)
        self.parse_first_op(tid, qid)
        kvs = self.get_kv_tuples(query)
        assert len(kvs) == 1
        k, v = kvs[0]
        op = OP('I', tid, qid, k, v)
        self.ops_for_key[k].append(op)
        self.T[tid].add(op)

    def parse_commit(self, query: str):
        qid, tid = self.get_query_txn_id(query)
        if qid != 0:
            self.T[tid].end_qid = qid

    def parse_line(self, query: str):
        qid, tid = self.get_query_txn_id(query)
        if qid == 0 and query.__contains__('INSERT'):
            return self.parse_init(query)
        if query.__contains__('returnresult'):
            return self.parse_return(query)
        if query.__contains__('finished'):
            return self.parse_finish(query)
        if query.__contains__('failed'):
            return self.parse_failed(query)
        if query.__contains__('SELECT'):
            return self.parse_select(query)
        elif query.__contains__('UPDATE'):
            return self.parse_update(query)
        elif query.__contains__('DELETE'):
            return self.parse_delete(query)
        elif query.__contains__('INSERT'):
            return self.parse_insert(query)
        elif query.__contains__('COMMIT'):
            return self.parse_commit(query)

    def remove_unfinished_operation(self):
        """ 删除所有未完成的，或者事务未成功提交的操作"""
        filt = lambda op: op.finish is not None and self.T[op.tid].committed
        for i in range(len(self.ops_for_key)):
            self.ops_for_key[i] = list(filter(filt, self.ops_for_key[i]))

    def is_concurrency(self, op1: OP, op2: OP):
        T1 = self.T[op1.tid]
        T2 = self.T[op2.tid]
        if T2.end_ts < T1.begin_ts or T2.begin_ts > T1.end_ts:
            return False
        return True

    def get_edge_type(self, op1: OP, op2: OP):
        state = ''
        if op2.finish > self.T[op1.tid].end_ts:
            state = 'C'
        return op1.op_type + state + op2.op_type

    def is_write(self, op: OP):
        return op.op_type in WTypes

    def is_read(self, op: OP):
        return op.op_type in RTypes

    def insert_edge(self, op1: OP, op2: OP):
        insert_flag = False
        if self.is_write(op1) and self.is_write(op2):
            if op1.ver_index + 1 == op2.ver_index:
                insert_flag = True
        elif self.is_read(op1) and self.is_write(op2):
            if op1.ver_index + 1 == op2.ver_index:
                insert_flag = True
        elif self.is_write(op1) and self.is_read(op2):
            if op1.ver_index == op2.ver_index:
                insert_flag = True

        if insert_flag:
            edge_type = self.get_edge_type(op1, op2)
            self.indegree[op2.tid] += 1
            self.edge[op1.tid].append(Edge(edge_type, op2.tid))

    def build_graph(self):
        # 对每个变量建边
        for ops in self.ops_for_key:
            for op1 in ops:
                for op2 in ops:
                    # 0. op1 和 op2 至少有一个写操作
                    if self.is_read(op1) and self.is_read(op2):
                        continue
                    # 1. op1 和 op2 不是同一个事务
                    if op1.tid == op2.tid:
                        continue
                    # 2. op1 和 op2 是并发事务
                    if not self.is_concurrency(op1, op2):
                        continue
                    # 3. op1 的 version <= op2 的version
                    if op1.ver_index > op2.ver_index:
                        continue
                    # 4. 边来自于相邻的 version 或者相同的 version
                    if op2.ver_index - op1.ver_index > 1:
                        continue
                    self.insert_edge(op1, op2)

    def check_cycle(self, total):
        """ 拓扑排序，判断是否有环"""
        q = queue.Queue()
        for i, degree in enumerate(self.indegree):
            if degree == 0:
                q.put(i)

        ans = []
        while not q.empty():
            now = q.get()
            ans.append(now)
            for val in self.edge[now]:
                next_node = val.out
                self.indegree[next_node] -= 1
                if self.indegree[next_node] == 0:
                    q.put(next_node)
        if len(ans) == total:
            return False
        return True

    def dfs(self, now: int, type):
        self.visit1[now] = 1
        if self.visit[now] == 1: return
        self.visit[now] = 1
        self.path.append(now)
        self.edge_type.append(type)
        for v in self.edge[now]:
            if self.visit[v.out] == 0:
                self.dfs(v.out, v.type)
            else:
                self.path.append(v.out)
                self.edge_type.append(v.type)
                with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
                    for i in range(0, len(self.path)):
                        redirect_output(f.write, str(self.path[i]))
                        if i != len(self.path) - 1:
                            redirect_output(f.write, "->" + self.edge_type[i + 1] + "->")
                    redirect_output(f.write, "\n\n")
                self.path.pop()
                self.edge_type.pop()
        self.path.pop()
        self.edge_type.pop()
        self.visit[now] = 0

    def check(self):
        print("-" * 60)
        print("Start checking ", self.file)
        go_end = False  # 如果不是所有的事务的所有操作都正常执行，就不进行 check
        for query in self.lines:
            if not re.findall(r'Q\d+-T\d+', query):
                continue

            if query.__contains__('Rollback') or query.__contains__('Timeout') or query.__contains__('failed reason'):
                go_end = True

            print('Parsing', query)
            error = self.parse_line(query)
            if error is not None:
                break

        if error is not None:
            output_result(file, result_folder, ts_now, "Error")
            print_error(result_folder, ts_now, error)
            return

        cycle = False
        self.remove_unfinished_operation()
        self.build_graph()
        if not go_end:
            cycle = self.check_cycle(self.total_num + 2)
        if cycle:
            output_result(self.file, self.result_folder, self.ts_now, "Cyclic")
            for i in range(self.total_num + 2):
                if not self.visit1[i]:
                    self.dfs(i, 'null')
        else:
            output_result(self.file, self.result_folder, self.ts_now, 'Avoid')
            print_path(self.result_folder, self.ts_now, self.edge)


if __name__ == '__main__':
    result_folder = "check_result/"
    ts_now = time.strftime("%Y%m%d_%H%M%S", time.localtime())

    if not os.path.exists(result_folder):
        os.makedirs(result_folder)

    directory = './pg-serializable'
    files = []
    for file in os.listdir(directory):
        file = os.path.join(directory, file)
        files.append(file)

    # files = ['./test-result/IP0-ICP1-WP1.txt', './test-result/IR0-ICP1-WCP1.txt']
    # files = ['./test-result/IP0-ICP1-WP1.txt']

    checker_cache = dict()
    for file in files:
        checker = Checker(file, result_folder, ts_now)
        checker.check()
        checker_cache[file] = checker
