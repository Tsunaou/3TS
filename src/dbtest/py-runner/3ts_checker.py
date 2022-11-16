# -*- coding: utf-8 -*-
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
import sys
from typing import List, Tuple, Optional

WTypes = {'W', 'D', 'I'}  # 写操作的集合
RTypes = {'R', 'P'}  # 读操作的集合

debug_flag = False  # 如果为真，结果不输出到文件，而是打印到屏幕


def debug_print(*args, **kwargs):
    if debug_flag:
        print(*args, **kwargs)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class Edge:
    """ 表示 POP Graph 中的一条边
    :ivar type: 边的类型 例如 ICW, WR
    :ivar out: 边指向的事务 tid
    """

    def __init__(self, edge_type: str, out: int):
        self.type: str = edge_type
        self.out: int = out

    def __str__(self):
        return "-{}->{}".format(self.type, self.out)


class OP:
    """ 表示执行结果中的一个操作
    :ivar op_type: 操作的类型，例如 'R', 'W', 'I', 'D', 'P'
    :ivar tid: 事务的 ID. 例如 Q0-T1 中, tid = 1
    :ivar qid: 查询语句的编号. 例如 Q0-T1 中, qid = 0
    :ivar key: 操作的对象
    :ivar value: 操作的值
    :ivar finish: 标志是否完成，如果完成，则为操作完成的时间戳，否则为 None
    :ivar ver_index: 表示操作了 key 上的第几个 version 的数据
    """

    def __init__(self, op_type, tid, qid, key, value=None, finish=None) -> None:
        super().__init__()
        self.op_type = op_type
        self.tid = tid
        self.qid = qid
        self.key = key
        self.value = value
        self.finish = finish
        self.ver_index = None  # 操作了 key 上的第几个 version 的数据

    def __str__(self) -> str:
        return "Q{}-T{}: {}({}, {}) with ver {}, at {}".format(
            self.qid, self.tid, self.op_type, self.key, self.value, self.ver_index, self.finish)


class Txn:
    """ 表示执行结果中的一个事务
    :ivar tid: 事务的 ID. 例如 Q0-T1 中, tid = 1
    :ivar begin_qid: 事务开启的语句编号. 例如 Q0-T1 中, qid = 0. 这里取事务中第一个读/写操作的值
    :ivar end_qid: 事务结束的语句编号. 这里取执行 Commit 语句的为基准
    :ivar begin_ts: 事务的开始时间戳. 以第一个操作 (即 begin_qid 的语句) 的 finish time 为基准
    :ivar end_ts: 事务的提交时间戳. 以未发生 failure 事件事务的 Commit 语句的 finish time 为基准
    :ivar ops: 事务包含操作
    :ivar failed_reason: 事务未成功提交的原因. 即为 ODBC 执行中抛出的异常
    :ivar committed: 事务是否正常提交
    """

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
        return "T{}: [begin: {}, end: {}]".format(self.tid, self.begin_ts, self.end_ts)


def find_data(query: str, pivot: str):
    """ 返回某个查询语句中，出现在 pivot 后第一个数字
    :param query: 查询语句
    :param pivot: 分割符
    :return:
    """
    pos = query.find(pivot)
    if pos == -1:
        return pos
    pos += len(pivot)
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
    """ 重定向文件输出. 如果 debug_flag 为真，会
    :param func: 原定的写文件的函数
    :param content: 写文件内容
    :return:
    """
    debug_print(content, end='')
    func(content)


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
    """ 用于检测某次测试执行输出是否有环

    :ivar input: 包含测试执行日志的文件路径
    :ivar output: 检测结果的输出路径

    :ivar lines: 读入的执行日志序列. 取出了源文件中对应行的所有空格和 \n
    :ivar T: 事务 Transaction 的集合. T[i] 表示事务 Ti，有效事务从 T1 开始
    :ivar ops_for_key: 每个 key 上操作的集合. ops_for_key[k] 表示变量 k 的所有操作
    :ivar version_list: 每个 key 上版本序的集合. version_list[k] 表示 k 上的版本序; 初始化为 None
    :ivar edges_for_key: 从某个 key 出发的指向其他 key 的边的集合
    :ivar indegree: 记录节点的入度
    :ivar considered: 记录某个节点是否已经被 DFS 遍历过
    :ivar visit: 用于 DFS 找路时方式遍历相同节点
    """

    def __init__(self, input: str, output: str) -> None:
        super().__init__()
        self.file = input
        self.output = output
        self.lines = None

        init_flag = self.read_file()
        if init_flag is False:
            return

        self.total_num = self.get_total_num()  # 在初始 INSERT 语句中出现的最大的 k

        key_range = self.total_num + 2  # 检测中可能出现的 key 的范围. 开大一些范围，防止越界
        self.T: List[Txn] = [Txn() for i in range(
            key_range)]  # T[i] 表示事务 Ti，有效事务从 T1 开始
        self.ops_for_key: List[List[OP]] = [
            [] for i in range(key_range)]  # ops_for_key[k] 表示变量 k 的所有操作
        self.version_list: List[List[Optional[int]]] = [[None] for i in
                                                        range(key_range)]  # version_list[k] 表示 k 上的版本序; 初始化为 None

        # 建边 检测相关
        # 从某个 key 出发的指向其他 key 的边的集合.
        self.edges_for_key: List[List[Edge]] = [[] for i in range(key_range)]
        # DFS 找环相关
        self.indegree = [0] * key_range  # 记录节点的入度
        self.considered = [False] * key_range  # 记录某个节点是否已经被 DFS 遍历过
        self.visit = [False] * key_range  # 用于 DFS 找路时方式遍历相同节点
        self.path = []  # 记录一次 DFS 时的路径，用于找环
        self.edge_type = []  # 记录 DFS 路径表示的边的种类

        self.init = True

    def read_file(self) -> bool:
        try:
            with open(self.file, 'r') as f:
                lines = f.readlines()
            if lines is None:
                eprint('[ERROR] file {} is empty'.format(self.file))
                self.init = False
                return False
        except FileNotFoundError as e:
            eprint('[ERROR] file {} not exists'.format(self.file))
            return False

        self.lines = list(map(lambda x: x.strip('\n').replace(' ', ''), lines))
        return True

    def print_path(self):
        with open(self.output, "a+") as f:
            flag = 0
            for i in range(len(self.edges_for_key)):
                for v in self.edges_for_key[i]:
                    if flag == 0:
                        flag = 1
                    else:
                        redirect_output(f.write, ", ")
                    redirect_output(f.write, str(i) + "->" +
                                    v.type + "->" + str(v.out))
            redirect_output(f.write, "\n\n")

    def output_result(self, cyclic):
        with open(self.output, "a+") as f:
            redirect_output(f.write, self.file + ": " + cyclic + "\n")

    def print_error(self, error_msg):
        with open(self.output, "a+") as f:
            redirect_output(f.write, error_msg + "\n")
            redirect_output(f.write, "\n\n")

    def init_successfully(self) -> bool:
        return self.init

    def get_kv_tuples(self, query: str) -> List[Tuple[int, int]]:
        """ 解析语句中形如 (1,1) (2,2) 这样的元组并以列表的形式返回
        :param query:
        :return:
        """
        kvs = re.findall(r'\(\d+,\d+\)', query)
        kvs = [eval(kv) for kv in kvs]
        return kvs

    def get_query_txn_id(self, query) -> Tuple[int, int]:
        indexes = re.findall(r'Q(\d+)-T(\d+)', query)
        assert len(indexes) == 1
        qid = int(indexes[0][0])
        tid = int(indexes[0][1])
        return qid, tid

    def get_total_num(self) -> int:
        """ 返回执行历史中 Q0-T1 中 insert 的最大变量. 该值用于初始化数组

        例如 insert (0,0) (2,2) (4,4) (6,6) 结果就是 6
        当然这里的处理有点特例化，因为根据样例生成的规则，后续 insert 的 key 不会超过出初始化时范围

        :return: 执行历史中 Q0-T1 中 insert 的最大变量
        """
        res = 0
        for query in self.lines:
            if query.startswith("Q0") and query.__contains__('INSERT'):
                kvs = self.get_kv_tuples(query)
                assert len(kvs) == 1
                res = max(res, kvs[0][0])
            elif query.startswith("Q1"):
                break
        return res

    def parse_init(self, query: str) -> None:
        """ 解析初始事务的 Insert 操作 """
        kvs = self.get_kv_tuples(query)
        assert len(kvs) == 1
        k, v = kvs[0]
        # 初始的 insert 操作给 k 上增加一个新的版本 v
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
        """ 解析日志中的一行
        :param query:
        :return:
        """
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
        def filt(op): return op.finish is not None and self.T[op.tid].committed
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
            self.edges_for_key[op1.tid].append(Edge(edge_type, op2.tid))

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
                    # TODO(tsunaouyang): 这个是否有必要？
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
        """ 根据拓扑排序，判断是否有环 """
        q = queue.Queue()
        for i, degree in enumerate(self.indegree):
            if degree == 0:
                q.put(i)

        ans = []
        while not q.empty():
            now = q.get()
            ans.append(now)
            for val in self.edges_for_key[now]:
                next_node = val.out
                self.indegree[next_node] -= 1
                if self.indegree[next_node] == 0:
                    q.put(next_node)
        if len(ans) == total:
            return False
        return True

    def dfs(self, now: int, etype: str):
        self.considered[now] = True
        if self.visit[now]:
            return
        self.visit[now] = True
        self.path.append(now)
        self.edge_type.append(etype)

        for edge in self.edges_for_key[now]:
            if not self.visit[edge.out]:
                self.dfs(edge.out, edge.type)
            else:
                self.path.append(edge.out)
                self.edge_type.append(edge.type)

                with open(self.output, "a+") as f:
                    for i in range(0, len(self.path)):
                        redirect_output(f.write, str(self.path[i]))
                        if i != len(self.path) - 1:
                            redirect_output(
                                f.write, "->" + self.edge_type[i + 1] + "->")
                    redirect_output(f.write, "\n\n")

                self.path.pop()
                self.edge_type.pop()
        self.path.pop()
        self.edge_type.pop()
        self.visit[now] = False

    def check(self):
        debug_print("-" * 60)
        debug_print("Start checking ", self.file)
        go_end = False  # 如果不是所有的事务的所有操作都正常执行，就不进行 check
        error_msg = None

        for query in self.lines:
            if not re.findall(r'Q\d+-T\d+', query):
                continue

            # 如果执行历史有明确的终止，就不进行环检测
            # TODO(Tsunaouyang): 其实存在一种可能 -> 一个并发的事务 fail 了但是导致其他正常执行的错误出现了数据异常
            if query.__contains__('Rollback') or query.__contains__('Timeout') or query.__contains__('failed reason'):
                go_end = True

            # debug_print('Parsing', query)
            error_msg = self.parse_line(query)
            if error_msg is not None:
                break

        if error_msg is not None:
            self.output_result('Error')
            self.print_error(error_msg)
            return

        cycle = False
        self.remove_unfinished_operation()
        self.build_graph()
        key_range = self.total_num + 2
        if not go_end:
            cycle = self.check_cycle(key_range)
        if cycle:
            self.output_result('Cyclic')
            for i in range(key_range):
                if not self.considered[i]:
                    self.dfs(i, 'null')
        else:
            self.output_result('Avoid')
            self.print_path()


if __name__ == '__main__':
    result_folder = "../check_result/mariadb/"
    ts_now = time.strftime("%Y%m%d_%H%M%S", time.localtime())

    if not os.path.exists(result_folder):
        os.makedirs(result_folder)

    directory = '../databases/mariadb/mariadb/repeatable-read'
    files = []
    for file in os.listdir(directory):
        file = os.path.join(directory, file)
        files.append(file)

    # files = ['./test-result/IP0-ICP1-WP1.txt', './test-result/IR0-ICP1-WCP1.txt']
    # files = ['./test-result/IP0-ICP1-WP1.txt']

    checker_cache = dict()
    result_output = result_folder + "/check_result" + ts_now + ".txt"
    for file in files:
        checker = Checker(file, result_output)
        checker.check()
        checker_cache[file] = checker
