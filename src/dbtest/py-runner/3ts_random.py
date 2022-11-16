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

import argparse
from typing import List

op_set = ["P", "R", "W", "I"]  # 允许的操作
illegal_op_set = {"RR", "RP", "PR", "PP"}  # 不允许的操作. 边一定要有至少一个写操作


class RandomListGenerator:
    """ 遍历所有可能的 POP 环图

    :ivar n_var: 操作变量数量
    :ivar n_txn: 并行事务数量
    :ivar do_test_list: 输出文件路径
    :ivar pop_graphs: 满足条件的 POP 环图集合
    :ivar visit: 某个变量组合是否被访问到
    """

    def __init__(self, n_var, n_txn, do_test_list) -> None:
        super().__init__()
        self.n_var = n_var
        self.n_txn = n_txn
        self.pop_graphs = []
        self.visit = set()
        self.do_test_list = do_test_list

    def write(self):
        with open(self.do_test_list, "w") as f:
            f.writelines('\n'.join(self.pop_graphs) + '\n')

    def gen(self):
        var_counter = [1] * self.n_var
        self.allocating(var_counter)
        print("[INFO] Get {} POP Graphs".format(self.pop_graphs.__len__()))

    def dfs(self, var_counter: List[int], res: list) -> None:
        """ DFS 遍历所有可能的组合
        :param var_counter: 每个变量剩余能被用到的次数
        :param res: 记录中间结果
        """
        if len(res) == self.n_txn:
            pop_graph = '-'.join(res)
            self.pop_graphs.append(pop_graph)
            return
        for i in range(self.n_var):
            if var_counter[i] != 0:
                var_counter[i] -= 1
                for op1 in op_set:
                    for op2 in op_set:
                        op = op1 + op2

                        if op in illegal_op_set:
                            continue

                        op += str(i)
                        res.append(op)
                        self.dfs(var_counter.copy(), res)
                        res.pop()

                        # 只有 res 非空的时候才能添加 C 边. 因为
                        if res:
                            res1 = res.copy()
                            opC = op1 + 'C' + op2 + str(i)
                            res1.append(opC)
                            self.dfs(var_counter.copy(), res1)
                            res1.pop()

    def allocating(self, var_counter: List[int]) -> None:
        """ 给各个变量分配出现次数并依据出现次数生成环图
        :param var_counter: 长度为 n_var 的列表. var_counter[i] 表示变量 i 被用到几次

        example:
        3 变量 5 事务的样例. 初始化 var_counter = [1, 1, 1] 表明每个变量都用到一次,
        但是 5 事务还需要 2 个变量, 因此相当于 DFS 出
           [1 1 0], [1 0 1], [0 1 1], [2 0 0], [0 2 0], [0 0 2] 这 6 种分配可能
        之后，对于每一种可能进行环生成
        """
        if sum(var_counter) == self.n_txn:
            s = ''.join(map(str, var_counter))
            if s not in self.visit:
                self.visit.add(s)
                res = []
                self.dfs(var_counter.copy(), res)
            return

        for i in range(self.n_var):
            var_counter[i] += 1
            self.allocating(var_counter)
            var_counter[i] -= 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--var", default=2, help="操作变量数量")
    parser.add_argument("-c", "--txn", default=3, help="并行事务数量")
    parser.add_argument(
        "-o", "--out", default="do_test_list.txt", help="输出文件路径")

    args = parser.parse_args()

    gen = RandomListGenerator(args.var, args.txn, args.out)
    gen.gen()
    gen.write()
