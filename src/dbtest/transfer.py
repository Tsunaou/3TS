import sys
import re
import os


def parse_table_name(stmt: str):
    table_num = re.findall(r't(\d+)', stmt)[0]
    return "t{}".format(table_num)


def parse_value_tuple(insert: str):
    value_tuple = re.findall(r'\(\d+,\s*\d+\)', insert)[0]
    return value_tuple


def parse_update_value(update: str):
    update_value = re.findall(r'v=(\d+)', update)[0]
    return update_value


def parse_pred(stmt: str):
    pred = stmt.split('where')[1].strip()
    return pred


def process_line(line: str):
    if line.__contains__('-'):
        seq, tid, stmt = line.split('-')
        # print(seq, tid, stmt)
        stmt = stmt.strip(';').lower()

        if stmt.__contains__('select'):
            table = parse_table_name(stmt)
            if stmt.__contains__('where'):
                pred = parse_pred(stmt)
                res = "{}.select({})".format(table, pred)
            else:
                res = "{}.get(*)".format(table)
        elif stmt.__contains__('insert'):
            table = parse_table_name(stmt)
            value_tuple = parse_value_tuple(stmt)
            res = "{}.put{}".format(table, value_tuple)
        elif stmt.__contains__('update'):
            table = parse_table_name(stmt)
            pred = parse_pred(stmt)
            update_value = parse_update_value(stmt)
            res = "{}.update({},{})".format(table, pred, update_value)
        elif stmt.__contains__('delete'):
            table = parse_table_name(stmt)
            pred = parse_pred(stmt)
            res = "{}.delete({})".format(table, pred)
        elif stmt.__contains__('begin'):
            res = "begin"
        elif stmt.__contains__('commit'):
            res = "commit"
        elif stmt.__contains__('rollback'):
            res = "rollback"
        else:
            sys.stderr.write("[ERROR] Invalid stmt type for our processing program\n")
            sys.stderr.write("-- [STMT] {}\n".format(stmt))
            res = None

        return "{}-{}-{}".format(seq, tid, res)
    else:
        return line


def transfer_testcases(testfile, input_dir, output_dir):
    input_file = os.path.join(input_dir, testfile)
    output_file = os.path.join(output_dir, testfile)

    with open(input_file, 'r') as f:
        raw_cases = list(map(str.strip, f.readlines()))

    end_flag = False
    results = list()
    for line in raw_cases:
        line = line.lower()
        if line.__contains__('paramnum'):
            continue
        if line.__contains__('drop') or line.__contains__('create'):
            continue

        if line.__contains__('serializable'):
            end_flag = True

        if end_flag:
            results.append(line)
        else:
            results.append(process_line(line))

    with open(output_file, 'w') as f:
        for line in results:
            f.write(line)
            f.write('\n')


if __name__ == '__main__':
    base_dir = "t/pred"
    out_dir = "t/mongodb_p"

    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    testfiles = os.listdir(base_dir)
    testfiles.sort()
    for testfile in testfiles:
        print(testfile.replace('.txt', ''))
        transfer_testcases(testfile, base_dir, out_dir)
