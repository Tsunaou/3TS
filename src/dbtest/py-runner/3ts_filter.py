import re
# from re import Pattern

do_test_list_path = '../do_test_list_base.txt'
opt1_list_path = '../opt1.txt'
opt2_list_path = '../opt2.txt'
opt3_list_path = '../opt3.txt'


# invalid conflicts: II IW WI ID DI DD (also need to include 'C', e.g., ICI, in total 12 = 6*2)
invalid_conflicts = [
    r'IC*I', r'IW', r'WC*I', r'IC*D', r'DC*I', r'DC*D',
]

# invalid pattern: WW-WW WW-WR WW-RW WR-WW WR-WR WR-RW RW-WW RW-RW
# (also need to include 'C', e.g., WCW0-WW0 WW-WCW WCW1-WCW1, in total 64 = 8*4(with and without c)*2 (object 0 or 1))
invalid_patterns = [
    r'[W|I]C*[W|I]0-[W|I]C*[W|I]0',
    r'[W|I]C*[W|I]0-[W|I]C*R0',
    r'[W|I]C*[W|I]0-RC*[W|I]0',
    r'[W|I]C*R0-[W|I]C*[W|I]0',
    r'[W|I]C*R0-[W|I]C*R0',
    r'[W|I]C*[W|I]0-RC*[W|I]0',
    r'RC*[W|I]0-[W|I]C*[W|I]0',
    r'RC*[W|I]0-RC*[W|I]0',
    r'[W|I]C*[W|I]1-[W|I]C*[W|I]1',
    r'[W|I]C*[W|I]1-[W|I]C*R1',
    r'[W|I]C*[W|I]1-RC*[W|I]1',
    r'[W|I]C*R1-[W|I]C*[W|I]1',
    r'[W|I]C*R1-[W|I]C*R1',
    r'[W|I]C*[W|I]1-RC*[W|I]1',
    r'RC*[W|I]1-[W|I]C*[W|I]1',
    r'RC*[W|I]1-RC*[W|I]1',
]


def match_pattern(p, testcase: str):
    # testcase : 'IR0-ICR1-ICR1'
    if p.findall(testcase):
        return False
    # 考虑 'ICR1-IR0-ICR1'的情况
    lines = testcase.split('-')
    case2 = "{}-{}-{}".format(lines[1], lines[2], lines[0])
    if p.findall(case2):
        return False
    return True


def report_results(opt0_cases, opt1_cases, opt2_cases, opt3_cases):
    print('-' * 100)
    print('Opt0 number:', len(opt0_cases))
    print('-' * 100)
    print('Opt1 number:', len(opt1_cases))
    print('-' * 100)
    print('Opt2 number:', len(opt2_cases))
    # for case in opt2_cases:
    #     print(case)
    print('-' * 100)
    print('Opt3 number:', len(opt3_cases))
    # for case in opt3_cases:
    #     print(case)


def write_do_test_list(testcases, path):
    with open(path, 'w') as f:
        for case in testcases:
            f.write(case)
            f.write('\n')


if __name__ == '__main__':
    with open(do_test_list_path, 'r') as f:
        opt0_cases = list(map(str.strip, f.readlines()))

    # 统计testcase个数数据
    # OPT1 only remove "P"
    p1 = re.compile('P')
    opt1_cases = list(filter(lambda x: p1.findall(x) == [], opt0_cases))
    # OPT2 remove "P" and remove invalid pattern
    p2 = re.compile('|'.join(invalid_conflicts))
    opt2_cases = list(filter(lambda x: match_pattern(p2, x), opt1_cases))
    # OPT3 remove "P" ， invalid pattern，invalid conflicts
    p3 = re.compile('|'.join(invalid_conflicts))
    opt3_cases = list(filter(lambda x: match_pattern(p3, x), opt2_cases))

    report_results(opt0_cases, opt1_cases, opt2_cases, opt3_cases)

    # write_do_test_list(opt1_cases, opt1_list_path)
    # write_do_test_list(opt2_cases, opt2_list_path)
    # write_do_test_list(opt3_cases, opt3_list_path)

    databases = ['dmdb', 'hgdb',  'oceanbase',
                 'oracle',  'polardb',  'postgres']

    for db in databases:
        path = '../report/{}/cycles.log'.format(db)
        with open(path, 'r') as f:
            cycles = set(map(str.strip, f.readlines()))
        print('Found {} cycles while testing {}'.format(len(cycles), db))
        print('-- {} cycles are remained after opt1'.format(len(cycles.intersection(set(opt1_cases)))))
        print('-- {} cycles are remained after opt2'.format(len(cycles.intersection(set(opt2_cases)))))
        print('-- {} cycles are remained after opt3'.format(len(cycles.intersection(set(opt3_cases)))))

        diffs = set(cycles).difference(opt2_cases)
        for diff in diffs:
            print(diff)
