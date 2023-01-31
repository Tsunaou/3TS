import re
# from re import Pattern

# invalid conflicts: II IW WI ID DI DD (also need to include 'C', e.g., ICI, in total 12 = 6*2)
invalid_conflicts = [
    r'IC*I', r'IC*W', r'WC*I', r'IC*D', r'DC*I', r'DC*D',
]

# invalid pattern: WW-WW WW-WR WW-RW WR-WW WR-WR WR-RW RW-WW RW-RW
# (also need to include 'C', e.g., WCW0-WW0 WW-WCW WCW1-WCW1, in total 64 = 8*4(with and without c)*2 (object 0 or 1))
invalid_patterns = [
    r'WC*W0-WC*W0',
    r'WC*W0-WC*R0',
    r'WC*W0-RC*W0',
    r'WC*R0-WC*W0',
    r'WC*R0-WC*R0',
    r'WC*W0-RC*W0',
    r'RC*W0-WC*W0',
    r'RC*W0-RC*W0',
    r'WC*W1-WC*W1',
    r'WC*W1-WC*R1',
    r'WC*W1-RC*W1',
    r'WC*R1-WC*W1',
    r'WC*R1-WC*R1',
    r'WC*W1-RC*W1',
    r'RC*W1-WC*W1',
    r'RC*W1-RC*W1',
]

do_test_list_path = '../do_test_list_base.txt'


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


if __name__ == '__main__':
    with open(do_test_list_path, 'r') as f:
        testcases = list(map(str.strip, f.readlines()))

    # 统计testcase个数数据
    # OPT1 only remove "P"
    p1 = re.compile('P')
    opt1_cases = list(filter(lambda x: p1.findall(x) == [], testcases))
    # OPT2 remove "P" and remove invalid pattern
    p2 = re.compile('|'.join(invalid_conflicts))
    opt2_cases = list(filter(lambda x: match_pattern(p2, x), opt1_cases))
    # OPT3 remove "P" ， invalid pattern，invalid conflicts
    p3 = re.compile('|'.join(invalid_patterns))
    opt3_cases = list(filter(lambda x: match_pattern(p3, x), opt2_cases))

    print('-' * 100)
    print('Original number:', len(testcases))
    print('-' * 100)
    print('Opt1 number:', len(opt1_cases))
    print('-' * 100)
    print('Opt2 number:', len(opt2_cases))
    for case in opt2_cases:
        print(case)
    print('-' * 100)
    print('Opt3 number:', len(opt3_cases))
    for case in opt3_cases:
        print(case)
