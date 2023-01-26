
to_filters = [
    "P",
    "II", "ICI",
    "WI", "WCI",
    "RW0-RW0", "RW1-RW1",
]

if __name__ == '__main__':
    cmd = "cat ./do_test_list_base.txt"
    for pattern in to_filters:
        cmd = cmd + " | grep -v {}".format(pattern)
    print(cmd)
