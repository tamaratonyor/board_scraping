"""
@author Usigbe Ataga
Challenge 2:
"""


def check_sum(numbers: int):
    """
    This code requires that the first two values be 9 or less, and last values be 99 or less.
    :param numbers: of types Int to be analyzed
    :return: True if the 2 preceding values sequentially add up to the third following value
    """
    num_str = str(numbers)
    print(f'For {num_str}:')
    val1 = int(num_str[0])
    val2 = int(num_str[1])
    n = 0
    match_flag = True
    for num in num_str[2::]:
        if not n:
            val3 = int(num)
        else:
            val3 = int(str(n) + num)

        if val3 != val1 + val2 and not n:
            n = val3
            continue
        elif val3 != val1 + val2 and n:
            match_flag = False
            n = 0
            continue
        else:
            match_flag = True
            n = 0
            val1 = val2
            val2 = val3
    return match_flag


if __name__ == "__main__":
    print(check_sum(66121830))  # True
    print(check_sum(66131830))  # False
