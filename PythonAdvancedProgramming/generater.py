__author__ = 'jjzhu'


def fun1():
    """
    tokenize标准程序库使用
    :return:
    """
    import tokenize
    reader = open('./iterator.py').__next__()
    tokens = tokenize.generate_tokens(reader)
    print(type(tokens))
    while True:
        try:
            tokens.__next__()
        except StopIteration:
            break


def fun2():
    def power(values):
        for value in values:
            print('powering %s' % str(value))
            yield value

    def adder(values):
        for value in values:
            print('adding to %s' % str(value))
            if value % 2 == 0:
                yield value + 3
            else:
                yield value + 2
    elem = [1, 4, 7, 9, 12, 19]
    res = adder(power(elem))
    while True:
        try:
            print(res.__next__())
        except StopIteration:
            break
if __name__ == '__main__':
    fun2()
