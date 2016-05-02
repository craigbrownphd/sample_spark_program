# sample input file
# u0 i0
# u0 i1
# u0 i3
# u1 i2
# u1 i3
# u2 i1
# u2 i2
# u2 i3
# u3 i0
# u3 i1
# u4 i0
# u4 i1
# u4 i2
# u4 i3

def test0():
    FILEPATH = "./data/access0.log"
    NUM_USERS = 5
    NUM_ITEMS = 5
    with open(FILEPATH, 'w') as f:
        for u in range(0,NUM_USERS):
            for i in range(0,NUM_ITEMS):
                if u < i:
                    f.write("user_{} item_{}\n".format(u,i))
test0()

def test1():
    FILEPATH = "./data/access1.log"
    NUM_USERS = 10
    NUM_ITEMS = 10
    NUM_COCLICKED_ITEMS = 10
    with open(FILEPATH, 'w') as f:
        for u in range(0,NUM_USERS):
            for i in range(0,NUM_ITEMS):
                if u < i:
                    f.write("user_{} item_{}\n".format(u,i))
test1()

def test2():
    FILEPATH = "./data/access2.log"
    NUM_USERS = 100
    NUM_ITEMS = 100
    with open(FILEPATH, 'w') as f:
        for u in range(0,NUM_USERS):
            for i in range(0,NUM_ITEMS):
                if u > i:
                    f.write("user_{} item_{}\n".format(u,i))
test2()

def test3():
    FILEPATH = "./data/access3.log"
    NUM_USERS = 6
    NUM_ITEMS = 6
    with open(FILEPATH, 'w') as f:
        for i in range(0,NUM_ITEMS):
            f.write("user_{} item_{}\n".format(0,i))
        for u in range(0,NUM_USERS):
            for i in range(0,NUM_ITEMS):
                if u < i:
                    f.write("user_{} item_{}\n".format(u,i))
test3()
