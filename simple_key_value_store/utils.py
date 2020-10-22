import struct
import Client
import threading


def genSets(n, c):
    total = 0
    sets = []
    for x in range(n):
        D = {"key" + str(i): "value" + str(i) for i in range(total, c + total)}
        sets.append(D)
        total += c
    return sets


def runClient(name, tests):
    client = Client.Client(name=name, tests=tests, debug=True)
    client.run()


def runTests(n, c):
    S = genSets(n, c)
    for i, s in enumerate(S):
        tests = []
        for d in s:
            msg = 'set ' + d + ' ' + \
                str(len(s[d]) + 1) + ' \r\n ' + s[d] + ' \r\n'
            ans = 'NOT-STORED\r\n'
            tests.append([msg.encode(), bytearray(ans.encode())])
            msg = 'set ' + d + ' ' + str(len(s[d])) + ' \r\n ' + s[d] + ' \r\n'
            ans = 'STORED\r\n'
            tests.append([msg.encode(), bytearray(ans.encode())])
            msg = 'get ' + d + ' \r\n'
            ans = 'VALUE ' + d + ' ' + \
                str(len(s[d])) + ' \r\n ' + s[d] + ' \r\n END\r\n'
            tests.append([msg.encode(), bytearray(ans.encode())])
        t = threading.Thread(
            target=runClient, args=("Client" + str(i), tests,))
        t.start()


runTests(5, 5)
