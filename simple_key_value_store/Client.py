import socket
import struct
import time
import sys


class Client(object):
    def __init__(self, networkConfig=('localhost', 12345), tests=None, debug=False, name="Client"):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(5)
        self.networkConfig = networkConfig
        self.tests = tests
        self.debug = debug
        self.name = name 

    def createMessage(self, command, key=None, value=None):
        message = ''
        if command == 'set' or command == 'append':
            if key is not None and value is not None:
                message = command + ' ' + key + ' ' + str(len(value)) + ' \r\n' + value + '\r\n'
            else:
                return None
        elif command == 'get':
            if key is not None:
                message = command + ' ' + key + ' \r\n'
            else:
                return None
        return message.encode()

    # START REFERENCE: https://stackoverflow.com/questions/17667903/python-socket-receive-large-amount-of-data
    def send_msg(self, sock, msg):
        # Prefix each message with a 4-byte length (network byte order)
        msg = struct.pack('>I', len(msg)) + msg
        sock.sendall(msg)

    def recv_msg(self, sock):
        # Read message length and unpack it into an integer
        raw_msglen = self.recvall(sock, 4)
        if not raw_msglen:
            return None
        msglen = struct.unpack('>I', raw_msglen)[0]
        # Read the message data
        return self.recvall(sock, msglen)

    def recvall(self, sock, n):
        # Helper function to recv n bytes or return None if EOF is hit
        data = bytearray()
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data
# END REFERENCE: https://stackoverflow.com/questions/17667903/python-socket-receive-large-amount-of-data

    def send(self, msg):
        msg = struct.pack('>I', len(msg)) + msg
        self.socket.sendall(msg)
    
    def receive(self):
        raw_msglen = self.recvall(self.socket, 4)
        if not raw_msglen:
            return None
        msglen = struct.unpack('>I', raw_msglen)[0]
        # Read the message data
        return self.recvall(self.socket, msglen)

    def run(self):
        self.socket.connect(self.networkConfig)
        clientTests = self.tests
        totalTests = len(clientTests)
        passedTests = 0
        for msg, ans in clientTests:

            self.send_msg(self.socket, msg)
            response = self.recv_msg(self.socket)
            # print('res',response)
            # print('ans',ans)
            if response == ans:
                passedTests += 1
            if self.debug:
                print(self.name, 'result:', 'SUCCESS' if response == ans else 'FAIL')

        print(self.name, 'Tests passed:', passedTests, 'of', totalTests)

        self.socket.close()
        sys.exit()
    
    def connect(self):
        self.socket.connect(self.networkConfig)

    def greet(self):
        print('Client')

    def get(self, key):
        message = self.createMessage('get', key=key)
        # print('message',message)
        self.send(message)
        response = self.receive()
        response = response.decode(encoding='utf-8').split('\r\n')
        required_size = int(response[0].split(' ')[2])
        if required_size == len(response[1]) and required_size > 0:
            return response[1]
        return None

    def set(self, key, value):
        message = self.createMessage('set', key=key, value=value)
        # print('message', message)
        self.send(message)
        response = self.receive()
        # print(response)
    
    def append(self, key, value):
        message = self.createMessage('append', key=key, value=value)
        # print('message', message)
        self.send(message)
        response = self.receive()
        # print(response)


if __name__ == '__main__':
    client = Client()
    client.connect()
    # client.get('key24')
    # client.append('key24', 'lol2')
    # print(client.get('key24'))
    # client.run()
