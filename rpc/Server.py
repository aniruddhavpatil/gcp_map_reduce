from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class Server(object):
    quit = False
    def __init__(self, networkConfig, methods):
        self.server = SimpleXMLRPCServer(
            networkConfig, requestHandler=RequestHandler, allow_none=True, logRequests=False)
        self.server.register_introspection_functions()
        self.methods = methods
        for method in self.methods:
            self.server.register_function(method)
        self.server.register_function(self.kill)
    
    def kill(self):
        self.quit = True

    def run(self):
        while not self.quit:
            self.server.handle_request()


def main():
    server = Server(('localhost', 8000))


if __name__ == '__main__':
    main()
