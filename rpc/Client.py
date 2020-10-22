import xmlrpc.client

class Client:
    def __init__(self, networkConfig):
        self.url = self.getURL(networkConfig)
        self.stub = xmlrpc.client.ServerProxy(self.url)
        try:
            if len(self.stub.system.listMethods()) > 0:
                print('Connected to RPC server at', self.url)
        except:
            print('Server not running')
    def getURL(self, config):
        return 'http://' + config[0] + ':' + str(config[1])
    
    def run(self, method, *args):
        try:
            serverMethod = getattr(self.stub, method)
            serverMethod(*args)
        except xmlrpc.client.Fault as f:
            print(f)
            print("Requested method not found")
        except Exception as e:
            print("Unknown error")
            print(e)
            

def main():
    client = Client(('localhost', 8000))  
    # client.run('kill') 
    
if __name__ == '__main__':
    main()
