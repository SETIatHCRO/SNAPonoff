#!/usr/bin/python
  
##
# snap_cli.py
# A simple client/server interface for snap_obs.py
##

import select
import socket
import sys
import signal
from communication import send, receive
import time
from threading import Thread
import snap_array_helpers



class SNAPServer(object):
    """ Simple chat server using select """

    def __init__(self, port=3490, backlog=5):
        self.clients = 0
        # Client map
        self.clientmap = {}
        # Output socket list
        self.inputs = []
        self.outputs = []
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(('',port))
        print 'Listening to port',port,'...'
        self.server.listen(backlog)
        # Trap keyboard interrupts
        signal.signal(signal.SIGINT, self.sighandler)

    def sighandler(self, signum, frame):
        # Close the server
        print 'Shutting down server...'
        # Close existing client sockets
        for o in self.outputs:
            o.close()
        self.outputs = [];

        for i in self.inputs:
            i.close()
        self.inputs = [];

        self.server.close()


    def kill(self):
        print 'Shutting down server...'
        for o in self.outputs:
            o.close()
        for i in self.inputs:
            i.close()


    def getname(self, client):

        # Return the printable name of the
        # client, given its socket...
        info = self.clientmap[client]
        host, name = info[0][0], info[1]
        return '@'.join((name, host))

    def serve(self):

        #self.inputs = [self.server,sys.stdin]
        self.inputs = [self.server]
        self.outputs = []
        self.error = []

        running = 1

        while running:

            try:
                #inputready,outputready,exceptready = select.select(self.inputs, self.outputs, self.error)
                inputready,outputready,exceptready = select.select(self.inputs, self.outputs, self.inputs, 1)
            except select.error, e:
                break
            except socket.error, e:
                break

            for s in inputready:

                if s == self.server:
                    # handle the server socket
                    client, address = self.server.accept()
                    print 'snapserver: got connection %d from %s' % (client.fileno(), address)
                    # Read the login name
                    cname = receive(client).split('NAME: ')[1]

                    # Compute client name and send back
                    self.clients += 1
                    send(client, 'CLIENT: ' + str(address[0]))
                    self.inputs.append(client)

                    self.clientmap[client] = (address, cname)
                    # Send joining information to other clients
                    msg = '\n(Connected: New client (%d) from %s)' % (self.clients, self.getname(client))
                    for o in self.outputs:
                        # o.send(msg)
                        send(o, msg)

                    self.outputs.append(client)

                elif s == sys.stdin:
                    # handle standard input
                    junk = sys.stdin.readline()
                    running = 0
                else:
                    # handle all other sockets
                    try:
                        # data = s.recv(BUFSIZ)
                        data = receive(s)
                        if data:
                            # Send as new client's message...
                            msg = '\n#[' + self.getname(s) + ']>> ' + data
                            # Send data to all except ourselves
                            for o in self.outputs:
                                if o != s:
                                    # o.send(msg)
                                    send(o, msg)
                        else:
                            print 'chatserver: %d hung up' % s.fileno()
                            self.clients -= 1
                            s.close()
                            self.inputs.remove(s)
                            self.outputs.remove(s)

                            # Send client leaving information to others
                            msg = '\n(Hung up: Client from %s)' % self.getname(s)
                            for o in self.outputs:
                                # o.send(msg)
                                send(o, msg)

                    except socket.error, e:
                        # Remove
                        self.inputs.remove(s)
                        self.outputs.remove(s)



        self.server.close()
        print "Server has closed down. Bye."


class SNAPClient(object):
    """ A simple command line chat client using select """

    def __init__(self, host='127.0.0.1', port=3490):
        self.name = "snap_client"
        # Quit flag
        self.flag = False
        self.port = int(port)
        self.host = host
        # Initial prompt
        self.prompt='[' + '@'.join((self.name, socket.gethostname().split('.')[0])) + ']> '
        # Connect to server at port
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((host, self.port))
            print 'Connected to chat server@%d' % self.port
            # Send my name...
            send(self.sock,'NAME: ' + self.name) 
            data = receive(self.sock)
            # Contains client address, set it
            addr = data.split('CLIENT: ')[1]
            self.prompt = '[' + '@'.join((self.name, addr)) + ']> '
        except socket.error, e:
            print 'Could not connect to chat server @%d' % self.port
            sys.exit(1)

    def printMenu(self):
        print "Menu:"
        print " m, or menu, or ? - print this menu"
        print " q - quit"

    def handleMenuInput(self, string):
        if(string == "m" or string == "menu" or string == "?"):
             self.printMenu()


    def cmdloop(self):

        running = 1

        while not self.flag:
            try:
                self.printMenu()

                sys.stdout.write(self.prompt)
                sys.stdout.flush()


                # Wait for input from stdin & socket
                try:
                    inputready, outputready,exceptrdy = select.select([0, self.sock], [],[])
                except select.error, e:
                    break
                except socket.error, e:
                    break
                
                for i in inputready:
                    if i == 0:
                        data = sys.stdin.readline().strip()

                        if(data == "q"):
                            print "exiting client. bye.\n"
                            self.sock.close()
                            return
                        else:
                            self.handleMenuInput(data)

                        if data: send(self.sock, data)
                    elif i == self.sock:
                        data = receive(self.sock)
                        if not data:
                            print 'Shutting down.'
                            self.flag = True
                            break
                        else:
                            sys.stdout.write(data + '\n')
                            sys.stdout.flush()
                            
            except KeyboardInterrupt:
                print 'Interrupted.'
                self.sock.close()
                break
            
            
def testThread(snapServer):
    time.sleep(20)
    snapServer.kill()


if __name__ == "__main__":

    import sys

    host = ""
    port = -1

    if len(sys.argv) <= 1:
        print "Syntax: <server|test|...> ..."
        print " For \"test\", which starts the server and kills it after 20 seconds"
        print "   %s test port" % sys.argv[0]
        print " For \"server\""
        print "   %s server port" % sys.argv[0]
        print " For \"client\""
        print "   %s <host> <port>" % sys.argv[0]
        sys.exit()

    if(sys.argv[1] == "test"):
        port = int(sys.argv[2])
        snapServer = SNAPServer(port)
        t = Thread(target=testThread, args=(snapServer,))
        t.start()
        snapServer.serve()
        sys.exit(0)
    elif(sys.argv[1] == "server"):
        port = int(sys.argv[2])
        SNAPServer(port).serve()
        sys.exit(0)
    else:
        host = sys.argv[1]
        port = int(sys.argv[2])
        client = SNAPClient(host, port)
        client.cmdloop()
        sys.exit(0)

