import sys

import socket
import time
import logging
from threading import Thread
import snap_onoffs_contants

PROGRAM_STATE_RUN   = 1
PROGRAM_STATE_PAUSE = 2
PROGRAM_STATE_QUIT  = 3
program_state = PROGRAM_STATE_RUN
SOCKET_PORT = 13333

def set_state(state):
    global program_state
    program_state = state

def get_state():
    global program_state
    return program_state

# The server that listens for commands
def server_listen(port=SOCKET_PORT):

    global program_state

    logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)

    logger.info("Opening socket and listening")
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serversocket.bind((socket.gethostname(), port))
    serversocket.listen(2)

    try:
        while True:
            (clientsocket, address) = serversocket.accept()
            logger.info("Server connection accepted")
            msg = ''
            while(True):
                msg += clientsocket.recv(1)
                if not "\n" in msg:
                    continue
                break
            logger.info("Message received: %s" % msg)
            # do something with msg
            if msg.startswith('kill'): #Kill this socket
                logger.info("Killing the command server")
                clientsocket.send("Killing the command server")
                clientsocket.close()
                serversocket.close()
                break
            if msg.startswith('quit'):
                program_state = PROGRAM_STATE_QUIT
                logger.info("Setting program_state = PROGRAM_STATE_QUIT")
                clientsocket.send("Setting program_state = PROGRAM_STATE_QUIT")
                #clientsocket.close()
                #serversocket.close()
                #break
            if msg.startswith('pause'):
                print "pause"
                program_state = PROGRAM_STATE_PAUSE
                logger.info("Setting program_state = PROGRAM_STATE_PAUSE")
                clientsocket.send("Setting program_state = PROGRAM_STATE_PAUSE")
                clientsocket.close()
            if msg.startswith('run'):
                print "run"
                program_state = PROGRAM_STATE_RUN
                logger.info("Setting program_state = PROGRAM_STATE_RUN")
                clientsocket.send("Setting program_state = PROGRAM_STATE_RUN")
                clientsocket.close()
    except KeyboardInterrupt:
        logger.info("Server connection closed, keyboard interrupt")
        serversocket.close()
        raise
    except:
        logger.info("Server connection closed")

def server_close():

    logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)

    try:
        logger.info("Closing server socket")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((socket.gethostname(), 13333))
        sock.send("quit\n");
        data = sock.recv(100);
        sock.close()
    except:
        pass

def client_msg(msg, port=SOCKET_PORT):
    logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((socket.gethostname(), port))
    logger.info("Sending messge %s" % msg)
    sock.send(msg + "\n");
    data = sock.recv(100);
    sock.close()

def server_thread(port=SOCKET_PORT):

    t = Thread(target=server_listen, args=(port, ))
    t.start()

    return t


if __name__== "__main__":

    logger = logging.getLogger(snap_onoffs_contants.LOGGING_NAME)
    logger.setLevel(logging.INFO)
    sh = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter('[%(asctime)-15s] %(message)s')
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    if(len(sys.argv) == 1): 
        try:
            t = server_thread()
            t.join
        except KeyboardInterrupt:
            server_close()
        except:
            server_close()
    else:
        client_msg(sys.argv[1])
