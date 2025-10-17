import sys
import os
from socket import *
import threading
import time
import queue
import pickle
import random
import select

"""
References used:
Youtube -> https://www.youtube.com/watch?v=LnbvhoxHn8M
Book -> Computer Networks: a Systems Approach, sections 2.5
"""

class Window:
    """
    Class to abstract a sliding window
    It assumes an infinite sequence number instead of one constrained by the window size (1,2,3,4,5,.....)
    """ 

    __window = []
    __windowCond = threading.Condition()
    __closed = False
    __windowSize = 0
    __nSeq = 0

    def __init__(self, windowSize : int) -> None:
        self.__windowSize = windowSize

    def ack_old(self) -> None: 
        """
        Acknowledges the oldest block in the window (dequeue)
        will notify anyone that tried to append a frame to the window but got blocked doing so
        """
        self.__raise_if_closed__()
        with self.__windowCond:
            self.__window.pop(0)
            self.__windowCond.acquire()
            self.__windowCond.notify()
            print("Ive notified all locks")

    def append(self, bytes_to_add : bytes) -> None:
        """
        Append a frame to the window
        """
        self.__raise_if_closed__()
        with self.__windowCond:
            if (len(self.__window) == self.__windowSize): # If someone tries adding something to the window it is blocked if the window is full
                print("Ive been locked")
                self.__windowCond.wait() 
                print("Ive been freed")
            self.__window.append((self.__nSeq,bytes_to_add))
            self.__nSeq += 1

    def last(self) -> tuple[int, bytes]:
        """Return the last block in the queue"""
        self.__raise_if_closed__()
        return self.__window[len(self.__window) - 1]
    
    def expected_ack(self) -> int:
        """
        Returns the expected ack number (nSeq of the oldest frame in the window, first element)
        """
        return self.__window[0][0]
    
    def close(self) -> None:
        """
        Closes the window and thus if anyone tries to add anything an exception will be raised
        Iterator -> Will work until the window is empty then will raise an exception
        Append -> Wont work exception is raised
        Last -> Will work until the window is empty then will raise an exception
        Ack_old -> Will work until the window is empty then will raise an exception
        """
        self.__closed = True

    def empty(self) -> bool:
        return len(self.__window) == 0

    def __raise_if_closed__(self):
        if(self.__closed and len(self.__window) == 0):
            raise ValueError("Window is Closed")

    
    def __iter__(self): # Return an iterator for the window (If one needs to go through all the blocks to retransmit them all again)
        self.__raise_if_closed__()
        return iter(self.__window)
    


#def sendDatagram( blockNo, contents, sock, end ):
#    rand = random.randint(0,9)
#    if rand > 1:
#        toSend = (blockNo, contents)
#        msg = pickle.dumps( toSend)
#        sock.sendto( msg, end)

def sendDatagram (msg, sock, address):
    # msg is a byte array ready to be sent
    # Generate random number in the range of 1 to 10
    rand = random.randint(1, 10)
    if rand > 2:
        print("I sent a datagram")
        sock.sendto(msg, address)
    else:
        print("I failed to send a datagram")

def waitForAck( s, timeout ):
    rx, tx, er = select.select( [s], [],[], timeout)
    return rx!=[]


def tx_thread(s : socket, receiver, window : Window, timeout : float ):
    def send_all_window():
        # Send the whole window
        for nSeq, data in window:
            message = (nSeq, data)
            msg = pickle.dumps(message)
            sendDatagram(msg, s, receiver)

    try:
        while True:
            if waitForAck(s, timeout):
                buf, rem = s.recvfrom( 256 )
                req = pickle.loads(buf)
                if req[0] == window.expected_ack() - 1: # Wrong ack condition (aka receiver didnt receive the blocks sent)
                    #Send all window
                    print("The receiver sent me an incorrect ack number I will send the whole window")
                    send_all_window()

                else: # Right ack condition (Receiver received all the blocks and acknowledged them) "Block for everything went right"
                    dif = req[0] - window.expected_ack() + 1 # if expected ack is equal to the one received 
                    #then dequeue only one block but if it is cumulative we will have to dequeue the difference
                    print("The receiver as received blocks to {}".format(req[0]))
                    print("I will now ack this blocks")
                    for _ in range(dif):
                        window.ack_old()

            else: # Timeout conditon
                # Send the whole window
                print("A timeout happened i will send the whole window")
                send_all_window()

    except ValueError:
        print("Window closed and empty shuting tx_thread down")            
        
def sendBlock(s : socket, receiver, window : Window, fileBytes : bytes):  #producer
    #TO DO
    window.append(fileBytes)
    request = window.last()
    req = pickle.dumps(request)
    sendDatagram(req, s, receiver)

def main(hostname, senderPort, windowSize, timeOutInSec):
    s = socket( AF_INET, SOCK_DGRAM)
    s.bind((hostname, senderPort))
    print("Server running on port {}, {}".format(sys.argv[1], gethostbyname(gethostname())))
    # interaction with receiver; no datagram loss
    buf, rem = s.recvfrom( 256 )
    req = pickle.loads( buf)
    fileName = req[0]
    blockSize = req[1]
    result = os.path.exists(fileName)
    if not result:
        print(f'file {fileName} does not exist in server')
        reply = ( 1, 0 )
        rep=pickle.dumps(reply)
        s.sendto( rep, rem )
        sys.exit(1)
    fileSize = os.path.getsize(fileName)
    reply = ( 0, fileSize)
    rep=pickle.dumps(reply)
    s.sendto( rep, rem )

    # file transfer; datagram loss possible

    window = Window(windowSize)

    tid = threading.Thread( target=tx_thread,
                            args=(s,rem, window, timeOutInSec))
    tid.daemon = True

    tid.start()
    f = open( fileName, 'rb')
    blockNo = 1

    while True:
        b = f.read( blockSize )
        sizeOfBlockRead = len(b)
        if sizeOfBlockRead > 0:
            sendBlock(s, rem, window, b)
        if sizeOfBlockRead == blockSize:
            blockNo=blockNo+1
        else:
            break

    print("I have finished sending i will now close the window and the file")

    f.close()
    window.close()
    tid.join()


if __name__ == "__main__":
    # python sender.py senderPort windowSize timeOutInSec
 

    if len(sys.argv) != 4:
        print("Usage: python sender.py senderPort windowSize timeOutInSec")
    else:
        senderPort = int(sys.argv[1])
        windowSize = int(sys.argv[2])
        timeOutInSec = int(sys.argv[2])
        hostname = gethostbyname(gethostname())
        random.seed( 5 )
        main( hostname, senderPort, windowSize, timeOutInSec)
