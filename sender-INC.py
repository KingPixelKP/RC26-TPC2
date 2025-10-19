import sys
import os
from socket import *
import threading
import pickle
import random
import select

blocksInWindow = 0
empty_cond = threading.Condition()
close_cond = threading.Lock()
window = []

def sendDatagram( blockNo, contents, sock, end ):
    rand = random.randint(0,9)
    if rand > 1:
        toSend = (blockNo, contents)
        msg = pickle.dumps( toSend)
        sock.sendto( msg, end)
        
def waitForAck( s, seg ):
    rx, tx, er = select.select( [s], [],[], seg)
    return rx!=[]


def tx_thread( s, receiver, cond, timeout ):

    s1 = True

    def send_all_window():
        # Send the whole window
        for nSeq, data in window:
            sendDatagram(nSeq, data, s, receiver)

    while True:
        if close_cond.locked() and len(window) == 0: 
            break
        if waitForAck(s, timeout):
            buf, rem = s.recvfrom( 256 )
            req = pickle.loads(buf)
            empty_cond.acquire()
            if len(window) == 0:
                print("Waiting for window to fill")
                empty_cond.wait()
            empty_cond.release()
            if req[0] == window[0][0] - 1 and s1:
                print("I passed to state 2")
                s1 = False
                pass
            elif req[0] == window[0][0] - 1 and not s1: # Wrong ack condition (aka receiver didnt receive the blocks sent)
                #Send all window
                print("I received ack {}, wanted {}".format(req[0], window[0][0]))
                print("This is my window")
                for a, _ in window:
                    print("{} ".format(a), end="")
                print()
                send_all_window()
                s1 = True
            else: # Right ack condition (Receiver received all the blocks and acknowledged them) "Block for everything went right"
                    with cond:
                        dif = req[0] - window[0][0] + 1 # if expected ack is equal to the one received 
                        for i in range(dif):
                            print("I acked block {}".format(req[0]+i))
                            window.pop(0)
                        cond.notify()
        else: # Timeout conditon
            # Send the whole window
            print("A timeout happened i will send the whole window")
            send_all_window()
            

def sendBlock( seqNo, fileBytes, s, receiver, windowSize, cond ):  #producer
    with cond:
        if (len(window) == windowSize):
            cond.wait()
        window.append((seqNo, fileBytes))
        sendDatagram(seqNo, fileBytes, s, receiver)
        print("I sent block {}".format(seqNo))
        with empty_cond:
            print("Filled up window")
            empty_cond.notify()

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
        reply = ( -1, 0 )
        rep=pickle.dumps(reply)
        s.sendto( rep, rem )
        sys.exit(1)
    fileSize = os.path.getsize(fileName)
    reply = ( 0, fileSize)
    rep=pickle.dumps(reply)
    s.sendto( rep, rem )
    # file transfer; datagram loss possible
    windowCond = threading.Condition()
    tid = threading.Thread( target=tx_thread,
                            args=(s,rem, windowCond,timeOutInSec))
    tid.start()
    f = open( fileName, 'rb')
    blockNo = 1
    
    while True:
        b = f.read( blockSize  )
        sizeOfBlockRead = len(b)
        if sizeOfBlockRead > 0:
            sendBlock( blockNo, b, s, rem, windowSize, windowCond)
        if sizeOfBlockRead == blockSize:
            blockNo=blockNo+1
        else:
            break
    f.close()
    close_cond.acquire()
    tid.join()
    close_cond.release()
    print("Transmission ended")


if __name__ == "__main__":
    # python sender.py senderPort windowSize timeOutInSec
 

    if len(sys.argv) != 4:
        print("Usage: python sender.py senderPort windowSize timeOutInSec")
    else:
        senderPort = int(sys.argv[1])
        windowSize = int(sys.argv[2])
        timeOutInSec = int(sys.argv[3])
        hostname = gethostbyname(gethostname())
        random.seed( 5 )
        main( hostname, senderPort, windowSize, timeOutInSec)
