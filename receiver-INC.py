import sys
from socket import *
import threading
import select
import time
import queue
import pickle
import random

close_cond = threading.Lock()

def sendAck( ackNo, sock, end ):
    rand = random.randint(0,9)
    if rand > 1:
        toSend = (ackNo,)
        msg = pickle.dumps( toSend)
        sock.sendto( msg, end)

def waitForData( s, seg ):
    rx, _, _ = select.select( [s], [],[], seg)
    return rx!=[]

def rx_thread( s, sender, que : queue.Queue, bSize):
    nSeq = 1
    while True:
        if close_cond.locked():
            break
        #time.sleep(0.25)
        if waitForData(s, 1):
            rep, _ = s.recvfrom(bSize+32)
            reply = pickle.loads(rep)
            
            if que.full(): #Received a block either good seq or not doesnt matter because the window is full
                print("Window full acking {}".format(nSeq))
                sendAck(nSeq-1, s, sender)

                #Try again to listen
            else:  
                
                #Received correct block
                if reply[0] == nSeq:
                    #Send Ack
                    sendAck(reply[0], s, sender)

                    #Put block's data into the queue and slide the window
                    que.put(reply[1])
                    nSeq += 1
                    print("I acked block {}".format(reply[0]))
                    
                else: #Received wrong block send this info to the sender and try again
                    print("I received block {}, wanted {}".format(reply[0], nSeq))
                    sendAck(nSeq - 1, s, sender)
    return
    
def receiveNextBlock( q ):
    return q.get()

def main(sIP, sPort, fNameRemote, fNameLocal, blockSize):

    s = socket( AF_INET, SOCK_DGRAM)
    #interact with sender without losses
    request = (fNameRemote, blockSize)
    req = pickle.dumps(request)
    sender = (sIP, sPort)
    print("sending request")
    s.sendto( req, sender)
    print("waiting for reply")
    rep, ad = s.recvfrom(128)
    reply = pickle.loads(rep)
    print(f"Received reply: code = {reply[0]} fileSize = {reply[1]}")
    if reply[0]!=0:
        print(f'file {fNameRemote} does not exist in sender')
        sys.exit(1)
    #start transfer with data and ack losses
    fileSize = reply[1]
    q = queue.Queue(1)
    tid = threading.Thread( target=rx_thread, args=(s, sender, q, blockSize))
    tid.start()
    f = open( fNameLocal, 'wb')
    noBytesRcv = 0
    while noBytesRcv < fileSize:
        print(f'Going to receive; noByteRcv={noBytesRcv}')
        b = receiveNextBlock( q )
        sizeOfBlockReceived = len(b)
        if sizeOfBlockReceived > 0:
            f.write(b)
            noBytesRcv += sizeOfBlockReceived

    f.close()
    close_cond.acquire()
    tid.join()
    close_cond.release()
    print("Transfer finished")
       

if __name__ == "__main__":
    # python receiver.py senderIP senderPort fileNameInSender fileNameInReceiver blockSize
    if len(sys.argv) != 6:
        print("Usage: python receiver.py senderIP senderPort fileNameRemote fileNameLocal blockSize")
        sys.exit(1)
    senderIP = sys.argv[1]
    senderPort = int(sys.argv[2])
    fileNameRemote = sys.argv[3]
    fileNameLocal = sys.argv[4]
    blockSize = int(sys.argv[5])
    random.seed( 7 )
    main( senderIP, senderPort, fileNameRemote, fileNameLocal, blockSize)
    
