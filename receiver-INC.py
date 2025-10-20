import sys
from socket import *
import threading
import select
import time
import math
import queue
import pickle
import random

FIN = -2

STATE_1 = 1
STATE_FIN1 = -1
STATE_FIN2 = -2

TIMEOUT_MAX = 10

def print_important(string : str) -> None:
    print("---",end="")
    for _ in range(len(string)):
        print("-", end="")
    print("---")
    print("   ",end="")

    print(string)
    
    print("---",end="")
    for _ in range(len(string)):
        print("-", end="")
    print("---")


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

    timeouts = 0
    current_state = STATE_1

    nSeq = 1
    while True:
        if timeouts == TIMEOUT_MAX:
            #The sender has not talked for a while closing
            print_important("Timeout Closed")
            break
        if waitForData(s, 1):
            rep, _ = s.recvfrom(bSize+32)
            blockNum, message = pickle.loads(rep)
            
            if current_state == STATE_FIN1 and blockNum == FIN:
                #Received Ack from server close
                print_important("Connection Closed")
                break;
            elif blockNum == FIN:
                # Start closing transmission
                current_state = STATE_FIN1
                print_important("Closing connection")
                sendAck(FIN, s, sender) # Send an ackowledge with the FIN
                for _ in range(10): # Send some FIN to make "sure" the sender receives one, even if the server doesnt itll timeout eventually
                    sendAck(FIN, s, sender) # Send a "data" with the FIN
                    
            elif current_state == STATE_1 and blockNum == nSeq:
                #Received correct block
                #Send Ack
                sendAck(blockNum, s, sender)
                #Put block's data into the queue and slide the window
                que.put(message, timeout = 1)
                nSeq += 1
            else: #Received wrong block send this info to the sender and try again
                sendAck(nSeq - 1, s, sender)
                
            timeouts = 0
        else:
            timeouts += 1
    
def receiveNextBlock( q ):
    try:
        return q.get(timeout = 11)
    except queue.Empty: # If queue did not fill within 11 secs assume thread "died"
        raise Exception("Queue did not receive data in the given ammount of time, closing main thread")

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
    q = queue.Queue()
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

    print_important("Writing finished")
    f.close()
    tid.join()
    print_important("Transfer Finished")
       

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
    
