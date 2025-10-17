import sys
from socket import *
import threading
import time
import queue
import pickle
import random

max_window_size = 1;

def sendAck( ackNo, sock, end ):
    rand = random.randint(0,9)
    if rand > 1:
        toSend = (ackNo,)
        msg = pickle.dumps( toSend)
        sock.sendto( msg, end)

def rx_thread( s, sender, que : queue.Queue, bSize, nSeq):
    try:
        while True:
            rep, ad = s.recvfrom(bSize+32)
            reply = pickle.loads(rep)
            
            if que.full(): #Received a block either good seq or not doesnt matter because the window is full
                print("My window is full but i received a block and thus will ignore it and send an acknowldge")
                sendAck(nSeq-1, s, sender)

                #Try again to listen
            else:  
                
                #Received correct block
                if reply[0] == nSeq:
                    print("I received the correct reply block i will now send an acknowledge packet")
                    #Send Ack
                    sendAck(reply[0], s, sender)

                    #Put block's data into the queue and slide the window
                    que.put(reply[1])
                    nSeq += 1
                    
                else: #Received wrong block send this info to the sender and try again
                    print("I received the wrong reply block i will notify the server and try again")
                    sendAck(nSeq - 1, s, sender)
    except queue.ShutDown:
        print("The queue is shutdown meaning, I will now end rx_thread proccess")
            
def receiveNextBlock( q ):
    return q.get()

def main(sIP, sPort, fNameRemote, fNameLocal, chunkSize):

    s = socket( AF_INET, SOCK_DGRAM)
    #interact with sender without losses
    request = (fNameRemote, chunkSize)
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
    q = queue.Queue(max_window_size)
    nSeq = 0
    tid = threading.Thread( target=rx_thread, args=(s, sender, q, chunkSize, nSeq))
    tid.start()
    # falta testar se existe o ficheiro local TODO
    f = open( fNameLocal, 'wb')
    noBytesRcv = 0
    while noBytesRcv < fileSize:
        print(f'Going to receive; noByteRcv={noBytesRcv}')

        b = receiveNextBlock( q )
        sizeOfBlockReceived = len(b)
        if sizeOfBlockReceived > 0:
            f.write(b)
            noBytesRcv += sizeOfBlockReceived

    print("I have received everything i will now shutdown the queue and close the file")

    f.close()
    q.shutdown()
    tid.join()
       

if __name__ == "__main__":
    # python receiver.py senderIP senderPort fileNameInSender fileNameInReceiver chunkSize
    if len(sys.argv) != 6:
        print("Usage: python receiver.py senderIP senderPort fileNameRemote fileNameLocal chunkSize")
        sys.exit(1)
    senderIP = sys.argv[1]
    senderPort = int(sys.argv[2])
    fileNameRemote = sys.argv[3]
    fileNameLocal = sys.argv[4]
    chunkSize = int(sys.argv[5])
    random.seed( 7 )
    main( senderIP, senderPort, fileNameRemote, fileNameLocal, chunkSize)
    
