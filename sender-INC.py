import sys
import os
from socket import *
import threading
import pickle
import random
import select

"""
References:
https://www.geeksforgeeks.org/computer-networks/tcp-connection-termination/
"""

FIN = -2

STATE_1 = 1
STATE_2 = 2
STATE_FIN1 = -1
STATE_FIN2 = -2

TIMEOUT_MAX = 10
TIMEOUT_MAX_EMPTY_WINDOW = 1

window = []

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

def sendDatagram( blockNo, contents, sock, end ):
    rand = random.randint(0,9)
    if rand > 1:
        toSend = (blockNo, contents)
        msg = pickle.dumps( toSend)
        sock.sendto( msg, end)


#def sendDatagram( blockNo, contents, sock, end ):
#    toSend = (blockNo, contents)
#    msg = pickle.dumps( toSend)
#    sock.sendto( msg, end)
        
def waitForAck( s, seg ):
    rx, _, _ = select.select( [s], [],[], seg)
    return rx!=[]

def tx_thread( s, receiver, cond, windowSize, timeout ):

    timeouts = 0
    current_state = STATE_1

    def expected_ack() -> int:
        return window[0][0]
    
    def last_ack() -> int:
        return expected_ack() - 1
    
    def send_all_window():
        # Send the whole window
        for nSeq, data in window:
            sendDatagram(nSeq, data, s, receiver)

    def remove_all_acked(acked : int):
        # Remove all the acked blocks
        with cond:
            dif = acked - expected_ack() + 1 # if expected ack is equal to the one received 
            for _ in range(dif):
                window.pop(0)
            cond.notify()

    while True:
        if timeouts == TIMEOUT_MAX:
            #The receiver has not answered for a while closing
            print_important("Timeout Closed")
            break
        if waitForAck(s, timeout):
            timeouts = 0
            buf, _ = s.recvfrom( 256 )
            r_ack, = pickle.loads(buf)

            if len(window) != 0:
                if current_state == STATE_FIN1 and r_ack == FIN: #Closing state
                    current_state = STATE_FIN2
                    print_important("Received Fin Ack")
                elif current_state == STATE_FIN2 and r_ack == FIN:
                    #Received close connection from client (All went well closing)
                    sendBlock(FIN, "", s, receiver, windowSize, cond)
                    print_important("Connection Closed")
                    break
                elif current_state == STATE_2: #State 2
                    if r_ack == last_ack():
                        send_all_window()
                    elif r_ack >= expected_ack():
                        remove_all_acked(r_ack)
                    current_state = STATE_1

                elif current_state == STATE_1: #State 1
                    if r_ack == last_ack(): # Wrong ack condition (aka receiver didnt receive the blocks sent)
                        send_all_window()
                        current_state = STATE_2
                    elif r_ack >= expected_ack(): # Right ack condition (Receiver received all the blocks and acknowledged them) "Block for everything went right"
                        remove_all_acked(r_ack)

        else: # Timeout conditon
            timeouts += 1

            if current_state == STATE_2:
                current_state = STATE_1

            if len(window) != 0:
                # Send the whole window
                #print("A timeout happened i will send the whole window")
                send_all_window()
            elif len(window) == 0 and timeouts >= TIMEOUT_MAX_EMPTY_WINDOW:
                # Start closing transmission
                print_important("Closing connection")
                current_state = STATE_FIN1
                sendBlock(FIN, "", s, receiver, windowSize, cond)
            

def sendBlock( seqNo, fileBytes, s, receiver, windowSize, cond ):  #producer
    with cond:
        if (len(window) >= windowSize):
            cond.wait(11)
            if len(window) >= windowSize: # If waited for the 11 secs then assume thread "died" 
                raise Exception("Window did not empty in the time given amount of time, closing main thread")

        window.append((seqNo, fileBytes))
        sendDatagram(seqNo, fileBytes, s, receiver)

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
                            args=(s,rem, windowCond, windowSize, timeOutInSec))
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

    print_important("Reading Finished")
    f.close()
    tid.join()
    print_important("Transfer Finished")


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
