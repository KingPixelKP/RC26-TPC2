d:
Youtube -> https://www.youtube.com/watch?v=LnbvhoxHn8M
Book -> Computer Networks: a Systems Approach, sections 2.5
"""

class Window:
    """
    Class to abstract a sliding window
    It assumes an infinite sequence number instead of one constrained by the window size (1,2,3,4,5,.....)
    """ 

    __window = []
    __emptyCond = threading.Condition()
    __fullCond = threading.Condition()
    __closed = False
    __windowSize = 0
    __nSeq = 0

    def __init__(self, windowSize : int) -> None:
        self.__windowSize = windowSize
        print("Window created with a windowSize of {}".format(windowSize))

    def ack_old(self) -> None: 
        """
        Acknowledges the oldest block in the window (dequeue)
        will notify anyone that tried to append a frame to the window but got blocked doing so
        """
        self.__raise_if_closed__()
        with self.__emptyCond:
            if self.empty():
                print("Window empty locking")
                self.__emptyCond.wait()
                print("Window isnt empty now unlocking")
            with self.__fullCond:
                self.__window.pop(0)
                self.__fullCond.notify()

    def put(self, bytes_to_add : bytes) -> None:
        """
        Append a frame to the window
        """
        self.__raise_if_closed__()
        with self.__fullCond:
            if self.full(): # If someone tries adding something to the window it is blocked if the window is full
                print("Window full locking")
                self.__fullCond.wait() 
                print("Window isnt full anymore unlocking")
            with self.__emptyCond:
                self.__window.append((self.__nSeq,bytes_to_add))
                self.__nSeq += 1
                self.__emptyCond.notify()

    def last(self) -> tuple[int, bytes]:
        """Return the last block in the queue"""
        self.__raise_if_closed__()
        return self.__window[len(self.__window) - 1]
    
    def expected_ack(self) -> int:
        """
        Returns the expected ack number (nSeq of the oldest frame in the window, first element)
        """
        with self.__emptyCond:
            if self.empty():
                print("Locked in ACK")
                self.__emptyCond.wait()
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
    
    def full(self) -> bool:
        return len(self.__window) == self.__windowSize

    def __raise_if_closed__(self):
        if(self.__closed and len(self.__window) == 0):
            raise ValueError("Window is Closed")

    
    def __iter__(self): # Return an iterator for the window (If one needs to go through all the blocks to retransmit them all again)
        self.__raise_if_closed__()
        with self.__emptyCond:
            if self.empty():
                print("Locked in ITER")
                self.__emptyCond.wait()
        return iter(self.__window)
    

def sendDatagram( msg, sock, address ):
    sock.sendto(msg, address)

#def sendDatagram (msg, sock, address):
#    # msg is a byte array ready to be sent
#    # Generate random number in the range of 1 to 10
#    rand = random.randint(1, 10)
#    if rand > 2:
#        print("I sent a datagram")
#        sock.sendto(msg, address)
#    else:
#        print("I failed to send a datagram")

def waitForAck( s, timeout ):
    rx, tx, er = select.select( [s], [],[], timeout)
    return rx!=[]


def tx_thread(s : socket, receiver, window : Window, timeout : float ):

    def send_all_window():
        # Send the whole window
        for nSeq