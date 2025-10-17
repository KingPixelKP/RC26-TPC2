- Acquire and Release read more
- Condition variables
	- Threads communicate between each other
	- wait() 
		- block thread execution until condition
	- notify() 
		- wake up a thread with a condition
	- notifyAll()
		- wake up all threads of that condition
- Implement Go Back N
## Start Phase
- block size is the size of the data sent to be used by the server
- If the file exists but its empty then
	- send it with a size 0
## File Transfer Phase
- Sender sends the blocks
	- Sends
		- (block_number, data)
	- Receives
		- Receiver's acks

- Receiver sends the acknowledges of the received blocks
	- Sends
		- (ack_block_number)
	- Receives
		- Sender's blocks
