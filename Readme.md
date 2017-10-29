Java Multithreaded

Implemetation:

Part 1:- I have implemented a distributed bank application, in which the branches of the bank transfer money to each other at random intervals of time. 
The initial balance for each branch is assigned by the controller.
The Controller would evenly divide the amount and send it to all the branches only if the total amount can be evenly divided.
If the total amount is divided into a decimal value, one of the branches in the distributed system will receive 1 dollar extra.

Part 2:- The second part consists of the Chandy-Lamport algorithm in order capture global snapshots of the bank. 
The output is the balance of each branch and the amount of money on all its incoming channels.
The controller randomly selects a branch and calls it's initSnapshot method. When the initSnapshot method of a branch is called, it sends a marker messsage to all the other branches and starts recording on all the communication channels. On receiving a marker message, if a branch is seeing it for the first time, it records the snapshot number and starts recording on other channels.

FIFO message delivery is also implemeted as per the requirement.


Sample Outputs:

Total Amount in the system: 3000
============================================
SNAPSHOT NUMBER: 1
branch1: 986
branch2-->branch1: 0
branch3-->branch1: 0
============================================
branch2: 1018
branch1-->branch2: 42
branch3-->branch2: 38
============================================
branch3: 916
branch1-->branch3: 0
branch2-->branch3: 0
============================================
============================================
SNAPSHOT NUMBER: 2
branch1: 986
branch2-->branch1: 0
branch3-->branch1: 0
============================================
branch2: 1088
branch1-->branch2: 0
branch3-->branch2: 0
============================================
branch3: 926
branch1-->branch3: 0
branch2-->branch3: 0
============================================
============================================
SNAPSHOT NUMBER: 3
branch1: 1004
branch2-->branch1: 0
branch3-->branch1: 0
============================================
branch2: 1087
branch1-->branch2: 0
branch3-->branch2: 0
============================================
branch3: 889
branch1-->branch3: 20
branch2-->branch3: 0
============================================
