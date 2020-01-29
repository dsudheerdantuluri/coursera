/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy( (void *) &msg->fromAddr, &memberNode->addr, sizeof(memberNode->addr));
        memcpy( (void *) &msg->heartbeat, &memberNode->heartbeat, sizeof(long));
        msg->size = 0;
        msg->ml[0] = 0;

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */

#ifdef DEBUGLOG
    static char s[1024];
#endif

    if (size < (sizeof(MessageHdr)))
    {
        return false;
    }

    MessageHdr *InputMsg = (MessageHdr *)data;

    switch (InputMsg->msgType)
    {

    case JOINREQ:
    {
        int id = InputMsg->fromAddr.addr[0];

        if (ml.find(id) == ml.end())
        {
            ml[id] = par->getcurrtime();
        
            log->logNodeAdd(&memberNode->addr, &InputMsg->fromAddr);

            auto OutputMsgSize = sizeof(MessageHdr) + sizeof(int) * ml.size();
            MessageHdr *OutputMsg = (MessageHdr *)malloc(OutputMsgSize);

            if (!OutputMsg)
            {
                return false;
            }

            OutputMsg->msgType = JOINREP;
            OutputMsg->fromAddr = memberNode->addr;
            OutputMsg->heartbeat = par->getcurrtime();
            auto index = 0;
            for (const auto &entry : ml)
            {
                OutputMsg->ml[index++] = entry.first;
            }
            OutputMsg->size = index;

            emulNet->ENsend(&memberNode->addr, &InputMsg->fromAddr, (char *)OutputMsg, OutputMsgSize);

            free(OutputMsg);
        }

        break;
    }

    case JOINREP:
    {
        int id = InputMsg->fromAddr.addr[0];
        auto it = ml.find(id);

        if (it == ml.end()) { 
            ml[id] = InputMsg->heartbeat;
            log->logNodeAdd(&memberNode->addr, &InputMsg->fromAddr);
            memberNode->inGroup = true;
        }

        int selfid = memberNode->addr.addr[0]; 
        for (int i = 0; i < InputMsg->size; ++i) {

            id = InputMsg->ml[i];
            if (id == selfid) continue;
            auto it = ml.find(id);
            if ( it == ml.end() )
            {
                ml[id] = InputMsg->heartbeat;
                Address member;
                memset(&member, 0, sizeof(Address));
                member.addr[0] = id;
                log->logNodeAdd(&memberNode->addr, &member);
            }
        }

        break; 
    }

    case PING:
    {
        int id = InputMsg->fromAddr.addr[0];

        auto OutputMsgSize = sizeof(MessageHdr) + sizeof(int) * ml.size();
        MessageHdr *OutputMsg = (MessageHdr *)malloc(OutputMsgSize);

        if (!OutputMsg)
        {
            return false;
        }

        OutputMsg->msgType = PONG;
        OutputMsg->fromAddr = memberNode->addr;
        OutputMsg->heartbeat = par->getcurrtime();
        auto index = 0;
        for (const auto &entry : ml)
        {
            OutputMsg->ml[index++] = entry.first;
        }
        OutputMsg->size = index;

        emulNet->ENsend(&memberNode->addr, &InputMsg->fromAddr, (char *)OutputMsg, OutputMsgSize);

        free(OutputMsg);

        break;
    }

    case PONG:
    {
        int id = InputMsg->fromAddr.addr[0];
        auto it = ml.find(id);

        if (it != ml.end())
        {
            ml[id] = InputMsg->heartbeat;
            sprintf(s, "PONG...%d", InputMsg->fromAddr.addr[0]);
            log->LOG(&memberNode->addr, s);
        }
        else
        {
            ml[id] = InputMsg->heartbeat;
#ifdef DEBUGLOG
            sprintf(s, "PONG1 ...");
            log->LOG(&memberNode->addr, s);
#endif
            log->logNodeAdd(&memberNode->addr, &InputMsg->fromAddr);
        }

        int selfid = memberNode->addr.addr[0];
        for (int i = 0; i < InputMsg->size; ++i)
        {

            id = InputMsg->ml[i];
            if (id == selfid)
                continue;
            auto it = ml.find(id);
            if (it == ml.end())
            {
                //ml[id] = InputMsg->heartbeat;
                Address member;
                memset(&member, 0, sizeof(Address));
                member.addr[0] = id;
#ifdef DEBUGLOG
                sprintf(s, "PONG2 ...%d", InputMsg->fromAddr.addr[0]);
                log->LOG(&memberNode->addr, s);
#endif
                //log->logNodeAdd(&memberNode->addr, &member);
                
                auto OutputMsgSize = sizeof(MessageHdr) + sizeof(int) * ml.size();
                MessageHdr *OutputMsg = (MessageHdr *)malloc(OutputMsgSize);

                if (!OutputMsg)
                {
                    return false;
                }

                OutputMsg->msgType = PING;
                OutputMsg->fromAddr = memberNode->addr;
                OutputMsg->heartbeat = par->getcurrtime();
                auto index = 0;
                for (const auto &entry : ml)
                {
                    OutputMsg->ml[index++] = entry.first;
                }
                OutputMsg->size = index;

#ifdef DEBUGLOG
        sprintf(s, "Sending ping2 to %d...", id);
        log->LOG(&memberNode->addr, s);
#endif
                emulNet->ENsend(&memberNode->addr, &member, (char *)OutputMsg, OutputMsgSize);

                free(OutputMsg);
            }
        }
        break; 
    }
    default:
    {
        return false;
    }
    }

    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

#ifdef DEBUGLOG
    static char s[1024];
#endif

    //
    // Collect nodes which have not ponged for a while
    //
    auto currenttime = par->getcurrtime();
    vector<int> noheartbeats;
    for (const auto entry:ml) {

      if (currenttime - entry.second > 2) {
          noheartbeats.push_back(entry.first);
      }
    }

    //
    // Delete those nodes
    //
    for (auto entry: noheartbeats) {
    
      ml.erase(ml.find(entry));
      
      Address addr;
      memset(&addr, 0, sizeof(Address));
      *(int *)(&addr.addr) = entry;
      *(short *)(&addr.addr[4]) = 0;
      
      log->logNodeRemove(&memberNode->addr, &addr);
    }

    //  
    // Send pings to members 
    //
    for (const auto &entry : ml)
    {
        auto OutputMsgSize = sizeof(MessageHdr) + sizeof(int) * ml.size();
        MessageHdr *OutputMsg = (MessageHdr *)malloc(OutputMsgSize);

        if (!OutputMsg)
        {
            continue;
        }

        OutputMsg->msgType = PING;
        OutputMsg->fromAddr = memberNode->addr;
        OutputMsg->heartbeat = par->getcurrtime();
        auto index = 0;
        for (const auto &entry1 : ml)
        {
            OutputMsg->ml[index++] = entry1.first;
        }
        OutputMsg->size = index;

        Address ToAddr;
        memset(&ToAddr, 0, sizeof(Address));
        *(int *)(&ToAddr.addr) = entry.first;
        *(short *)(&ToAddr.addr[4]) = 0;
        
#ifdef DEBUGLOG
        sprintf(s, "Sending ping1 to %d...", entry.first);
        log->LOG(&memberNode->addr, s);
#endif
        emulNet->ENsend(&memberNode->addr, &ToAddr, (char *)OutputMsg, OutputMsgSize);
            
        free(OutputMsg);
    }

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
