/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *address)
{
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node()
{
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing()
{
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	ring = curMemList;
	stabilizationProtocol();
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList()
{
	unsigned int i;
	vector<Node> curMemList;
	for (i = 0; i < this->memberNode->memberList.size(); i++)
	{
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key)
{
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret % RING_SIZE;
}

ReplicaType MP2Node::GetReplicaType(int replicaIndex)
{
	switch (replicaIndex)
	{
	case 0:
		return PRIMARY;
	case 1:
		return SECONDARY;
	case 2:
	default:
		return TERTIARY;
	}
}

void MP2Node::logSuccess(MessageType msgType,
						 bool coordinator,
						 int transID,
						 string key,
						 string value)
{
	if (transID == 0) return;

	switch (msgType)
	{

	case CREATE:
	{
		log->logCreateSuccess(&memberNode->addr,
							  coordinator,
							  transID,
							  key,
							  value);

		break;
	}

	case READ:
	{
		log->logReadSuccess(&memberNode->addr,
							coordinator,
							transID,
							key,
							value);
		
		break;
	}

	case UPDATE:
	{
		log->logUpdateSuccess(&memberNode->addr,
							  coordinator,
							  transID,
							  key,
							  value);
		
		break;
	}

	case DELETE:
	{
		log->logDeleteSuccess(&memberNode->addr,
							  coordinator,
							  transID,
							  key);

		break;
	}
	}
}

void MP2Node::logFail(MessageType msgType,
					  bool coordinator,
					  int transID,
					  string key,
					  string value)
{
	if (transID == 0) return;

	switch (msgType)
	{

	case CREATE:
	{
		log->logCreateFail(&memberNode->addr,
						   coordinator,
						   transID,
						   key,
						   value);

		break;
	}

	case READ:
	{
		log->logReadFail(&memberNode->addr,
						 coordinator,
						 transID,
						 key);

		break;
	}

	case UPDATE:
	{
		log->logUpdateFail(&memberNode->addr,
						   coordinator,
						   transID,
						   key,
						   value);

		break;
	}

	case DELETE:
	{
		log->logDeleteFail(&memberNode->addr,
						   coordinator,
						   transID,
						   key);

		break;
	}
	}
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value)
{
	/*
	 * Implement this
	 */

    auto transID = (!stabilizing) ? ++g_transID : 0;

	transInfo tInfo;

	tInfo.type = CREATE;
	tInfo.key = key;
	tInfo.value = value;
	tInfo.timestamp = par->getcurrtime();
	tInfo.numSucc = 0;
	tInfo.numFail = 0;

    acks[transID] = tInfo; 

    auto replicas = findNodes(key);

    for (int index = 0; index < replicas.size(); ++index) 
	{
		auto replica = replicas[index];

		Message m(transID, 
		          memberNode->addr, 
				  CREATE, 
				  key, 
				  value, 
				  GetReplicaType(index));

        string data(m.toString());

	    int size = emulNet->ENsend(&memberNode->addr, 
		                           replica.getAddress(),
								   (char *)data.c_str(),
								   (int)data.length());
	}
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key)
{
	/*
	 * Implement this
	 */
    auto transID = ++g_transID;

	transInfo tInfo;

	tInfo.type = READ;
	tInfo.key = key;
	tInfo.value = "";
	tInfo.timestamp = par->getcurrtime();
	tInfo.numSucc = 0;
	tInfo.numFail = 0;

    acks[transID] = tInfo; 

    auto replicas = findNodes(key);

    for (int index = 0; index < replicas.size(); ++index) 
	{
		auto replica = replicas[index];

		Message m(transID, 
		          memberNode->addr, 
				  READ, 
				  key);

        string data(m.toString());

	    int size = emulNet->ENsend(&memberNode->addr, 
		                           replica.getAddress(),
								   (char *)data.c_str(),
								   (int)data.length());
	}
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value)
{
	/*
	 * Implement this
	 */
    auto transID = ++g_transID;

	transInfo tInfo;

	tInfo.type = UPDATE;
	tInfo.key = key;
	tInfo.value = value;
	tInfo.timestamp = par->getcurrtime();
	tInfo.numSucc = 0;
	tInfo.numFail = 0;

    acks[transID] = tInfo; 

    auto replicas = findNodes(key);

    for (int index = 0; index < replicas.size(); ++index) 
	{
		auto replica = replicas[index];

		Message m(transID, 
		          memberNode->addr, 
				  UPDATE, 
				  key, 
				  value, 
				  GetReplicaType(index));

        string data(m.toString());

	    int size = emulNet->ENsend(&memberNode->addr, 
		                           replica.getAddress(),
								   (char *)data.c_str(),
								   (int)data.length());
	}
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key)
{
	/*
	 * Implement this
	 */
    auto transID = ++g_transID;

	transInfo tInfo;

	tInfo.type = DELETE;
	tInfo.key = key;
	tInfo.value = "";
	tInfo.timestamp = par->getcurrtime();
	tInfo.numSucc = 0;
	tInfo.numFail = 0;

    acks[transID] = tInfo; 

    auto replicas = findNodes(key);

    for (int index = 0; index < replicas.size(); ++index) 
	{
		auto replica = replicas[index];

		Message m(transID, 
		          memberNode->addr, 
				  DELETE, 
				  key);

        string data(m.toString());

	    int size = emulNet->ENsend(&memberNode->addr, 
		                           replica.getAddress(),
								   (char *)data.c_str(),
								   (int)data.length());
	}
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica)
{
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	Entry e(value, par->getcurrtime(), replica);

	return ht->create(key, e.convertToString());
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key)
{
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
    string entryStr = ht->read(key);

	if (entryStr == "") return entryStr;

	Entry e(entryStr);
	
	return e.value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica)
{
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	Entry e(value, par->getcurrtime(), replica);

	return ht->update(key, e.convertToString());
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key)
{
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages()
{
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char *data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while (!memberNode->mp2q.empty())
	{
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

        Message m(message);
		/*
		 * Handle the message types here
		 */

		switch (m.type)
		{
		case CREATE:
		{
			bool success = createKeyValue(m.key,
										  m.value,
										  m.replica);

			if (success)
			{
				logSuccess(CREATE,
						   false,
						   m.transID,
						   m.key,
						   m.value);
			}
			else
			{
				logFail(CREATE,
						false,
						m.transID,
						m.key,
						m.value);
			}

			Message reply(m.transID, this->memberNode->addr, REPLY, success); 

			string data(reply.toString());

			int size = emulNet->ENsend(&memberNode->addr,
									   &m.fromAddr,
									   (char *)data.c_str(),
									   (int)data.length());

			break;
		}
        
		case READ:
		{
			string value = readKey(m.key);

			if (value != "")
			{
				logSuccess(READ,
						   false,
						   m.transID,
						   m.key,
						   value); 
			}
			else
			{
				logFail(READ,
						false,
						m.transID,
						m.key,
						"");
			}

			Message readReply(m.transID, this->memberNode->addr, value); 

			string data(readReply.toString());

			int size = emulNet->ENsend(&memberNode->addr,
									   &m.fromAddr,
									   (char *)data.c_str(),
									   (int)data.length());

			break;
		}

		case UPDATE:
		{
			bool success = updateKeyValue(m.key,
										  m.value,
										  m.replica);

			if (success)
			{
				logSuccess(UPDATE,
						   false,
						   m.transID,
						   m.key,
						   m.value);
			}
			else
			{
				logFail(UPDATE,
						false,
						m.transID,
						m.key,
						m.value);
			}

			Message reply(m.transID, this->memberNode->addr, REPLY, success); 

			string data(reply.toString());

			int size = emulNet->ENsend(&memberNode->addr,
									   &m.fromAddr,
									   (char *)data.c_str(),
									   (int)data.length());

			break;

		}

		case DELETE:
		{
			bool success = deletekey(m.key);

			if (success)
			{
				logSuccess(DELETE,
						   false,
						   m.transID,
						   m.key,
						   "");
			}
			else
			{
				logFail(DELETE,
						false,
						m.transID,
						m.key,
						"");
			}
			
			Message reply(m.transID, this->memberNode->addr, REPLY, success); 

			string data(reply.toString());

			int size = emulNet->ENsend(&memberNode->addr,
									   &m.fromAddr,
									   (char *)data.c_str(),
									   (int)data.length());

			break;
		}
      
	    case READREPLY:
		{
			auto it = acks.find(m.transID);

			if (it != acks.end()) 
			{
				if (m.value != "")
				{
					it->second.numSucc++;
				}
				else
				{
					it->second.numFail++;
				}

				if (it->second.numSucc == (NUM_REPLICAS / 2 + 1))
				{
					logSuccess(it->second.type,
					           true,
							   m.transID,
							   it->second.key,
							   m.value);

					acks.erase(it);
				}
				else if (it->second.numFail == (NUM_REPLICAS / 2))
				{
					logFail(it->second.type,
					        true,
							m.transID,
							it->second.key,
							m.value);

					acks.erase(it);
				}
			}
			else 
			{
				//ignore
				// quorum/decision may have already been reached
			}
			
            break;
		}

		case REPLY:
		{
			auto it = acks.find(m.transID);

			if (it != acks.end()) 
			{
				if (m.success)
				{
					it->second.numSucc++;
				}
				else
				{
					it->second.numFail++;
				}

				if (it->second.numSucc == (NUM_REPLICAS / 2 + 1))
				{
					logSuccess(it->second.type,
					           true,
							   m.transID,
							   it->second.key,
							   it->second.value);

					acks.erase(it);
				}
				else if (it->second.numFail == (NUM_REPLICAS / 2))
				{
					logFail(it->second.type,
					        true,
							m.transID,
							it->second.key,
							it->second.value);

					acks.erase(it);
				}
			}
			else 
			{
				//ignore
				// quorum/decision may have already been reached
			}

		    break;	
		}
		}
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
	int currtime = par->getcurrtime();
	using mapIter = map<int, transInfo>::iterator; 

    mapIter it = acks.begin();

	while (it != acks.end())
	{
		if (currtime - it->second.timestamp >= 2)
		{
			logFail(it->second.type,
					true,
					it->first,
					it->second.key,
					it->second.value);

			it = acks.erase(it);
		}
		else
		{
			++it;
		}
	}
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key)
{
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3)
	{
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size() - 1).getHashCode())
		{
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else
		{
			// go through the ring until pos <= node
			for (int i = 1; i < ring.size(); i++)
			{
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode())
				{
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
					addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop()
{
	if (memberNode->bFailed)
	{
		return false;
	}
	else
	{
		return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
	}
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size)
{
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol()
{
	/*
	 * Implement this
	 */
	stabilizing = true;
	
	vector<pair<string, string>> 
	allKVPairs(ht->hashTable.begin(), ht->hashTable.end());

	ht->hashTable.clear();

    for (auto kv: allKVPairs)
	{
		string key(kv.first);

		Entry e(kv.second);

	    clientCreate(key, e.value);
	}

	stabilizing = false;
}
