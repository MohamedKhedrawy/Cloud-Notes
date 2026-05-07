- Multicast represents the process of nodes (or threads) communicating and sharing data with each other.
- As opposed to Broadcast which represents a single transmitter and multiple receivers, all Multicast nodes are both transmitters and receivers that transmits data and receives data simultaneously.
- This process is abstracted from the network topology.
- There are 2 approaches to solving this problem:
	1. **Centralized approach**: 
		- A server receives data from all nodes and transmits data to all nodes. 
		- This approach has a huge overhead as it needs to handle at least (2 x the number of nodes) operations (meaning it scales with n) for every piece of data (assuming no aggregation methods applied).
		- For the same reason very high latency as it will most likely need to handle operations one by one also scaling with n.
		- Lastly, it doesn't tolerate any failures. For example, if the server (or the centralized group of servers) fail, all communication of the whole system would be lost.
		- Additionally, partial failures are also not tolerated. For example, if the server loops over the nodes to transmit a particular piece of data, the process could fail mid loop which causes some nodes to have certain data while others don't or even have outdated contradicting data which may lead to confusion later.
	 2. **Distributed Approach**:
		 - All nodes communicates the recent data among themselves by both transmitting new data to its peers and receiving new data from its peers accordingly. 
		 - Any distributed approach probably includes  some forms of redundancy to tolerate failures of nodes (Replication of data).
		 - This approach ultimately balances the load among the nodes, so the load doesn't fall to one node or a particular group of nodes making sure the overhead at each node is low.
		 - Distributed algorithms (like Gossip) have about O(logn) latency as opposed to the O(n) of the centralized approach.
		 - Full failure is probabilistically impossible as it is very improbable for all geo-distributed nodes to fail simultaneously. 
		 - Partial failure is detected through keeping a Membership Group that tracks which nodes are part of the system and not faulty.
		 - It gets updated periodically and algorithms like SWIM and Gossip are used to find faulty nodes and spread this information to the other nodes