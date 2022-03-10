# **Gossip Protocol**

A decentralized group chat with replication implemented using gossip protocol.

### Gossip - Push Variant
Push gossip for disseminating information is implemented. Maintaining membership information, detecting failed nodes and sending group messages is implemented with the help of simple push gossip.
### Gossip - Pull Variant
Pull gossip is implemented to maintain the consistency of delivering every message to each other. So when any node joins it will send pull requests to known nodes to get all the messages stored in there storage.
### Exponential Backoff Algorithm
An exponential backoff algorithm is implemented to control the network congestion happening when gossip protocol is used to send messages. Exponential Backoff Algorithm controls the network traffic by not sending the same message to random nodes when a node gets the same message multiple times.

Run the below command to create jar file for the application (requirement maven)
> mvn clean install

Run the below command to start the application, this opens the Initial Node.
> java  -jar .\GroupChat.jar 'Provide-your-IPAddress' 8080

Run the below command to start the other nodes.
> java  -jar .\GroupChat.jar 'Provide-your-IPAddress' 8081 'Provide-your-IPAddress' 8080

> java  -jar .\GroupChat.jar  'Provide-your-IPAddress' 8082 'Provide-your-IPAddress' 8081
