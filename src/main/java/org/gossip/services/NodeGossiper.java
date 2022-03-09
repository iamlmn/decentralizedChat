package org.gossip.services;

import org.gossip.configs.GossipProperty;
import org.gossip.models.ChatMessage;
import org.gossip.models.GossipNode;
import org.gossip.models.GossipNodeStatus;
import org.gossip.services.FailureDetector;
import org.gossip.services.GossipUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class NodeGossiper {

    private static final Logger log = Logger.getLogger(NodeGossiper.class);
            //LoggerFactory.getLogger(NodeGossiper.class);

    private final GossipNode gossipNode;
    private final GossipProperty gossipProperty;
    private final ConcurrentHashMap<String, GossipNode> memberInfo; //stores all node which are online
    private final MembershipService membersConnector; // MemberShip serivce connector to transmit and recive gossips via UDP channel.
    private final ChatMessageService chatConnector;
    private final ConcurrentHashMap<String, Float> messageIndentifier = new ConcurrentHashMap<>(); // stores uniqueID of message they recieved if they recieve same message again, the probabilty of forwarding the message is decreased
    private Boolean isStopped = false;
    private List<ChatMessage> chatRepository = new ArrayList<>();
    private FailureDetector failureDetector = new FailureDetector();
    private GossipUtils utils = new GossipUtils();

    Thread sender;

    public NodeGossiper(InetSocketAddress nodeSocketAddress,
                        GossipProperty gossipProperty) {
        this.gossipProperty = gossipProperty;
        this.gossipNode = new GossipNode(nodeSocketAddress, 0, gossipProperty);
        this.membersConnector = new MembershipService(nodeSocketAddress.getPort());
        this.chatConnector = new ChatMessageService(nodeSocketAddress.getPort());
        this.memberInfo = new ConcurrentHashMap<>();
        memberInfo.putIfAbsent(gossipNode.getUniqueId(), gossipNode);

    }

    public NodeGossiper(InetSocketAddress nodeSocketAddress,
                        InetSocketAddress targetSocketAddress,
                        GossipProperty gossipProperty) {
        this(nodeSocketAddress, gossipProperty);
        GossipNode initialTargetNode = new GossipNode(targetSocketAddress, 0, gossipProperty);
        memberInfo.putIfAbsent(initialTargetNode.getUniqueId(), initialTargetNode);
        this.gossipNode.addKnownNodes(initialTargetNode.getUniqueId(), initialTargetNode);
        System.out.println("Node Online: "+initialTargetNode.getPort());
    }

    /*******************************************
     *          Start node gossip process      *
     *******************************************/
    public void startGossip() {
        startSenderThread();
        startReceiverThread();
        detectFailedNodesThread();
        startChatSenderThread();
        startChatReceiverThread();
        startReplicationThread();
        //getMemberInfo();
    }


    
    /*******************************************
     *      Start Sending process methods      *
     *******************************************/

    private void startSenderThread() {
        sender = new Thread(() -> {
            while (!isStopped) {
                membersConnector.sendGossipMessage(memberInfo, gossipNode,  gossipProperty);
                try {
                    Thread.sleep(gossipProperty.getUpdateFrequency().toMillis());
                } catch (InterruptedException e) {
                    log.error("Unable to start gossip sender thread", e);
                }
            }
        });
        sender.start();
    }

    // ***** part 2
    /*******************************************
     *      Start recieving helper methods      *
     *******************************************/

    private void startReceiverThread() {
        new Thread(() -> {
            while (!isStopped) {
                //Receive gossip message
                List<GossipNode> receivedList = membersConnector.receiveGossip();
                synchronized (memberInfo) {
                    utils.updateMembers(receivedList, memberInfo);
                }
            }
        }).start();
    }



    /*******************************************
     *      Failure detection methods          *
     *******************************************/
    // Starts detecting node failures
    private void detectFailedNodesThread() {
        new Thread(() -> {
            while (!isStopped) {
                failureDetector.detect(memberInfo, gossipProperty);
                synchronized (memberInfo) {
                    failureDetector.remove(memberInfo, gossipProperty);
                }
                try {
                    Thread.sleep(gossipProperty.getDetectionInterval().toMillis());
                } catch (InterruptedException ie) {
                    log.error("Failed to start failureDetectionThread", ie);
                }
            }
        }).start();
    }

    // ***** part 4
    /*******************************************
     *      Start sending Chat Message         *
     *******************************************/

      //Chat message service
    public void startChatSenderThread() {
        (new Thread() {
            @Override
            public void run() {
                sendChatMessages();
            }
        }).start();
    }

    public void sendChatMessages() {
        try {

            System.out.println("Give input message ");
            try (BufferedReader sysInput = new BufferedReader(new InputStreamReader(System.in))) {
                String input;
                while ((input = sysInput.readLine()) != null) {
                    final String msg = input;
                    String uniqueChatMessageID = UUID.randomUUID().toString();
                    messageIndentifier.put(uniqueChatMessageID, 1.00f);
                    
                    ChatMessage<String> message = new ChatMessage<>(gossipNode, msg, uniqueChatMessageID, false);
                    if (!chatRepository.contains(message) && !"pullInfo".equals(input)) {
                        synchronized (chatRepository) {
                            log.info("Adding message {"+input+ " From "+gossipNode.getPort()+"} in chatRepository");
                            chatRepository.add(message);
                        }
                    }
                    gossipChatMessage(message);
                }
            }
        } catch (NumberFormatException | IOException e) {
            log.error(e);
        }
    }

    public void gossipChatMessage(ChatMessage<String> message) {
        (new Thread() {
            @Override
            public void run() {
                List<String> randomMemberNodeIds = utils.getRandomNodes(memberInfo, gossipNode, gossipProperty.getPeerCount());
                randomMemberNodeIds.forEach((randomMemberNodeId) -> {
                    GossipNode randomTargetNode = memberInfo.get(randomMemberNodeId);

                    if (memberInfo.get(randomMemberNodeId) != null) {
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                chatConnector.sendMessage(randomTargetNode, message);
                            }
                        })
                                .start();
                    } else {
                        log.info("Not available port");
                    }
                });

            }
        }).start();
    }

    // ***** part 5
    /*******************************************
     *   Start recieving Chat Message  helper  *
     *******************************************/

    public void startChatReceiverThread() {
        (new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        //Accepting Request
                        final Socket s = chatConnector.getServerSocket().accept();
                        ChatMessage message = chatConnector.receiveMessage(s);
                                String mergingData = "True";
                                //Merging chatRepository if message contains DBinfo
                                if (message.containsDBinfo() ) {
                                    mergeDBInfo(message);
                                } else {
                                    String req = (String) message.getMessage();
                                    if (req.equals("pullInfo")) {
                                        sendDBInfo(message);
                                    } else {
                                        // Create a new thread for each recieve message and applying exponential backoff to control network congestion
                                        runExponentialBackOff(message);
                                    }
                                }
                            
                    } catch (IOException ex) {
                        log.error(ex);
                    }
                }
            }
        }).start();
    }
    
    // stops gossiping same message after its probability decreases by 1/64
    private void runExponentialBackOff(ChatMessage<String> message) {
        (new Thread() {
            @Override
            public synchronized void run() {
                try {
                    if (messageIndentifier.containsKey(message.getUUID())) {
                        float probability = messageIndentifier.get(message.getUUID());
                        probability /= 2;
                        if (probability >= (1 / 64f)) {
                            messageIndentifier.replace(message.getUUID(), probability);
                            Thread.sleep(gossipProperty.getUpdateFrequency().toMillis());
                            log.info("Forwarding Message: "+ message.getMessage() +" Probability changed to "+probability);
                            gossipChatMessage(message);
                        }

                    } else {
                        messageIndentifier.put(message.getUUID(), 1.00f);
                        chatRepository.add(message);
                        System.out.println("Recieved Message from " + message.getSender().getPort() + ": " + message.getMessage());
                        Thread.sleep(gossipProperty.getUpdateFrequency().toMillis());
                        gossipChatMessage(message);
                    }

                } catch (InterruptedException ex) {
                    log.error("Error:"+ex);
                }
            }
        }).start();
    }

    
    // ***** part 6
    /*******************************************
     *   Replication  Chat storage  helpers     *
     *******************************************/
     
    //Message Sync between nodes
    public void startReplicationThread() {
        (new Thread() {

            @Override
            public void run() {

                {
                    sendDBSyncMessage();
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                        log.error(e);
                    }
                }
                
            }


        }).start();
    }

    
    //Sends pull request when node start, to get all the message he didn't recieve
    public void sendDBSyncMessage() {
        final String msg = "pullInfo";
        String uniqueID = UUID.randomUUID().toString();
        messageIndentifier.put(uniqueID, 1.00f);
        ChatMessage<String> message = new ChatMessage<>(gossipNode, msg, uniqueID, false);
        gossipChatMessage(message);
    }
    
    //Sends chatRepository to node which requested pullinfo
    public void sendDBInfo(ChatMessage<String> msg) {
        (new Thread() {
            @Override
            public void run() {
                String uniqueID = UUID.randomUUID().toString();
                messageIndentifier.put(uniqueID, 1.00f);
                ChatMessage<List<ChatMessage>> message = new ChatMessage<>(gossipNode, chatRepository, uniqueID, true);
                chatConnector.sendMessage(msg.getSender(), message);
            }
        }).start();
    }

    //Merges chatRepository with recieved chatRepository
    public void mergeDBInfo(ChatMessage<List<ChatMessage>> msg) {
       (new Thread() {
            @Override
            public void run() {
            List<ChatMessage> newStorage = msg.getMessage();
                
                for (ChatMessage data : newStorage) {
                    synchronized(messageIndentifier){
       
                        if (messageIndentifier.containsKey(data.getUUID())){
                            log.info("Message already added");
                        } else {
                            
                            messageIndentifier.putIfAbsent(data.getUUID(), 1f);
                            System.out.println("Recieved Message from "+data.getSender().getPort()+": "+ data.getMessage());
                            log.info("Adding message {"+data.getMessage()+ " From "+ data.getSender().getPort()+"} in chatRepository");
                            chatRepository.add(data);
                        }
                        }
                    }
            }
        }).start();
    }


    //  Are they needed?

    public void stop() {
        isStopped = true;
    }

    public void getMemberInfo() {

        new Thread(() ->
        {
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                log.error("Unable to get member info", e);
            }
            synchronized (memberInfo) {
                log.info("List of hosts");
                memberInfo.values().forEach(node ->
                        log.info( gossipNode.getUniqueId()+" -> "+node.status));
            }
        }).start();
    }

}