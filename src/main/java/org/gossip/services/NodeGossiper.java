package org.gossip.services;

import org.gossip.configs.GossipProperty;
import org.gossip.models.ChatMessage;
import org.gossip.models.GossipNode;
import org.gossip.models.GossipNodeStatus;
import org.gossip.services.FailureDetector;
import org.gossip.services.GossipUtils;
import org.gossip.services.RandomNameGenerator;
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
    // private final RandomNameGenerator randomNameGenerator = new RandomNameGenerator();

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
        System.out.println("Node Online: "+ RandomNameGenerator.getUserName(initialTargetNode.getPort()) + '(' + initialTargetNode.getPort() + ')');
    }

    /*******************************************
     *          Start node gossip process      *
     *******************************************/
    public void startGossip() {
        initiateSenderThread();
        initiateReceiverThread();
        initiateFailureDetectionThread();
        initiateChatSenderThread();
        initiateChatReceiverThread();
        initiateReplicationThread();
    }


    /*******************************************
     *    1.  Start Sending process methods    *
     *******************************************/

    private void initiateSenderThread() {
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

    /*******************************************
     *     2. Start recieving helper methods    *
     *******************************************/

    private void initiateReceiverThread() {
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
     *     3.  Failure detection methods          *
     *******************************************/
    // Starts detecting node failures
    private void initiateFailureDetectionThread() {
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

    /*******************************************
     *      4. Start sending Chat Message         *
     *******************************************/

      //Chat message service
    public void initiateChatSenderThread() {
        (new Thread() {
            @Override
            public void run() {
                chatConnector.sendChatMessages(memberInfo, gossipProperty, messageIndentifier, gossipNode, chatRepository);
            }
        }).start();
    }


    /*******************************************
     *   5.Start recieving Chat Message  helper *
     *******************************************/

    public void initiateChatReceiverThread() {
        (new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        //Accepting Request
                        final Socket socket = chatConnector.getServerSocket().accept();
                        ChatMessage chatMessage = chatConnector.receiveMessage(socket);
                                String mergingData = "True";
                                // if message contains DBinfo then merge the chatstorage
                                if (!chatMessage.isUpdateRequestSet() ) {
                                    String req = (String) chatMessage.getMessage();
                                    if (req.equals("#pulldata")) {
                                        chatConnector.sendChatHistory(chatMessage, messageIndentifier, gossipNode, chatRepository);
                                    } else {
                                        // for each message received a new thread would be created and to control network congestion exponential backoff would be implemented.
                                        runExponentialBackOff(chatMessage);
                                    }
                                    
                                } else {
                                    // update Chat repositoey.
                                    utils.updateChatRepository(chatMessage, messageIndentifier, chatRepository);
                                }
                            
                    } catch (IOException ex) {
                        log.error(ex);
                    }
                }
            }
        }).start();
    }
    
    // stops gossiping same message after its probability decreases by 1/64
    private void runExponentialBackOff(ChatMessage<String> chat) {
        (new Thread() {
            @Override
            public synchronized void run() {
                try {
                    if (messageIndentifier.containsKey(chat.getUUID())) {
                        float probability = messageIndentifier.get(chat.getUUID());
                        probability /= 2;
                        if (probability >= (1 / 64f)) {
                            messageIndentifier.replace(chat.getUUID(), probability);
                            Thread.sleep(gossipProperty.getUpdateFrequency().toMillis());
                            log.info("Forwarding Message: "+ chat.getMessage() +" Probability changed to "+probability);
                            chatConnector.gossipChatMessage(chat, memberInfo, gossipProperty, gossipNode);
                        }
                        else {
                            log.info("Ending  gossiping message: "+ chat.getMessage() + " as Probability is "+ probability + "(< 1/64f)");
                        }

                    } else {
                        messageIndentifier.put(chat.getUUID(), 1.00f);
                        chatRepository.add(chat);
                        System.out.println("Recieved Message from " + chat.getSender().getPort() + ": " + chat.getMessage());
                        Thread.sleep(gossipProperty.getUpdateFrequency().toMillis());
                        chatConnector.gossipChatMessage(chat, memberInfo, gossipProperty, gossipNode);
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
    public void initiateReplicationThread() {
        (new Thread() {
            @Override
            public void run() {
                {
                    sendChatSyncRequest();
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
    public void sendChatSyncRequest() {
        final String pullMessage = "#pulldata";
        String uniqueID = UUID.randomUUID().toString();
        messageIndentifier.put(uniqueID, 1.00f);
        ChatMessage<String> message = new ChatMessage<>(gossipNode, pullMessage, uniqueID, false);
        chatConnector.gossipChatMessage(message, memberInfo, gossipProperty, gossipNode);
    }
    



}