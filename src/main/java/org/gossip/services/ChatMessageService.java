package org.gossip.services;

import org.gossip.configs.GossipProperty;
import org.gossip.models.ChatMessage;
import org.gossip.models.GossipNode;
import org.gossip.services.GossipUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ChatMessageService {
    private static final Logger log = Logger.getLogger(ChatMessageService.class);
    private ServerSocket datagramSocket;
    private GossipUtils utils = new GossipUtils();
    public ChatMessageService(int portToListen) {
        try {
            datagramSocket = new ServerSocket(portToListen);
        }  catch (IOException e) {
            log.error("Unable to open socket", e);
        }
    }
    public ServerSocket getServerSocket()
    {
        return datagramSocket;
    }

    public void sendChatMessages(ConcurrentHashMap<String, GossipNode> memberInfo,GossipProperty gossipProperty, ConcurrentHashMap<String, Float> messageIndentifier, GossipNode gossipNode, List<ChatMessage> chatRepository) {
        try {

            System.out.println("Give input message ");
            try (BufferedReader sysInput = new BufferedReader(new InputStreamReader(System.in))) {
                String input;
                while ((input = sysInput.readLine()) != null) {
                    final String msg = input;
                    String uniqueChatMessageID = UUID.randomUUID().toString();
                    messageIndentifier.put(uniqueChatMessageID, 1.00f);
                    
                    ChatMessage<String> message = new ChatMessage<>(gossipNode, msg, uniqueChatMessageID, false);
                    if (!chatRepository.contains(message) && !"#pulldata".equals(input)) {
                        synchronized (chatRepository) {
                            log.info("Adding message {"+input+ " From "+gossipNode.getPort()+"} in chatRepository");
                            chatRepository.add(message);
                        }
                    }
                    gossipChatMessage(message, memberInfo, gossipProperty, gossipNode);
                }
            }
        } catch (NumberFormatException | IOException e) {
            log.error(e);
        }
    }

    public void gossipChatMessage(ChatMessage<String> message, ConcurrentHashMap<String, GossipNode> memberInfo, GossipProperty gossipProperty, GossipNode gossipNode) {
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
                                sendMessage(randomTargetNode, message);
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

    public void sendChatHistory(ChatMessage<String> msg, ConcurrentHashMap<String, Float> messageIndentifier, GossipNode gossipNode, List<ChatMessage> chatRepository) {
        (new Thread() {
            @Override
            public void run() {
                String uniqueID = UUID.randomUUID().toString();
                messageIndentifier.put(uniqueID, 1.00f);
                ChatMessage<List<ChatMessage>> message = new ChatMessage<>(gossipNode, chatRepository, uniqueID, true);
                sendMessage(msg.getSender(), message);
            }
        }).start();
    }

    

    public void sendMessage(GossipNode node, ChatMessage message) {
        try {
                Socket s = new Socket(node.getInetAddress(), node.getPort());
                ObjectOutputStream data = new ObjectOutputStream(s.getOutputStream());
                data.writeObject(message);
                data.flush();
                s.close();
                log.info("Sent " +message.getMessage() +" to process "+ node.getSocketAddress()+" system time is "+LocalDateTime.now());
        } catch (Exception ex) {
            log.error("Unable to send message", ex);
        }

    }

    public ChatMessage receiveMessage(Socket socket) {
        try
        {
            ObjectInputStream inputStrean = new ObjectInputStream(socket.getInputStream());
            ChatMessage message = (ChatMessage)inputStrean.readObject();
            log.info("Recieved "+message.getMessage()+" from " +socket.getPort()+ " system time is "+LocalDateTime.now());
            return message;

        }
        catch (IOException | ClassNotFoundException e)
        {
        }
        return null;
    }
    
}