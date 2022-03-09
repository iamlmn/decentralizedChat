package org.gossip.services;

import org.gossip.configs.GossipProperty;
import org.gossip.models.ChatMessage;
import org.gossip.models.GossipNode;
import org.gossip.models.GossipNodeStatus;
import org.apache.log4j.Logger;
import org.gossip.services.NodeGossiper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class runBackOffAlgorithm {

    private static final Logger log = Logger.getLogger(runBackOffAlgorithm.class);

    // public void gossipChatMessage(ChatMessage<String> message, GossipProperty gossipProperty, ConcurrentHashMap<String, GossipNode> memberInfo, ChatMessageService chatConnector) {
    //     (new Thread() {
    //         @Override
    //         public void run() {
    //             List<String> randomMemberNodeIds = getRandomNodes(gossipProperty.getPeerCount());
    //             randomMemberNodeIds.forEach((randomMemberNodeId) -> {
    //                 GossipNode randomTargetNode = memberInfo.get(randomMemberNodeId);

    //                 if (memberInfo.get(randomMemberNodeId) != null) {
    //                     new Thread(new Runnable() {
    //                         @Override
    //                         public void run() {
    //                             chatConnector.sendMessage(randomTargetNode, message);
    //                         }
    //                     })
    //                             .start();
    //                 } else {
    //                     log.info("Not available port");
    //                 }
    //             });

    //         }
    //     }).start();
    // }

    // //stops gossiping same message after its probability decreases by 1/64
    // public void runExponentialBackOff(ChatMessage<String> message, GossipProperty gossipProperty, ConcurrentHashMap<String, Float> messageIndentifier, List<ChatMessage> chatRepository, ConcurrentHashMap<String, GossipNode> memberInfo, ChatMessageService chatConnector) {
    //     (new Thread() {
    //         @Override
    //         public synchronized void run() {
    //             try {
    //                 if (messageIndentifier.containsKey(message.getUUID())) {
    //                     float probability = messageIndentifier.get(message.getUUID());
    //                     probability /= 2;
    //                     if (probability >= (1 / 64f)) {
    //                         messageIndentifier.replace(message.getUUID(), probability);
    //                         Thread.sleep(gossipProperty.getUpdateFrequency().toMillis());
    //                         log.info("Forwarding Message: "+ message.getMessage() +" Probability changed to "+probability);
    //                         gossipChatMessage(message);
    //                     }

    //                 } else {
    //                     messageIndentifier.put(message.getUUID(), 1.00f);
    //                     chatRepository.add(message);
    //                     System.out.println("Recieved Message from "+message.getSender().getPort()+": "+message.getMessage());
    //                     Thread.sleep(gossipProperty.getUpdateFrequency().toMillis());
    //                     gossipChatMessage(message);
    //                 }

    //             } catch (InterruptedException ex) {
    //                 log.error("Error:"+ex);
    //             }
    //         }
    //     }).start();

    // }

    
}