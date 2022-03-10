package org.gossip.services;

// import org.gossip.configs.GossipProperty;
// import org.gossip.models.ChatMessage;
import org.gossip.models.GossipNode;
import org.gossip.models.ChatMessage;
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


public class GossipUtils {

    private static final Logger log = Logger.getLogger(GossipUtils.class);

    private final RandomNameGenerator randomNameGenerator = new RandomNameGenerator();
    // fetch random nodes from the cluster
    public List<String> getRandomNodes(ConcurrentHashMap<String, GossipNode> memberInfo, GossipNode gossipNode, int numberOfPeers) {
        List<String> clusterNodes = new ArrayList<>(memberInfo.keySet());

        //Remove self from peer list
        clusterNodes.remove(gossipNode.getUniqueId());

        //No Random nodes are picked if cluster size is less than peer count
        if (clusterNodes.size() <= numberOfPeers) {
            return clusterNodes;
        }
        Collections.shuffle(clusterNodes);
        return clusterNodes.subList(0, numberOfPeers);
    }


    // a function that updates the current members with new nodes
    public void updateMembers(List<GossipNode> receivedNodeList, ConcurrentHashMap<String, GossipNode> memberInfo) {
        for (GossipNode node : receivedNodeList) {
            String id = node.getUniqueId();
            synchronized(memberInfo){
            if (!memberInfo.containsKey(id)) {
                memberInfo.put(id, node);
                if(node.getStatus() == 1 ){
                System.out.println("Node Online: "+ randomNameGenerator.getUserName(node.getPort()) + '(' + node.getPort() + ')');
                }
                memberInfo.putIfAbsent(node.getUniqueId(), node);
                for (Map.Entry<String, GossipNode> n : node.getKnownNodes().entrySet()) {
                    memberInfo.putIfAbsent(n.getKey(), n.getValue());
                }
            } else {
                GossipNode currentMemberRecord = memberInfo.get(id);
                currentMemberRecord.update(node);
            }
        }
        }
    }

    //Merges chatRepository with recieved chatRepository
    public void updateChatRepository(ChatMessage<List<ChatMessage>> messageReceived, ConcurrentHashMap<String, Float> messageIndentifier, List<ChatMessage> chatRepository) {
       (new Thread() {
            @Override
            public void run() {
            List<ChatMessage> newStorage = messageReceived.getMessage();
                
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


    // public void stop() {
    //     isStopped = true;
    // }

    // public void getMemberInfo(memberInfo, gossipNode) {

    //     new Thread(() ->
    //     {
    //         try {
    //             Thread.sleep(30);
    //         } catch (InterruptedException e) {
    //             log.error("Unable to get member info", e);
    //         }
    //         synchronized (memberInfo) {
    //             log.info("List of hosts");
    //             memberInfo.values().forEach(node ->
    //                     log.info( gossipNode.getUniqueId()+" -> "+node.status));
    //         }
    //     }).start();
    // }

}