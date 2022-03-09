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


    //Update the Current Members with new Nodes
    public void updateMembers(List<GossipNode> receivedList, ConcurrentHashMap<String, GossipNode> memberInfo) {
        for (GossipNode member : receivedList) {
            String id = member.getUniqueId();
            synchronized(memberInfo){
            if (!memberInfo.containsKey(id)) {
                memberInfo.put(id, member);
                if(member.getStatus() == 1 ){
                System.out.println("Node Online: "+ randomNameGenerator.getUserName(member.getPort()) + '(' + member.getPort() + ')');
                }
                memberInfo.putIfAbsent(member.getUniqueId(), member);
                for (Map.Entry<String, GossipNode> n : member.getKnownNodes().entrySet()) {
                    memberInfo.putIfAbsent(n.getKey(), n.getValue());
                }
            } else {
                GossipNode existingMemberRecord = memberInfo.get(id);
                existingMemberRecord.update(member);
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