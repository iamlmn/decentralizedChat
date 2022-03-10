package org.gossip.services;

import org.gossip.configs.GossipProperty;
import org.gossip.models.ChatMessage;
import org.gossip.models.GossipNode;
import org.gossip.models.GossipNodeStatus;
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


public class FailureDetector {

    // constrcutor
    private static final Logger log = Logger.getLogger(FailureDetector.class);
    // private final RandomNameGenerator randomNameGenerator = new RandomNameGenerator();

    /*******************************************
     *      Failure detection methods          *
     *******************************************/

    ////Detect the failed node
    public void detect(ConcurrentHashMap<String, GossipNode> memberInfo, GossipProperty gossipProperty ) {
        LocalDateTime currentTimestamp = LocalDateTime.now();
        for (String member : memberInfo.keySet()) {
            GossipNode node = memberInfo.get(member);
            if (node.getStatus() == 1) {
                LocalDateTime failureDetectionTime = node.timestamp.plus(gossipProperty.getFailureTimeout());
                if (currentTimestamp.isAfter(failureDetectionTime)) {
                    node.setStatus(GossipNodeStatus.NODE_SUSPECT_DEAD);
                    System.out.println("Node " + RandomNameGenerator.getUserName(memberInfo.get(member).getPort()) + '(' + memberInfo.get(member).getPort() +  ") is Offline");
                    log.info("Suspecting a failed Node - "+ memberInfo.get(member));
                }
            }
        }
    }

    //Removing nodes that are failed
    public void remove(ConcurrentHashMap<String, GossipNode> memberInfo, GossipProperty gossipProperty) {
        LocalDateTime currentTimeStamp = LocalDateTime.now();
        for (String member : memberInfo.keySet()) {
            GossipNode node = memberInfo.get(member);
            if (node.getStatus() == 3) {
                LocalDateTime failureDetectionTime = node.timestamp.plus(gossipProperty.getFailureTimeout());
                if (currentTimeStamp.isAfter(failureDetectionTime)) {
                    log.info("Detected a failed node - Removing " + memberInfo.get(member));
                    memberInfo.remove(member); // remove Failed members
                }

            }
        }
    }

}


