package org.gossip.services;

import org.apache.logging.log4j.LogManager;
import org.apache.log4j.Logger;
import org.gossip.models.GossipNode;
import org.gossip.configs.GossipProperty;
import org.gossip.models.ChatMessage;
import org.gossip.services.GossipUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Arrays;
import java.util.*;

import java.util.concurrent.ConcurrentHashMap;

public class MembershipService {
    private static final Logger log = Logger.getLogger(MembershipService.class);
            //LoggerFactory.getLogger(GossipNodeConnector.class);

    private DatagramSocket datagramSocket;
    private GossipUtils utils = new GossipUtils();
    private final byte[] receivedBuffer = new byte[8192];
    private final DatagramPacket packetReceiver =
            new DatagramPacket(receivedBuffer, receivedBuffer.length);

    public MembershipService(int portToListen) {
        try {
            datagramSocket = new DatagramSocket(portToListen);
        } catch (SocketException e) {
            log.error("Unable to open socket for port "+portToListen, e);
        }
    }


    // Gossip the membership list to the peers
    public void sendGossipMessage(ConcurrentHashMap<String, GossipNode> memberInfo, GossipNode gossipNode, GossipProperty gossipProperty) {
        gossipNode.incrementHeartbeat();
        List<String> randomMemberNodeIds = utils.getRandomNodes(memberInfo, gossipNode, gossipProperty.getPeerCount());
        List<GossipNode> memeberNodes = new ArrayList<>(memberInfo.values());
        for (String randomMemberNodeId : randomMemberNodeIds) {
            GossipNode randomTargetNode = memberInfo.get(randomMemberNodeId);

            if (memberInfo.get(randomMemberNodeId) != null) {
                new Thread(() ->
                        this.sendGossip(memeberNodes, randomTargetNode.getSocketAddress()))
                        .start();
            } else {
                log.info("Node "+memberInfo.get(randomMemberNodeId)+" failed and is removed");
            }
        }
    }


    public List<GossipNode> receiveGossip() {
        List<GossipNode> gossipMessage = null;
        try {
            datagramSocket.receive(packetReceiver);

            try (ObjectInputStream objectInputStream = new ObjectInputStream(
                    new ByteArrayInputStream(packetReceiver.getData()))) {

                gossipMessage = (List<GossipNode>) objectInputStream.readObject();
                log.info("Received a gossip message " + gossipMessage);

            } catch (ClassNotFoundException e) {
                log.error("An error has occured while receiving message", e);
            }

        } catch (IOException e) {
            log.error("Failed to receive message", e);
        }

        return gossipMessage;
    }

    public void sendGossip(List<GossipNode> memberList, InetSocketAddress receiver) {
        byte[] bytesToWrite = getBytesToWrite(memberList);
        send(receiver, bytesToWrite);
    }

    private byte[] getBytesToWrite(List<GossipNode> memberList) {
        ByteArrayOutputStream bStream = new ByteArrayOutputStream();
        try (ObjectOutput oo = new ObjectOutputStream(bStream)) {
            oo.writeObject(memberList);
        } catch (IOException e) {
            log.error("Unable to write message", e);
        }
        return bStream.toByteArray();
    }

    private void send(InetSocketAddress target, byte[] data) {
        DatagramPacket packet = null;

        try {
            packet = new DatagramPacket
                    (data, data.length, target.getAddress(), target.getPort());
        } catch (Exception e1) {
            log.error("Data: {"+Arrays.toString(data)+"} length: "+data.length+" target inet: "+target.getAddress()+" target getport: "+target.getPort());

            //log.error("1.data" + Arrays.toString(data) + " length: " + data.length + " target inet" + target.getAddress() + "target getport" + target.getPort());

        }
        try {
            log.info("Sending gossip message to [" + target.toString() + "]");
            datagramSocket.send(packet);
        } catch (IOException e) {

            log.error("Fatal error trying to send: "
                    + packet + " to [" + target.toString() + "]");

        }
    }

}
