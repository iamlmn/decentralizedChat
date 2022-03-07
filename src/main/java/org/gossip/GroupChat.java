package org.gossip;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.gossip.configs.GossipProperty;
import org.gossip.services.NodeGossiper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;

public class GroupChat {

    private static final Logger log = Logger.getLogger(GroupChat.class);

    //Configuring Logger for creating log files
    public static void initLogger(String hostname, int port) {
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.ALL);

        //Define log pattern layout
        PatternLayout patternLayout = new PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n");

        try {
            // Define file appender with layout and output log file name
            RollingFileAppender fileAppenderHandler = new RollingFileAppender(patternLayout, hostname + port + "_logs.log");

            //Add the appender to root logger
            rootLogger.addAppender(fileAppenderHandler);
        } catch (IOException e) {
            System.out.println("Failed to add appender !!");
        }
    }


    public static void main(String[] args) {

        //initialNodeBoolean is used for starting initial communication
        boolean initialNodeBoolean = true;
        
        //NodeGossiper handles maintaining membership, failure detection, sending/recieving chat messages
        NodeGossiper initialNodeGossiper;
        
        if (args.length < 2) {
            System.out.println("Format is incorrect, please enter in following format.\n" +
                    "{sourceHostname} {source-port-number} [targetHostName] [target port number]");
            System.exit(-1);
        }
        String sourceHostName = args[0];
        int sourcePort = Integer.parseInt(args[1]);

        //Initialising Logger Configuration
        initLogger(sourceHostName, sourcePort);

        String targetHostName = "";
        int targetPort = 0;
        if (args.length > 2){
            initialNodeBoolean = false;
            targetHostName = args[2];
            targetPort = Integer.parseInt(args[3]);
        }        
        
        //Get default gossip configurations
        GossipProperty gossipProperty = setDefaultGossipProperty();

        // Create source socket
        InetSocketAddress primaryNodeAddress = new InetSocketAddress(sourceHostName, sourcePort);

        if (initialNodeBoolean == true) {
            initialNodeGossiper = new NodeGossiper(primaryNodeAddress, gossipProperty);

        } else {
            initialNodeGossiper = new NodeGossiper(primaryNodeAddress, new InetSocketAddress(targetHostName, targetPort), gossipProperty);

        }
        System.out.println("Node Started at- "+primaryNodeAddress.getAddress() +"::"+primaryNodeAddress.getPort());
        initialNodeGossiper.startGossip();

    }

    private static GossipProperty setDefaultGossipProperty() {
        return new GossipProperty(
                Duration.ofSeconds(1), // failure timeout
                Duration.ofSeconds(1), // cleanup timeout
                Duration.ofMillis(500), // updateFrequency
                Duration.ofMillis(500), // detectionInterval
                4 // peer count
        );
    }

}



