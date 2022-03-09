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
        Logger rootLoggerHanlder = Logger.getRootLogger();
        rootLoggerHanlder.setLevel(Level.ALL);

        //Define log pattern layout
        PatternLayout patternLayout = new PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n");

        try {
            // Define file appender with layout and output log file name
            RollingFileAppender fileAppenderHandler = new RollingFileAppender(patternLayout, hostname + port + "_logs.log");

            //Add the appender to root logger
            rootLoggerHanlder.addAppender(fileAppenderHandler);
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
            System.out.println("Input format is incorrect, please enter in following format.\n" +
                    "{sourceHostname} {source-port-number} [targetHostName] [target port number]");
            System.exit(-1);
        }
        String sourceHostName = args[0];
        if (validateIPv4(sourceHostName) == false) {
            System.out.println("Invalid source IP address");
            System.exit(-1);
        }

        int sourcePort = Integer.parseInt(args[1]);

        //Initialising Logger Configuration
        initLogger(sourceHostName, sourcePort);

        String targetHostName = "";
        int targetPort = 0;
        if (args.length > 2){
            initialNodeBoolean = false;
            targetHostName = args[2];
            if (validateIPv4(targetHostName) == false) {
                System.out.println("Invalid target IP address");
                System.exit(-1);
                }
            targetPort = Integer.parseInt(args[3]);
        }        
        
        //Get default gossip configurations
        GossipProperty gossipProperty = setDefaultGossipProperty();

        // Create source socket
        InetSocketAddress primaryNodeAddress = new InetSocketAddress(sourceHostName, sourcePort);
        initialNodeGossiper = (initialNodeBoolean == true) ? new NodeGossiper(primaryNodeAddress, gossipProperty) : new NodeGossiper(primaryNodeAddress, new InetSocketAddress(targetHostName, targetPort), gossipProperty);
        System.out.println("Node Started at- "+primaryNodeAddress.getAddress() +"::"+primaryNodeAddress.getPort());
        initialNodeGossiper.startGossip();

    }

    public static boolean validateIPv4(String IP) {
        String[] nums = IP.split("\\.", -1);
        for (String x : nums) {
        // Validate integer in range (0, 255):
        // 1. length of chunk is between 1 and 3
        if (x.length() == 0 || x.length() > 3) return false;
        // 2. no extra leading zeros
        if (x.charAt(0) == '0' && x.length() != 1) return false;
        // 3. only digits are allowed
        for (char ch : x.toCharArray()) {
            if (! Character.isDigit(ch)) return false;
        }
        // 4. less than 255
        if (Integer.parseInt(x) > 255) return false;
        }
        return true;
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



