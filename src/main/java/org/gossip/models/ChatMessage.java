package org.gossip.models;


import java.io.Serializable;

public class ChatMessage<T> implements Serializable
{
    private T message;
    private final String uuid;
    private boolean updateRequest;
    private GossipNode sender;
   
    public ChatMessage(GossipNode sender, T message, String id, boolean updateRequest)
    {
        this.message = message;
        this.uuid = id;
        this.updateRequest = updateRequest;
        this.sender = sender;
    }

    public String getUUID()
    {
        return uuid;
    }

    public boolean isUpdateRequestSet()
    {
        return this.updateRequest;
    }

    public T getMessage()
    {
        return message;
    }

    public GossipNode getSender()
    {
        return sender;
    }


}
