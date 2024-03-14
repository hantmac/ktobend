package com.databend.ktobend;

import java.util.List;
import java.util.Set;

public class MessagesBatch {
    private List<String> messages;
    private Set<String> batches;
    private long firstMessageOffset;
    private long lastMessageOffset;

    public MessagesBatch(List<String> messages,Set<String> batches, long firstMessageOffset, long lastMessageOffset) {
        this.messages = messages;
        this.batches = batches;
        this.firstMessageOffset = firstMessageOffset;
        this.lastMessageOffset = lastMessageOffset;
    }

    // getters and setters
    public List<String> getMessages() {
        return messages;
    }

    public Set<String> getBatches() {
        return batches;
    }

    public Boolean Empty() {
        return messages.isEmpty();
    }

    public void setMessages(List<String> messages) {
        this.messages = messages;
    }

    public long getFirstMessageOffset() {
        return firstMessageOffset;
    }

    public void setFirstMessageOffset(long firstMessageOffset) {
        this.firstMessageOffset = firstMessageOffset;
    }

    public long getLastMessageOffset() {
        return lastMessageOffset;
    }

    public void setLastMessageOffset(long lastMessageOffset) {
        this.lastMessageOffset = lastMessageOffset;
    }
}
