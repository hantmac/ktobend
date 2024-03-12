package com.databend.ktobend;

import java.util.List;

public class MessagesBatch {
    private List<String> messages;
    private long firstMessageOffset;
    private long lastMessageOffset;

    public MessagesBatch(List<String> messages, long firstMessageOffset, long lastMessageOffset) {
        this.messages = messages;
        this.firstMessageOffset = firstMessageOffset;
        this.lastMessageOffset = lastMessageOffset;
    }

    // getters and setters
    public List<String> getMessages() {
        return messages;
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
