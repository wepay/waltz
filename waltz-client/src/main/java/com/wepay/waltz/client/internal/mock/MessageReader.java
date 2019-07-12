package com.wepay.waltz.client.internal.mock;

import com.wepay.waltz.common.message.AbstractMessage;

import java.util.ArrayList;

class MessageReader {

    private int nextIndex = 0;
    private final ArrayList<AbstractMessage> messageList;

    MessageReader(ArrayList<AbstractMessage> messageList) {
        this.messageList = messageList;
    }

    AbstractMessage nextMessage(long timeout) throws InterruptedException {
        long due = System.currentTimeMillis() + timeout;
        synchronized (this) {
            synchronized (messageList) {
                while (true) {
                    if (nextIndex < messageList.size()) {
                        return messageList.get(nextIndex++);
                    } else {
                        long now = System.currentTimeMillis();

                        if (now < due) {
                            messageList.wait(due - now);
                        } else {
                            return null;
                        }
                    }
                }
            }
        }
    }

}
