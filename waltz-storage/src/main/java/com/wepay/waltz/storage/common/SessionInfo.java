package com.wepay.waltz.storage.common;

import com.wepay.riff.network.MessageAttributeReader;
import com.wepay.riff.network.MessageAttributeWriter;

public class SessionInfo {
    public final long sessionId;
    public final long lowWaterMark;

    public SessionInfo(final long sessionId, final long lowWaterMark) {
        this.sessionId = sessionId;
        this.lowWaterMark = lowWaterMark;
    }

    public void writeTo(MessageAttributeWriter writer) {
        writer.writeLong(sessionId);
        writer.writeLong(lowWaterMark);
    }

    public static SessionInfo readFrom(MessageAttributeReader reader) {
        return new SessionInfo(reader.readLong(), reader.readLong());
    }

    public String toString() {
        return "sessionId=" + sessionId + " lowWaterMark=" + lowWaterMark;
    }

}
