package com.wepay.waltz.storage.common;

import com.wepay.riff.network.MessageAttributeReader;
import com.wepay.riff.network.MessageAttributeWriter;

public class SessionInfo {
    public final long sessionId;
    public final long lowWaterMark;
    public final long localLowWaterMark;

    public SessionInfo(final long sessionId, final long lowWaterMark, final long localLowWaterMark) {
        this.sessionId = sessionId;
        this.lowWaterMark = lowWaterMark;
        this.localLowWaterMark = localLowWaterMark;
    }

    public void writeTo(MessageAttributeWriter writer) {
        writer.writeLong(sessionId);
        writer.writeLong(lowWaterMark);
        writer.writeLong(localLowWaterMark);
    }

    public static SessionInfo readFrom(MessageAttributeReader reader) {
        return new SessionInfo(reader.readLong(), reader.readLong(), reader.readLong());
    }

    public String toString() {
        return "sessionId=" + sessionId + " lowWaterMark=" + lowWaterMark + " localLowWaterMark=" + localLowWaterMark;
    }

}
