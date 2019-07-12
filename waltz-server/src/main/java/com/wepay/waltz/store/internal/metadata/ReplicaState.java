package com.wepay.waltz.store.internal.metadata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReplicaState {

    public static final long UNRESOLVED = Long.MIN_VALUE;

    private static final byte VERSION = 1;
    private static final long DIVISOR = 0xFFFFFFFFL;

    public final ReplicaId replicaId;
    public final long sessionId;
    public final long closingHighWaterMark;

    public ReplicaState(final ReplicaId replicaId, final long sessionId, final long closingHighWaterMark) {
        this.replicaId = replicaId;
        this.sessionId = sessionId;
        this.closingHighWaterMark = closingHighWaterMark;
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(VERSION);
        replicaId.writeTo(out);
        out.writeLong(sessionId);
        out.writeLong(closingHighWaterMark);
    }

    public static ReplicaState readFrom(DataInput in) throws IOException {
        byte version = in.readByte();

        if (version != VERSION) {
            throw new IOException("unsupported version");
        }

        ReplicaId replicaId = ReplicaId.readFrom(in);
        long sessionId = in.readLong();
        long highWaterMark = in.readLong();

        return new ReplicaState(replicaId, sessionId, highWaterMark);
    }

    @Override
    public int hashCode() {
        return (int) ((replicaId.hashCode() ^ sessionId ^ closingHighWaterMark) / DIVISOR);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof ReplicaState) {
            ReplicaState other = (ReplicaState) o;
            return (this.replicaId.equals(other.replicaId)
                && this.sessionId == other.sessionId
                && this.closingHighWaterMark == other.closingHighWaterMark
            );
        } else {
            return false;
        }
    }

}
