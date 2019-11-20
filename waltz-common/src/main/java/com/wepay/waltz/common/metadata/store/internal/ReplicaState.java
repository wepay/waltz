package com.wepay.waltz.common.metadata.store.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class is used to implement the state of a replica.
 */
public class ReplicaState {

    public static final long UNRESOLVED = Long.MIN_VALUE;

    private static final byte VERSION = 1;
    private static final long DIVISOR = 0xFFFFFFFFL;

    public final ReplicaId replicaId;
    public final long sessionId;
    public final long closingHighWaterMark;

    /**
     * Class constructor.
     * @param replicaId The {@link ReplicaId}.
     * @param sessionId The session Id.
     * @param closingHighWaterMark The high-water mark of the corresponding replica Id.
     */
    public ReplicaState(final ReplicaId replicaId, final long sessionId, final long closingHighWaterMark) {
        this.replicaId = replicaId;
        this.sessionId = sessionId;
        this.closingHighWaterMark = closingHighWaterMark;
    }

    /**
     * Writes replica state metadata via the {@link DataOutput} provided.
     * @param out The interface that converts the data to a series of bytes.
     * @throws IOException thrown if the write fails.
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(VERSION);
        replicaId.writeTo(out);
        out.writeLong(sessionId);
        out.writeLong(closingHighWaterMark);
    }

    /**
     * Reads replica state metadata via the {@link DataInput} provided.
     * @param in The interface that reads bytes from a binary stream and converts it
     *        to the data of required type.
     * @return Returns the {@code ReplicaState}.
     * @throws IOException thrown if the read fails.
     */
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
