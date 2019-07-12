package com.wepay.waltz.common.message;

import com.wepay.riff.network.MessageAttributeReader;
import com.wepay.riff.network.MessageAttributeWriter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

public class Record {

    public final long transactionId;
    public final ReqId reqId;
    public final int header;
    public final byte[] data;
    public final int checksum;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "internal class")
    public Record(long transactionId, ReqId reqId, int header, byte[] data, int checksum) {
        if (data == null) {
            throw new NullPointerException();
        }

        this.transactionId = transactionId;
        this.reqId = reqId;
        this.header = header;
        this.data = data;
        this.checksum = checksum;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(transactionId) ^ reqId.hashCode() ^ header ^ checksum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o instanceof Record) {
            Record other = (Record) o;
            return this.transactionId == other.transactionId
                && this.reqId.equals(other.reqId)
                && this.header == other.header
                && this.checksum == other.checksum && Arrays.equals(this.data, other.data);
        } else {
            return false;
        }
    }

    public String toString() {
        return "Record(transactionId=" + transactionId + ",reqId=" + reqId + ",header=" + header + ",checksum=" + checksum + ")";
    }

    public void writeTo(MessageAttributeWriter writer) {
        writer.writeLong(transactionId);
        reqId.writeTo(writer);
        writer.writeInt(header);
        writer.writeByteArray(data);
        writer.writeInt(checksum);
    }

    public static Record readFrom(MessageAttributeReader reader) {
        long transactionId = reader.readLong();
        ReqId reqId = ReqId.readFrom(reader);
        int header = reader.readInt();
        byte[] data = reader.readByteArray();
        int checksum = reader.readInt();

        return new Record(transactionId, reqId, header, data, checksum);
    }

}
