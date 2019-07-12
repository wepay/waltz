package com.wepay.waltz.common.message;

import com.wepay.riff.network.MessageAttributeReader;
import com.wepay.riff.network.MessageAttributeWriter;

public class RecordHeader {

    public final long transactionId;
    public final ReqId reqId;
    public final int header;

    public RecordHeader(long transactionId, ReqId reqId, int header) {
        this.transactionId = transactionId;
        this.reqId = reqId;
        this.header = header;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(transactionId) ^ reqId.hashCode() ^ header;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o instanceof RecordHeader) {
            RecordHeader other = (RecordHeader) o;
            return this.transactionId == other.transactionId
                && this.reqId.equals(other.reqId)
                && this.header == other.header;
        } else {
            return false;
        }
    }

    public String toString() {
        return "Recordheader(transactionId=" + transactionId + ",reqId=" + reqId + ",header=" + header + ")";
    }

    public void writeTo(MessageAttributeWriter writer) {
        writer.writeLong(transactionId);
        reqId.writeTo(writer);
        writer.writeInt(header);
    }

    public static RecordHeader readFrom(MessageAttributeReader reader) {
        long transactionId = reader.readLong();
        ReqId reqId = ReqId.readFrom(reader);
        int header = reader.readInt();

        return new RecordHeader(transactionId, reqId, header);
    }

}
