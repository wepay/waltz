package com.wepay.waltz.storage.common.message.admin;

public class MetricsRequest extends AdminMessage {

    public MetricsRequest(long seqNum) {
        super(seqNum);
    }

    @Override
    public byte type() {
        return AdminMessageType.METRICS_REQUEST;
    }
}
