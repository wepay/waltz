package com.wepay.waltz.storage.common.message.admin;

public class MetricsResponse extends AdminMessage {

    public final String metricsJson;

    public MetricsResponse(long seqNum, String metricsJson) {
        super(seqNum);

        this.metricsJson = metricsJson;
    }

    @Override
    public byte type() {
        return AdminMessageType.METRICS_RESPONSE;
    }
}
