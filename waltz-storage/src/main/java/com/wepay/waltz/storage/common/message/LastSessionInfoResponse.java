package com.wepay.waltz.storage.common.message;

import com.wepay.waltz.storage.common.SessionInfo;

public class LastSessionInfoResponse extends StorageMessage {

    public final SessionInfo lastSessionInfo;

    public LastSessionInfoResponse(long sessionId, long seqNum, int partitionId, SessionInfo lastSessionInfo) {
        super(sessionId, seqNum, partitionId);

        this.lastSessionInfo = lastSessionInfo;
    }

    @Override
    public byte type() {
        return StorageMessageType.LAST_SESSION_INFO_RESPONSE;
    }
}
