package com.wepay.waltz.storage.server.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.wepay.riff.metrics.core.MetricRegistry;
import com.wepay.riff.network.Message;
import com.wepay.riff.network.MessageCodec;
import com.wepay.riff.network.MessageHandler;
import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.storage.common.message.admin.AdminFailureResponse;
import com.wepay.waltz.storage.common.message.admin.AdminMessage;
import com.wepay.waltz.storage.common.message.admin.AdminMessageCodecV0;
import com.wepay.waltz.storage.common.message.admin.AdminMessageType;
import com.wepay.waltz.storage.common.message.admin.AdminOpenRequest;
import com.wepay.waltz.storage.common.message.admin.AdminSuccessResponse;
import com.wepay.waltz.storage.common.message.admin.AssignedPartitionStatusResponse;
import com.wepay.waltz.storage.common.message.admin.LastSessionInfoRequest;
import com.wepay.waltz.storage.common.message.admin.LastSessionInfoResponse;
import com.wepay.waltz.storage.common.message.admin.MetricsResponse;
import com.wepay.waltz.storage.common.message.admin.PartitionAssignmentRequest;
import com.wepay.waltz.storage.common.message.admin.PartitionAvailableRequest;
import com.wepay.waltz.storage.common.message.admin.RecordListRequest;
import com.wepay.waltz.storage.common.message.admin.RecordListResponse;
import com.wepay.waltz.storage.exception.ConcurrentUpdateException;
import com.wepay.waltz.storage.exception.StorageException;
import com.wepay.waltz.storage.exception.StorageRpcException;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AdminServerHandler extends MessageHandler {

    private static final Logger logger = Logging.getLogger(AdminServerHandler.class);

    private static final int QUEUE_LOW_WATER_MARK = 300;
    private static final int QUEUE_HIGH_WATER_MARK = 600;

    private static final HashMap<Short, MessageCodec> CODECS = new HashMap<>();
    static {
        CODECS.put((short) 0, AdminMessageCodecV0.INSTANCE);
    }

    private static final String HELLO_MESSAGE = "Waltz Storage Admin Server";

    private final StorageManager storageManager;

    public AdminServerHandler(StorageManager storageManager) {
        super(CODECS, HELLO_MESSAGE, null, QUEUE_LOW_WATER_MARK, QUEUE_HIGH_WATER_MARK);

        this.storageManager = storageManager;
    }

    @Override
    protected void process(Message msg) throws Exception {
        AdminMessage message = (AdminMessage) msg;

        switch (message.type()) {
            case AdminMessageType.OPEN_REQUEST:
                try {
                    storageManager.open(((AdminOpenRequest) message).key, ((AdminOpenRequest) message).numPartitions);
                    success();

                } catch (IllegalArgumentException ex) {
                    failure(new StorageException("wrong key format"));
                } catch (StorageException ex) {
                    failure(ex);
                }
                break;

            case AdminMessageType.PARTITION_AVAILABLE_REQUEST:
                try {
                    storageManager.setPartitionAvailable(((PartitionAvailableRequest) msg).partitionId, ((PartitionAvailableRequest) msg).toggled);
                    success(message);

                } catch (ConcurrentUpdateException | IOException | StorageException ex) {
                    failure(ex, message);
                }
                break;

            case AdminMessageType.PARTITION_ASSIGNMENT_REQUEST:
                try {
                    storageManager.setPartitionAssignment(((PartitionAssignmentRequest) msg).partitionId, ((PartitionAssignmentRequest) msg).toggled, ((PartitionAssignmentRequest) msg).deleteStorageFiles);
                    success(message);

                } catch (ConcurrentUpdateException | IOException | StorageException ex) {
                    failure(ex, message);
                }
                break;

            case AdminMessageType.RECORD_LIST_REQUEST:
                try {
                    RecordListRequest recordListRequest = (RecordListRequest) message;
                    Partition partition = storageManager.getPartition(recordListRequest.partitionId);
                    if (partition != null) {
                        ArrayList<Record> records  = partition.getRecords(recordListRequest.transactionId, recordListRequest.maxNumRecords);
                        sendMessage(new RecordListResponse(message.seqNum, recordListRequest.partitionId, records), true);
                    } else {
                        failure(new Exception("Partition:" + recordListRequest.partitionId + " is not assigned."), message);
                    }

                } catch (IOException | StorageException ex) {
                    failure(ex, message);
                }
                break;

            case AdminMessageType.LAST_SESSION_INFO_REQUEST:
                try {
                    LastSessionInfoRequest lastSessionInfoRequest = (LastSessionInfoRequest) message;
                    Partition partition = storageManager.getPartition(lastSessionInfoRequest.partitionId);
                    if (partition != null) {
                        SessionInfo sessionInfo = partition.getLastSessionInfo();
                        sendMessage(new LastSessionInfoResponse(message.seqNum, lastSessionInfoRequest.partitionId, sessionInfo), true);
                    } else {
                        failure(new Exception("Partition:" + lastSessionInfoRequest.partitionId + " is not assigned."), message);
                    }

                } catch (StorageException ex) {
                    failure(ex, message);
                }
                break;

            case AdminMessageType.METRICS_REQUEST:
                ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
                String metricsJson = ow.writeValueAsString(MetricRegistry.getInstance());
                sendMessage(new MetricsResponse(message.seqNum, metricsJson), true);
                break;

            case AdminMessageType.ASSIGNED_PARTITION_STATUS_REQUEST:
                Map<Integer, Boolean> partitionStatusMap = new HashMap<>();

                // Check partition's assignment and availability
                Set<Integer> partitionSet = storageManager.getAssignedPartitionIds();
                for (Integer id : partitionSet) {
                    Partition partition = storageManager.getPartition(id);
                    PartitionInfoSnapshot partitionInfoSnapshot = partition.getPartitionInfoSnapshot();
                    if (partitionInfoSnapshot.isAssigned && partitionInfoSnapshot.isAvailable) {
                        partitionStatusMap.put(partitionInfoSnapshot.partitionId, true);
                    } else {
                        partitionStatusMap.put(partitionInfoSnapshot.partitionId, false);
                    }
                }

                sendMessage(new AssignedPartitionStatusResponse(message.seqNum, partitionStatusMap), true);
                break;

            default:
                throw new IllegalStateException("unknown message type: " + msg.type());
        }
    }

    private void success() {
        sendMessage(new AdminSuccessResponse(-1L), true);
    }

    private void success(AdminMessage msg) {
        sendMessage(new AdminSuccessResponse(msg.seqNum), true);
    }

    private void failure(Exception ex) {
        sendMessage(new AdminFailureResponse(-1L, new StorageRpcException(ex)), true);
    }

    private void failure(Exception ex, AdminMessage msg) {
        sendMessage(new AdminFailureResponse(msg.seqNum, new StorageRpcException(ex)), true);
    }
}
