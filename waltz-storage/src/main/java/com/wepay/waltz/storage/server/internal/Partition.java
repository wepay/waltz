package com.wepay.waltz.storage.server.internal;

import com.wepay.riff.metrics.core.Counter;
import com.wepay.riff.metrics.core.Gauge;
import com.wepay.riff.metrics.core.Meter;
import com.wepay.riff.metrics.core.MetricGroup;
import com.wepay.riff.metrics.core.MetricRegistry;
import com.wepay.riff.metrics.core.Timer;
import com.wepay.riff.network.Message;
import com.wepay.riff.util.Logging;
import com.wepay.riff.util.RequestQueue;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.common.util.LRUCache;
import com.wepay.waltz.common.util.QueueConsumerTask;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.storage.common.message.AppendRequest;
import com.wepay.waltz.storage.common.message.FailureResponse;
import com.wepay.waltz.storage.common.message.LastSessionInfoResponse;
import com.wepay.waltz.storage.common.message.MaxTransactionIdResponse;
import com.wepay.waltz.storage.common.message.RecordHeaderListRequest;
import com.wepay.waltz.storage.common.message.RecordHeaderListResponse;
import com.wepay.waltz.storage.common.message.RecordHeaderRequest;
import com.wepay.waltz.storage.common.message.RecordHeaderResponse;
import com.wepay.waltz.storage.common.message.RecordListRequest;
import com.wepay.waltz.storage.common.message.RecordListResponse;
import com.wepay.waltz.storage.common.message.RecordRequest;
import com.wepay.waltz.storage.common.message.RecordResponse;
import com.wepay.waltz.storage.common.message.SetLowWaterMarkRequest;
import com.wepay.waltz.storage.common.message.StorageMessage;
import com.wepay.waltz.storage.common.message.StorageMessageType;
import com.wepay.waltz.storage.common.message.SuccessResponse;
import com.wepay.waltz.storage.common.message.TruncateRequest;
import com.wepay.waltz.storage.exception.ConcurrentUpdateException;
import com.wepay.waltz.storage.exception.StorageException;
import com.wepay.waltz.storage.exception.StorageRpcException;
import com.wepay.zktools.util.Uninterruptibly;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.zip.CRC32;

public class Partition {

    private static final Logger logger = Logging.getLogger(Partition.class);
    private static final String FILE_NAME_FORMAT = "%019d.%s";
    private static final MetricRegistry REGISTRY = MetricRegistry.getInstance();

    private final UUID key;
    private final Path directory;
    private final PartitionInfo partitionInfo;
    private final ArrayList<Segment> segments;
    private final long segmentSizeThreshold;
    private final RequestProcessingTask task;
    private final String metricsGroup;
    private final LRUCache<Segment, Object> segmentLRUCache;

    private Meter appendMeter;
    private Timer appendLatencyTimer;
    private Counter lastSessionInfoRequestCounter;
    private Counter setLowWaterMarkRequestCounter;
    private Counter truncateRequestCounter;
    private Counter recordHeaderRequestCounter;
    private Counter recordRequestCounter;
    private Counter maxTransactionRequestCounter;
    private Counter recordHeaderListRequestCounter;
    private Counter recordListRequestCounter;

    private Segment currentSegment;
    private volatile long sessionId;

    Partition(UUID key, Path directory, PartitionInfo partitionInfo, long segmentSizeThreshold, int segmentCacheCapacity) {
        this.key = key;
        this.directory = directory;
        this.partitionInfo = partitionInfo;
        this.segmentSizeThreshold = segmentSizeThreshold;
        this.segments = new ArrayList<>();
        this.task = new RequestProcessingTask();
        this.sessionId = partitionInfo.sessionId();
        this.segmentLRUCache = new LRUCache<>(segmentCacheCapacity, entry -> cleanupFunc(entry));
        this.metricsGroup = String.format("%s.partition-%d", MetricGroup.WALTZ_STORAGE_METRIC_GROUP, partitionInfo.partitionId);

        // Register metrics
        registerMetrics();
    }

    protected void cleanupFunc(Map.Entry<Segment, Object> entry) {
        synchronized (segments) {
            Segment segment = entry.getKey();
            if (!segment.isClosed() && !segment.isWritable()) {
                segment.closeChannel();
            }
        }
    }

    void open() throws StorageException, IOException {
        synchronized (segments) {

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "*.seg")) {
                for (Path segPath : stream) {
                    Path segFileName = segPath.getFileName();

                    if (segFileName == null) {
                        throw new IllegalStateException("found no segment file name: " + segPath.toString());
                    }

                    String idxName = segFileName.toString().replace(".seg", ".idx");
                    Path idxPath = directory.resolve(idxName);
                    segments.add(new Segment(key, segPath, idxPath, partitionInfo, segmentSizeThreshold));
                }
            }

            segments.sort(Segment.FIRST_TRANSACTION_ID_COMPARATOR);

            if (segments.size() == 0) {
                Path segPath = directory.resolve(String.format(FILE_NAME_FORMAT, 0L, "seg"));
                Path idxPath = directory.resolve(String.format(FILE_NAME_FORMAT, 0L, "idx"));
                Segment.create(key, segPath, idxPath, partitionInfo.partitionId, 0L);
                segments.add(new Segment(key, segPath, idxPath, partitionInfo, segmentSizeThreshold));
            }

            int size = segments.size();

            // Make the last segment writable. This will recover it if damaged by a fault.
            Segment segment = segments.get(size - 1);
            segment.setWritable();
            currentSegment = segment;

            // Add all segments except the writable segment to LRU Cache.
            for (int i = 0; i < size - 1; i++) {
                segmentLRUCache.put(segments.get(i), null);
            }
        }
        task.start();
    }

    void close() {
        CompletableFuture<Boolean> future = task.stop();
        synchronized (segments) {
            for (Segment segment : segments) {
                segment.close();
                segmentLRUCache.remove(segment);
            }
        }
        Uninterruptibly.run(future::get);

        // Un-register metrics
        unregisterMetrics();
    }

    void deleteSegments() throws IOException {
        synchronized (segments) {
            for (Segment segment : segments) {
                segment.delete();
                segmentLRUCache.remove(segment);
            }
            segments.clear();
        }
    }

    int checksum() throws IOException {
        CRC32 crc32 = new CRC32();

        for (Segment segment : segments) {
            segment.checksum(crc32);
        }

        return (int) crc32.getValue();
    }

    void receiveMessage(Message msg, PartitionClient client) {
        StorageMessage message = (StorageMessage) msg;

        if (this.sessionId < message.sessionId) {
            this.sessionId = message.sessionId;
        }

        task.enqueue(new RequestContext(message, client));
    }

    PartitionInfoSnapshot getPartitionInfoSnapshot() {
        return partitionInfo.getSnapshot();
    }

    public SessionInfo getLastSessionInfo() {
        return partitionInfo.getLastSessionInfo();
    }

    // Truncates transactions after the given transaction id
    private void truncate(long transactionId) throws StorageException, IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("truncating the partition: transactionId={} partitionInfo[{}]", transactionId, partitionInfo);
        }

        synchronized (segments) {
            while (segments.size() > 1) {
                int i = segments.size() - 1;
                Segment segment = segments.get(i);

                if (transactionId + 1 >= segment.firstTransactionId()) {
                    break;
                } else {
                    segment.delete();
                    segmentLRUCache.remove(segment);
                    segments.remove(i);
                    currentSegment = segments.get(segments.size() - 1);
                }
            }

            currentSegment.ensureChannelOpened();

            if (currentSegment.maxTransactionId() > transactionId) {
                currentSegment.truncate(transactionId);
            }

            // Make the last segment writable.
            currentSegment.setWritable();
            segmentLRUCache.remove(currentSegment);
        }
    }

    private long getMaxTransactionId() {
        synchronized (segments) {
            return currentSegment.maxTransactionId();
        }
    }

    private void append(ArrayList<Record> records) throws StorageException, IOException {
        synchronized (segments) {
            int off = 0;
            while (off < records.size()) {
                if (currentSegment.size() > segmentSizeThreshold) {
                    segmentLRUCache.putIfAbsent(currentSegment, null);
                    addSegment();
                }
                off = currentSegment.append(records, off);
            }
        }
    }

    private void addSegment() throws StorageException, IOException {
        logger.debug("adding a segment: partitionInfo[{}]", partitionInfo);

        long firstTransactionId = 0;

        if (currentSegment != null) {
            currentSegment.flush();
            currentSegment.setReadOnly();
            firstTransactionId = currentSegment.nextTransactionId();
        }

        Path segPath = directory.resolve(String.format(FILE_NAME_FORMAT, firstTransactionId, "seg"));
        Path idxPath = directory.resolve(String.format(FILE_NAME_FORMAT, firstTransactionId, "idx"));
        Segment.create(key, segPath, idxPath, partitionInfo.partitionId, firstTransactionId);

        Segment segment = new Segment(key, segPath, idxPath, partitionInfo, segmentSizeThreshold);
        segment.setWritable();
        segments.add(segment);
        currentSegment = segment;
    }

    private RecordHeader getRecordHeader(long transactionId) throws StorageException, IOException {
        synchronized (segments) {
            Segment segment = SegmentFinder.findSegment(segments, transactionId);
            if (segment == null) {
                return null;
            } else {
                if (!segment.isWritable()) {
                    segment.ensureChannelOpened();
                    segmentLRUCache.putIfAbsent(segment, null);
                }
                return segment.getRecordHeader(transactionId);
            }
        }
    }

    private Record getRecord(long transactionId) throws StorageException, IOException {
        Segment segment = SegmentFinder.findSegment(segments, transactionId);
        if (segment == null) {
            return null;
        } else {
            synchronized (segments) {
                if (!segment.isWritable()) {
                    segment.ensureChannelOpened();
                    segmentLRUCache.putIfAbsent(segment, null);
                }
                return segment.getRecord(transactionId);
            }
        }
    }

    public ArrayList<Record> getRecords(long transactionId, int maxNumRecords) throws StorageException, IOException {
        ArrayList<Record> recordList = new ArrayList<>();
        for (int i = 0; i < maxNumRecords; i++) {
            Record r = getRecord(transactionId + i);
            if (r == null) {
                break;
            } else {
                recordList.add(r);
            }
        }
        return recordList;
    }

    private void deleteOrphanedStorageFiles() throws IOException {
        if (!PartitionInfo.Flags.isFlagSet(partitionInfo.getFlags(), PartitionInfo.Flags.PARTITION_IS_ASSIGNED)) {
            synchronized (segments) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "*.{seg,idx}")) {
                    for (Path segOrIdxPath : stream) {
                        try {
                            Files.deleteIfExists(segOrIdxPath);
                        } catch (IOException ex) {
                            logger.error("failed to delete file: " + segOrIdxPath.toString());
                        }
                    }
                }
            }
        }
    }

    private void registerMetrics() {
        appendMeter = REGISTRY.meter(metricsGroup, "append");
        appendLatencyTimer = REGISTRY.timer(metricsGroup, "append-latency");

        REGISTRY.gauge(metricsGroup, "session-id", (Gauge<Long>) () -> partitionInfo.sessionId());
        REGISTRY.gauge(metricsGroup, "process-queue-size", (Gauge<Integer>) () -> task.queueSize());
        REGISTRY.gauge(metricsGroup, "low-water-mark", (Gauge<Long>) () -> partitionInfo.getLowWaterMark());
        REGISTRY.gauge(metricsGroup, "local-low-water-mark", (Gauge<Long>) () -> partitionInfo.getLocalLowWaterMark());
        REGISTRY.gauge(metricsGroup, "flags", (Gauge<Integer>) () -> partitionInfo.getFlags());
        REGISTRY.gauge(metricsGroup, "sequence-number", (Gauge<Integer>) () -> partitionInfo.getSequenceNumber());
        REGISTRY.gauge(metricsGroup, "max-transaction-id", (Gauge<Long>) () -> getMaxTransactionId());

        lastSessionInfoRequestCounter = REGISTRY.counter(metricsGroup, "last-session-info-request");
        setLowWaterMarkRequestCounter = REGISTRY.counter(metricsGroup, "set-low-water-mark-request");
        truncateRequestCounter = REGISTRY.counter(metricsGroup, "truncate-request");
        recordHeaderRequestCounter =  REGISTRY.counter(metricsGroup, "record-header-request");
        recordRequestCounter = REGISTRY.counter(metricsGroup, "record-request");
        maxTransactionRequestCounter = REGISTRY.counter(metricsGroup, "max-transaction-request");
        recordHeaderListRequestCounter = REGISTRY.counter(metricsGroup, "record-header-list-request");
        recordListRequestCounter = REGISTRY.counter(metricsGroup, "record-list-request");
    }

    private void unregisterMetrics() {
        REGISTRY.remove(metricsGroup, "append");
        REGISTRY.remove(metricsGroup, "append-latency");
        REGISTRY.remove(metricsGroup, "session-id");
        REGISTRY.remove(metricsGroup, "process-queue-size");
        REGISTRY.remove(metricsGroup, "low-water-mark");
        REGISTRY.remove(metricsGroup, "local-low-water-mark");
        REGISTRY.remove(metricsGroup, "flags");
        REGISTRY.remove(metricsGroup, "sequence-number");
        REGISTRY.remove(metricsGroup, "max-transaction-id");
        REGISTRY.remove(metricsGroup, "last-session-info-request");
        REGISTRY.remove(metricsGroup, "set-low-water-mark-request");
        REGISTRY.remove(metricsGroup, "truncate-request");
        REGISTRY.remove(metricsGroup, "record-header-request");
        REGISTRY.remove(metricsGroup, "record-request");
        REGISTRY.remove(metricsGroup, "max-transaction-request");
        REGISTRY.remove(metricsGroup, "record-header-list-request");
        REGISTRY.remove(metricsGroup, "record-list-request");
    }

    private static class RequestContext {

        final StorageMessage message;
        final PartitionClient client;

        RequestContext(StorageMessage message, PartitionClient client) {
            this.message = message;
            this.client = client;
        }

    }

    private class RequestProcessingTask extends QueueConsumerTask<RequestContext> {

        RequestProcessingTask() {
            super("Storage-P" + partitionInfo.partitionId, new RequestQueue<>(new ArrayBlockingQueue<>(100)));
        }

        @Override
        public void process(RequestContext requestContext) throws Exception {
            StorageMessage msg = requestContext.message;

            try {
                checkPermissions(requestContext.message);

                switch (requestContext.message.type()) {
                    case StorageMessageType.LAST_SESSION_INFO_REQUEST:
                        lastSessionInfoRequestCounter.inc();

                        requestContext.client.sendMessage(
                            new LastSessionInfoResponse(msg.sessionId, msg.seqNum, msg.partitionId, getLastSessionInfo()),
                            true
                        );
                        break;

                    case StorageMessageType.SET_LOW_WATER_MARK_REQUEST:
                        setLowWaterMarkRequestCounter.inc();

                        SetLowWaterMarkRequest setLowWaterMarkRequest = (SetLowWaterMarkRequest) requestContext.message;
                        long lowWaterMark = setLowWaterMarkRequest.lowWaterMark;
                        long maxTransactionId = getMaxTransactionId();

                        if (maxTransactionId > lowWaterMark) {
                            throw new StorageException("low-water mark is lower than the max transaction id");

                        } else if (maxTransactionId < lowWaterMark) {
                            logger.debug("the storage falling behind. The current max transaction id is smaller than the low-water mark");
                        }

                        currentSegment.flush();
                        partitionInfo.setLowWaterMark(setLowWaterMarkRequest.sessionId, lowWaterMark, maxTransactionId);
                        success(requestContext);
                        break;

                    case StorageMessageType.TRUNCATE_REQUEST:
                        truncateRequestCounter.inc();

                        // Truncate transactions after the specified transaction id
                        TruncateRequest truncateRequest = (TruncateRequest) msg;
                        long newMaxTransactionId = getMaxTransactionId();
                        if (newMaxTransactionId > truncateRequest.transactionId) {
                            newMaxTransactionId = truncateRequest.transactionId;
                            truncate(truncateRequest.transactionId);
                        }
                        requestContext.client.sendMessage(
                            new MaxTransactionIdResponse(msg.sessionId, msg.seqNum, partitionInfo.partitionId, newMaxTransactionId),
                            true
                        );
                        break;

                    case StorageMessageType.APPEND_REQUEST:
                        appendMeter.mark();
                        appendLatencyTimer.time((Timer.CheckedRunnable<Exception>) () -> {
                            AppendRequest appendRequest = (AppendRequest) msg;
                            append(appendRequest.records);
                            success(requestContext);
                        });
                        break;

                    case StorageMessageType.RECORD_HEADER_REQUEST:
                        recordHeaderRequestCounter.inc();

                        RecordHeaderRequest recordHeaderRequest = (RecordHeaderRequest) msg;
                        RecordHeader recordHeader = getRecordHeader(recordHeaderRequest.transactionId);
                        requestContext.client.sendMessage(
                            new RecordHeaderResponse(msg.sessionId, msg.seqNum, partitionInfo.partitionId, recordHeader),
                            true
                        );
                        break;

                    case StorageMessageType.RECORD_REQUEST:
                        recordRequestCounter.inc();

                        RecordRequest recordRequest = (RecordRequest) msg;
                        Record record = getRecord(recordRequest.transactionId);
                        requestContext.client.sendMessage(
                            new RecordResponse(msg.sessionId, msg.seqNum, partitionInfo.partitionId, record),
                            true
                        );
                        break;

                    case StorageMessageType.MAX_TRANSACTION_ID_REQUEST:
                        maxTransactionRequestCounter.inc();

                        requestContext.client.sendMessage(
                            new MaxTransactionIdResponse(msg.sessionId, msg.seqNum, partitionInfo.partitionId, getMaxTransactionId()),
                            true
                        );
                        break;

                    case StorageMessageType.RECORD_HEADER_LIST_REQUEST:
                        recordHeaderListRequestCounter.inc();

                        RecordHeaderListRequest recordHeaderListRequest = (RecordHeaderListRequest) msg;
                        ArrayList<RecordHeader> recordHeaderList = new ArrayList<>();
                        for (int i = 0; i < recordHeaderListRequest.maxNumRecords; i++) {
                            RecordHeader r = getRecordHeader(recordHeaderListRequest.transactionId + i);
                            if (r == null) {
                                break;
                            } else {
                                recordHeaderList.add(r);
                            }
                        }
                        requestContext.client.sendMessage(
                            new RecordHeaderListResponse(msg.sessionId, msg.seqNum, partitionInfo.partitionId, recordHeaderList),
                            true
                        );
                        break;

                    case StorageMessageType.RECORD_LIST_REQUEST:
                        recordListRequestCounter.inc();

                        RecordListRequest recordListRequest = (RecordListRequest) msg;
                        ArrayList<Record> recordList = getRecords(recordListRequest.transactionId, recordListRequest.maxNumRecords);
                        requestContext.client.sendMessage(
                            new RecordListResponse(msg.sessionId, msg.seqNum, partitionInfo.partitionId, recordList),
                            true
                        );
                        break;

                    default:
                        throw new IllegalStateException("unknown message type: " + msg.type());
                }

            } catch (RuntimeException ex) {
                failure(requestContext, ex);
                throw ex;

            } catch (Exception ex) {
                failure(requestContext, ex);
            }
        }

        private void success(RequestContext ctx) {
            ctx.client.sendMessage(
                new SuccessResponse(ctx.message.sessionId, ctx.message.seqNum, partitionInfo.partitionId), true
            );
        }

        private void failure(RequestContext ctx, Exception cause) {
            logger.warn("failed to process request: message type=" + ctx.message.type(), cause);
            StorageRpcException exception = new StorageRpcException(cause);
            ctx.client.sendMessage(
                new FailureResponse(ctx.message.sessionId, ctx.message.seqNum, partitionInfo.partitionId, exception), true
            );
        }

        /**
         * Checks whether a partition's assignment/availability flags are enabled. If a message is used by
         * offline recovery, partition should be set unavailable. Otherwise, partition should be set available.
         * If there is a violation, a {@link StorageException} will be thrown.
         *
         * If unassigned, no messages will be allowed through.
         * If unavailable, no read or write messages will be allowed through.
         * @param storageMessage Message with assignment flag.
         * @throws StorageException Thrown if partition is unassigned or unavailable.
         * @throws ConcurrentUpdateException Thrown if message session inconsistent with partition session id.
         */
        private void checkPermissions(StorageMessage storageMessage) throws StorageException, ConcurrentUpdateException {
            boolean partitionIsAssigned = PartitionInfo.Flags.isFlagSet(partitionInfo.getFlags(), PartitionInfo.Flags.PARTITION_IS_ASSIGNED);

            if (!partitionIsAssigned) {
                throw new StorageException(String.format("partition %s is not assigned to storage node.", partitionInfo.partitionId));
            }

            boolean partitionIsAvailable = PartitionInfo.Flags.isFlagSet(partitionInfo.getFlags(), PartitionInfo.Flags.PARTITION_IS_AVAILABLE);

            if (storageMessage.usedByOfflineRecovery) {
                if (partitionIsAvailable) {
                    throw new StorageException(String.format("cannot run offline recovery while partition is online"));
                }

                if (storageMessage.sessionId != partitionInfo.sessionId()) {
                    throw new StorageException("partition last session id has changed to " + partitionInfo.sessionId() + ", but request context contains " + storageMessage.sessionId);
                }
            } else {
                if (!partitionIsAvailable) {
                    throw new StorageException(String.format("read and write for partition=%s have been disabled on storage node", partitionInfo.partitionId));
                }

                if (storageMessage.sessionId != sessionId) {
                    throw new ConcurrentUpdateException("session id has changed to " + sessionId + ", but request context contains " + storageMessage.sessionId);
                }
            }
        }

        @Override
        protected void exceptionCaught(Throwable ex) {
            logger.error("exception caught", ex);
        }

    }

}
