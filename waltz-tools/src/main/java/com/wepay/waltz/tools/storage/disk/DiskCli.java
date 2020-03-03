package com.wepay.waltz.tools.storage.disk;

import com.wepay.waltz.client.Serializer;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.util.Cli;
import com.wepay.waltz.common.util.SubcommandCli;
import com.wepay.waltz.exception.SubCommandFailedException;
import com.wepay.waltz.storage.exception.StorageException;
import com.wepay.waltz.storage.server.internal.ControlFile;
import com.wepay.waltz.storage.server.internal.PartitionInfoSnapshot;
import com.wepay.waltz.storage.server.internal.Segment;
import com.wepay.waltz.storage.server.internal.SegmentFileHeader;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * CLI tool to interact with segment files in storage nodes.
 * Must be run from the local machine where the segment files are located.
 */
public final class DiskCli extends SubcommandCli {

    private DiskCli(String[] args, boolean useByTest) {
        super(args, useByTest, Arrays.asList(
                new Subcommand(Verify.NAME, Verify.DESCRIPTION, Verify::new),
                new Subcommand(Statistic.NAME, Statistic.DESCRIPTION, Statistic::new),
                new Subcommand(DumpSegment.NAME, DumpSegment.DESCRIPTION, DumpSegment::new),
                new Subcommand(DumpControlFile.NAME, DumpControlFile.DESCRIPTION, DumpControlFile::new)
        ));
    }

    /**
     * The class extends {@link DiskBaseCli}, which verifies checksums and indexes.
     */
    protected static class Verify extends DiskBaseCli {
        private static final String NAME = "verify-segment";
        private static final String DESCRIPTION = "Verify segment checksums and indexes.";
        private static final String FULL_PATH_SEGMENT_FILE = "full_path_segment_file";
        private static final int MAX_FAILURE_THRESHOLD = 100;

        Verify(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option transactionOption = Option.builder("t")
                    .longOpt("transaction")
                    .desc("Specify the transaction id to verify. If missing, verify all transactions.")
                    .hasArg()
                    .build();
            transactionOption.setRequired(false);
            options.addOption(transactionOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            try {
                if (cmd.getArgList().size() == 0) {
                    throw new SubCommandFailedException("Missing segment file path.");
                }
                String segmentFilePath = cmd.getArgList().get(0);

                initWithSegmentFilePath(segmentFilePath);

                if (cmd.hasOption("t")) {
                    long transactionId = Long.parseLong(cmd.getOptionValue("t"));
                    if (transactionId < 0) {
                        throw new SubCommandFailedException("Transaction id must be a non-negative integer");
                    }
                    verifyTransaction(transactionId);
                } else {
                    verifyTransactions();
                }
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Failed to read segment data file. %n%s", e.getMessage()));
            } finally {
                close();
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions(), FULL_PATH_SEGMENT_FILE);
        }

        protected boolean verifyTransactions() throws IOException, StorageException {
            List<List<String>> content = new ArrayList<>();
            long transactionId = firstTransactionId;
            int failCount = 0;
            while (true) {
                Record record = segment.getRecord(transactionId);
                if (record == null) {
                    break;
                }
                long offset = index.get(transactionId);
                // if index check fail or checksum check fail, append a new row to failed table
                if (record.transactionId != transactionId || segment.checkRecord(offset, transactionId) == -1) {
                    // in case of too many failure, only append row if failedCount <= MAX_FAILURE_THRESHOLD
                    if (++failCount <= MAX_FAILURE_THRESHOLD) {
                        List<String> failedRow = new ArrayList<>();
                        String indexCheckPass = (record.transactionId == transactionId) ? "pass" : "fail";
                        String checksumPass = (segment.checkRecord(offset, transactionId) != -1) ? "pass" : "fail";
                        failedRow.add(String.valueOf(transactionId));
                        failedRow.add(indexCheckPass);
                        failedRow.add(checksumPass);
                        content.add(failedRow);
                    }
                }
                transactionId++;
            }
            if (content.size() > 0) {
                List<String> headers = new ArrayList<>();
                headers.add("TRANSACTION ID");
                headers.add("INDEX VERIFICATION");
                headers.add("CHECKSUM VERIFICATION");

                ConsoleTable ct = new ConsoleTable(headers, content, false);
                ct.printTable();
            }
            reportVerification(transactionId - firstTransactionId, failCount);
            return (failCount == 0);
        }

        protected boolean verifyTransaction(long transactionId) throws Exception {
            Record record = segment.getRecord(transactionId);
            if (record == null) {
                throw new Exception("record not found for transaction id " + transactionId);
            }
            boolean verified = true;
            // verify index
            List<String> headers = new ArrayList<>();
            headers.add("INDEX VERIFICATION");
            headers.add("CHECKSUM VERIFICATION");
            List<String> row = new ArrayList<>();

            // verify index
            if (record.transactionId != transactionId) {
                row.add(String.format("fail (expected transactionId = %d, but got transactionId = %d)", transactionId, record.transactionId));
                verified = false;
            } else {
                row.add("pass");
            }

            // verify checksum
            long offset = index.get(transactionId);
            if (segment.checkRecord(offset, transactionId) == -1) {
                row.add("fail");
                verified = false;
            } else {
                row.add("pass");
            }

            List<List<String>> content = new ArrayList<>();
            content.add(row);
            ConsoleTable ct = new ConsoleTable(headers, content, false);
            ct.printTable();
            return verified;
        }

        private void reportVerification(long numTransactions, int failCount) {
            if (failCount == 0) {
                System.out.println("INDEX VERIFICATION SUMMARY: all check pass!");
            } else {
                if (failCount > MAX_FAILURE_THRESHOLD) {
                    System.out.println(String.format("......%n%nFail count exceed print limit: %d", MAX_FAILURE_THRESHOLD));
                }
                System.out.println(String.format("VERIFICATION SUMMARY: %d out of %d check failed", failCount, numTransactions));
            }
        }
    }

    /**
     * The class extends {@link DiskBaseCli}, which displays the statistics
     * of a Waltz storage segment to stdout.
     */
    protected static class Statistic extends DiskBaseCli {
        private static final String NAME = "segment-statistic";
        private static final String DESCRIPTION = "Display segment statistic information.";
        private static final String FULL_PATH_SEGMENT_FILE = "full_path_segment_file";

        protected int numOfRows;
        protected int maxRowByteSize;
        protected int minRowByteSize;
        protected double avgRowByteSize;
        protected double midRowByteSize;

        Statistic(String[] args) {
            super(args);
            numOfRows = 0;
            maxRowByteSize = Integer.MIN_VALUE;
            minRowByteSize = Integer.MAX_VALUE;
            avgRowByteSize = -1;
            midRowByteSize = -1;
        }

        @Override
        protected void configureOptions(Options options) {
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            try {
                if (cmd.getArgList().size() == 0) {
                    throw new SubCommandFailedException("Missing segment file path.");
                }
                String segmentFilePath = cmd.getArgList().get(0);

                initWithSegmentFilePath(segmentFilePath);

                collectStatistic();
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Failed to read segment data file. %n%s", e.getMessage()));
            } finally {
                close();
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions(), FULL_PATH_SEGMENT_FILE);
        }

        protected void collectStatistic() throws StorageException, IOException {
            long transactionId = firstTransactionId;
            List<Integer> rowByteSizeList = new ArrayList<>();

            while (true) {
                Record record = segment.getRecord(transactionId++);
                if (record == null) {
                    break;
                }
                numOfRows++;
                maxRowByteSize = Math.max(record.data.length, maxRowByteSize);
                minRowByteSize = Math.min(record.data.length, minRowByteSize);
                rowByteSizeList.add(record.data.length);
            }
            avgRowByteSize = rowByteSizeList.size() > 0 ? calculateAverage(rowByteSizeList) : 0;
            midRowByteSize = rowByteSizeList.size() > 0 ? calculateMedian(numOfRows, rowByteSizeList) : 0;

            reportStatistic();
        }

        private double calculateAverage(List<Integer> rowByteSizeList) {
            double avgSize = 0;
            int num = 1;
            for (double size : rowByteSizeList) {
                avgSize += (size - avgSize) / num;
                num++;
            }
            return avgSize;
        }

        private double calculateMedian(int numOfRows, List<Integer> rowByteSizeList) {
            Collections.sort(rowByteSizeList);
            if (numOfRows % 2 != 0) {
                return rowByteSizeList.get(numOfRows / 2);
            } else {
                return (rowByteSizeList.get((numOfRows - 1) / 2) + rowByteSizeList.get(numOfRows / 2)) / 2.0;
            }
        }

        private void reportStatistic() {
            List<String> headers = new ArrayList<>();
            headers.add("number of rows");
            headers.add("maximum row byte");
            headers.add("minimum row byte");
            headers.add("average row byte");
            headers.add("median row byte");

            List<String> row = new ArrayList<>();
            row.add(String.valueOf(numOfRows));
            row.add(String.valueOf(maxRowByteSize));
            row.add(String.valueOf(minRowByteSize));
            row.add(String.valueOf(avgRowByteSize));
            row.add(String.valueOf(midRowByteSize));

            List<List<String>> content = new ArrayList<>();
            content.add(row);

            ConsoleTable ct = new ConsoleTable(headers, content, false);
            ct.printTable();
        }
    }

    /**
     * The class extends {@link DiskBaseCli}, which displays the control file contents to stdout.
     */
    protected static class DumpControlFile extends DiskBaseCli {
        private static final String NAME = "dump-control-file";
        private static final String DESCRIPTION = String.format("Display control file information. "
            + "Control file name should be %s", ControlFile.FILE_NAME);
        private static final String FULL_PATH_CONTROL_FILE = "full_path_control_file";

        DumpControlFile(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option partitionOption = Option.builder("p")
                .longOpt("partition")
                .desc("Specify the partition id to dump control file info for. If missing, display for all partitions.")
                .hasArg()
                .build();

            Option assignedOption = Option.builder("a")
                .longOpt("assigned")
                .desc("Display control file data for only assigned partitions. This is not supported with option 'p'")
                .hasArg()
                .build();

            partitionOption.setRequired(false);
            assignedOption.setRequired(false);

            options.addOption(partitionOption);
            options.addOption(assignedOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            try {
                if (cmd.getArgList().size() == 0) {
                    throw new SubCommandFailedException("Missing control file path.");
                }
                String controlFilePath = cmd.getArgList().get(0);

                if (cmd.hasOption("p") && cmd.hasOption("a")) {
                    throw new SubCommandFailedException("'assigned' is supported only if dumping for all partitions");
                }

                initWithControlFilePath(controlFilePath);

                List<List<String>> content = new ArrayList<>();
                if (cmd.hasOption("p")) {
                    int partitionId = Integer.parseInt(cmd.getOptionValue("p"));
                    int numPartitions = controlFile.getNumPartitions();
                    if (partitionId < 0 || partitionId >= numPartitions) {
                        throw new SubCommandFailedException(
                            String.format(
                                "Invalid partition id, Number of partitions: %s,"
                                    + " Min partition id: 0, Max partition id: %s",
                                numPartitions, numPartitions - 1)
                        );
                    }
                    content.add(getTableRow(controlFile.getPartitionInfoSnapshot(partitionId)));
                } else {
                    boolean printOnlyAssigned = cmd.hasOption("a");

                    controlFile.getPartitionIds().forEach(
                        partitionId -> {
                            PartitionInfoSnapshot partitionInfoSnapshot =
                                controlFile.getPartitionInfoSnapshot(partitionId);

                            if (partitionInfoSnapshot.isAssigned || !printOnlyAssigned) {
                                content.add(getTableRow(partitionInfoSnapshot));
                            }
                        }
                    );
                }

                dump(content);
            } catch (Exception e) {
                throw new SubCommandFailedException(e);
            } finally {
                close();
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions(), FULL_PATH_CONTROL_FILE);
        }

        protected void dump(List<List<String>> content) {
            ConsoleTable consoleTable = new ConsoleTable(getTableHeader(), content, false);

            System.out.println("Control File Key: " + controlFile.key);
            consoleTable.printTable();
        }

        private List<String> getTableHeader() {
            ArrayList<String> header = new ArrayList<>();
            header.add("PARTITION ID");
            header.add("SESSION ID");
            header.add("LOW-WATER MARK");
            header.add("LOCAL LOW-WATER MARK");
            header.add("IS-ASSIGNED");
            header.add("IS-AVAILABLE");
            return header;
        }

        private List<String> getTableRow(PartitionInfoSnapshot partitionInfoSnapshot) {
            ArrayList<String> row = new ArrayList<>();
            row.add(String.valueOf(partitionInfoSnapshot.partitionId));
            row.add(String.valueOf(partitionInfoSnapshot.sessionId));
            row.add(String.valueOf(partitionInfoSnapshot.lowWaterMark));
            row.add(String.valueOf(partitionInfoSnapshot.localLowWaterMark));
            row.add(String.valueOf(partitionInfoSnapshot.isAssigned));
            row.add(String.valueOf(partitionInfoSnapshot.isAvailable));
            return row;
        }
    }

    /**
     * The class extends {@link DiskCli}, which dumps the contents (both metadata and records) in a Waltz storage
     * segment to stdout.
     */
    protected static class DumpSegment extends DiskBaseCli {
        private static final String NAME = "dump-segment";
        private static final String DESCRIPTION = "Display segment metadata and record.";
        private static final String FULL_PATH_SEGMENT_FILE = "full_path_segment_file";

        private Serializer deserializer;

        DumpSegment(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option deserializerOption = Option.builder("d")
                    .longOpt("deserializer")
                    .desc("Specify deserializer to use")
                    .hasArg()
                    .build();
            Option statisticOption = Option.builder("m")
                    .longOpt("metadata")
                    .desc("Display segment metadata")
                    .build();
            Option verifyOption = Option.builder("r")
                    .longOpt("record")
                    .desc("Display segment record")
                    .build();
            Option transactionOption = Option.builder("t")
                    .longOpt("transaction")
                    .desc("Specify the transaction id to dump. If missing, dump all transactions.")
                    .hasArg()
                    .build();

            deserializerOption.setRequired(false);
            statisticOption.setRequired(false);
            verifyOption.setRequired(false);
            transactionOption.setRequired(false);

            options.addOption(statisticOption);
            options.addOption(verifyOption);
            options.addOption(transactionOption);
            options.addOption(deserializerOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            try {
                if (cmd.getArgList().size() == 0) {
                    throw new SubCommandFailedException("Missing segment file path.");
                }
                String segmentFilePath = cmd.getArgList().get(0);

                initWithSegmentFilePath(segmentFilePath);

                if (cmd.hasOption("d")) {
                    try {
                        deserializer = (Serializer) Class.forName(cmd.getOptionValue("d")).getConstructor().newInstance();
                    } catch (Exception e) {
                        throw new SubCommandFailedException(String.format("Failed to construct specified serializer. Caused by %s", e.toString()));
                    }
                } else {
                    deserializer = DefaultDeserializer.INSTANCE;
                }

                if (!cmd.hasOption("m") && !cmd.hasOption("r")) {
                    throw new SubCommandFailedException("Must specify at least one of 'metadata' or 'record'");
                } else {
                    if (cmd.hasOption("m")) {
                        System.out.println("\n[dumping data file header]\n");
                        dumpHeader();
                    }

                    if (cmd.hasOption("r")) {
                        System.out.println("\n[dumping transaction record]\n");
                        try {
                            if (cmd.hasOption("t")) {
                                long transactionId = Long.parseLong(cmd.getOptionValue("t"));
                                if (transactionId < 0) {
                                    throw new SubCommandFailedException("Transaction id must be a non-negative integer");
                                }
                                dumpRecord(transactionId);
                            } else {
                                dumpRecord();
                            }
                        } catch (Exception e) {
                            throw new SubCommandFailedException(String.format("Failed to read segment record. %n%s", e.getMessage()));
                        }
                    }
                }
            } catch (Exception e) {
                throw new SubCommandFailedException(e);
            } finally {
                close();
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions(), FULL_PATH_SEGMENT_FILE);
        }

        protected void dumpHeader() {
            SegmentFileHeader header = index.getHeader();
            List<String> headers = new ArrayList<>();
            headers.add("format version number");
            headers.add("creation time");
            headers.add("key");
            headers.add("partition id");
            headers.add("first transaction id");

            List<String> row = new ArrayList<>();
            row.add(String.valueOf(header.version));
            row.add(String.valueOf(header.creationTime));
            row.add(String.valueOf(header.key));
            row.add(String.valueOf(header.partitionId));
            row.add(String.valueOf(header.firstTransactionId));

            List<List<String>> content = new ArrayList<>();
            content.add(row);
            ConsoleTable ct = new ConsoleTable(headers, content, false);
            ct.printTable();
        }

        protected void dumpRecord() throws StorageException, IOException {
            long transactionId = index.getHeader().firstTransactionId;
            List<List<String>> table = new ArrayList<>();

            while (true) {
                Record record = segment.getRecord(transactionId++);
                if (record == null) {
                    break;
                }
                List<String> row = getTableRow(record);
                table.add(row);
            }
            List<String> header = getTableHeader();
            ConsoleTable ct = new ConsoleTable(header, table, true);
            ct.printTable();
        }

        protected void dumpRecord(long transactionId) throws Exception {
            Record record = segment.getRecord(transactionId);
            if (record == null) {
                throw new Exception(String.format("record not found for transaction id %d", transactionId));
            }
            List<String> header = getTableHeader();
            List<String> row = getTableRow(record);
            List<List<String>> table = new ArrayList<>();
            table.add(row);
            ConsoleTable ct = new ConsoleTable(header, table, true);
            ct.printTable();
        }

        private List<String> getTableHeader() {
            ArrayList<String> header = new ArrayList<>();
            header.add("TRANSACTION ID");
            header.add("REQUEST ID");
            header.add("HEADER");
            header.add("CHECKSUM");
            header.add("DATA IN HEX STRING (READABLE DATA)");
            return header;
        }

        private List<String> getTableRow(Record record) {
            ArrayList<String> row = new ArrayList<>();
            row.add(String.valueOf(record.transactionId));
            row.add(String.valueOf(record.reqId));
            row.add(String.valueOf(record.header));
            row.add(String.valueOf(record.checksum));
            row.add(deserializer.deserialize(record.data).toString());
            return row;
        }
    }

    private abstract static class DiskBaseCli extends Cli {
        protected ControlFile controlFile;
        protected Path controlFilePath;

        protected Segment segment;
        protected Segment.Index index;
        protected long firstTransactionId;
        protected Path segmentFilePath;
        protected Path indexFilePath;
        protected int partitionId;

        private DiskBaseCli(String[] args) {
            super(args);
            firstTransactionId = 0;
        }

        /**
         * Initializes only control file related fields based on the given controlFilePath.
         * Used for control file related commands.
         * @param controlFilePath Control file path
         * @throws IOException
         * @throws StorageException
         */
        protected void initWithControlFilePath(String controlFilePath) throws IOException, StorageException {
            this.controlFilePath = Paths.get(controlFilePath);

            if (!this.controlFilePath.endsWith(ControlFile.FILE_NAME)) {
                throw new FileNotFoundException(
                    String.format("Control file should be named as %s, but it is %s",
                        ControlFile.FILE_NAME, this.controlFilePath.getFileName())
                );
            }

            controlFile = loadControlFile();
        }

        /**
         * Initializes all the fields, including the control file related, based on the given segmentFilePath
         * @param segmentFilePath Segment file path
         * @throws IOException
         * @throws StorageException
         */
        protected void initWithSegmentFilePath(String segmentFilePath) throws IOException, StorageException {
            parseStorageDirectory(segmentFilePath);
            controlFile = loadControlFile();
            segment = loadSegmentFile();
            index = segment.index;
        }

        protected void close() {
            if (controlFile != null) {
                controlFile.close();
            }

            if (segment != null) {
                segment.close();
            }
        }

        private ControlFile loadControlFile() throws IOException, StorageException {
            try {
                if (!Files.exists(controlFilePath)) {
                    throw new FileNotFoundException(
                        String.format("Control file not found at %s", controlFilePath)
                    );
                }

                return new ControlFile(null, controlFilePath, -1, false);
            } catch (Exception e) {
                System.out.println("Error: Failed to load control file.");
                throw e;
            }
        }

        private Segment loadSegmentFile() throws StorageException {
            try {
                return new Segment(controlFile.key, segmentFilePath, indexFilePath,
                    controlFile.getPartitionInfo(partitionId), -1);
            } catch (Exception e) {
                System.out.println("Error: Failed to load segment data file.");
                throw e;
            }
        }

        /**
         * Helper method for parsing waltz storage directory, and enriching fields including
         * segmentFile, indexFile, controlFile and partitionId.
         *
         * @param segmentFilePath
         * @throws SubCommandFailedException
         */
        @SuppressFBWarnings(
                value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
                justification = "null pointer checked")
        private void parseStorageDirectory(String segmentFilePath) {
            this.segmentFilePath = Paths.get(segmentFilePath);
            indexFilePath = Paths.get(segmentFilePath.replaceAll("seg$", "idx"));

            Path parent = this.segmentFilePath.getParent();
            try {
                if (parent == null) {
                    throw new FileNotFoundException(String.format("Path: %s does not have a parent", this.segmentFilePath));
                } else if (parent.getParent() == null) {
                    throw new FileNotFoundException(String.format("Path: %s does not have a parent", parent));
                } else if (parent.getFileName() == null) {
                    throw new FileNotFoundException(String.format("Path: %s has zero elements", parent));
                } else {
                    controlFilePath = parent.getParent().resolve(ControlFile.FILE_NAME);
                    partitionId = Integer.parseInt(parent.getFileName().toString());
                }
            } catch (FileNotFoundException e) {
                throw new SubCommandFailedException("Failed to parse waltz storage directory.");
            }
        }
    }

    public static void main(String[] args) {
        new DiskCli(args, false).processCmd();
    }
}
