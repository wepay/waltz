package com.wepay.waltz.tools.storage.segment;

import com.wepay.waltz.client.Serializer;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.util.Cli;
import com.wepay.waltz.common.util.SubcommandCli;
import com.wepay.waltz.exception.SubCommandFailedException;
import com.wepay.waltz.storage.exception.StorageException;
import com.wepay.waltz.storage.server.internal.ControlFile;
import com.wepay.waltz.storage.server.internal.Segment;
import com.wepay.waltz.storage.server.internal.SegmentFileHeader;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.FileNotFoundException;
import java.io.IOException;
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
public final class SegmentCli extends SubcommandCli {

    private SegmentCli(String[] args, boolean useByTest) {
        super(args, useByTest, Arrays.asList(
                new Subcommand(Verify.NAME, Verify.DESCRIPTION, Verify::new),
                new Subcommand(Statistic.NAME, Statistic.DESCRIPTION, Statistic::new),
                new Subcommand(Dump.NAME, Dump.DESCRIPTION, Dump::new)
        ));
    }

    /**
     * The class extends {@link SegmentBaseCli}, which verifies checksums and indexes.
     */
    protected static class Verify extends SegmentBaseCli {
        private static final String NAME = "verify";
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

                init(segmentFilePath);

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
                segment.close();
                throw new SubCommandFailedException(String.format("Failed to read segment data file. %n%s", e.getMessage()));
            } finally {
                segment.close();
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
     * The class extends {@link SegmentBaseCli}, which displays the statistics
     * of a Waltz storage segment to stdout.
     */
    protected static class Statistic extends SegmentBaseCli {
        private static final String NAME = "statistic";
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

                init(segmentFilePath);

                collectStatistic();
            } catch (Exception e) {
                segment.close();
                throw new SubCommandFailedException(String.format("Failed to read segment data file. %n%s", e.getMessage()));
            } finally {
                segment.close();
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
     * The class extends {@link SegmentCli}, which dumps the contents (both metadata and records) in a Waltz storage
     * segment to stdout.
     */
    protected static class Dump extends SegmentBaseCli {
        private static final String NAME = "dump";
        private static final String DESCRIPTION = "Display segment metadata and record.";
        private static final String FULL_PATH_SEGMENT_FILE = "full_path_segment_file";

        private Serializer deserializer;

        Dump(String[] args) {
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

                init(segmentFilePath);

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
                segment.close();
                throw new SubCommandFailedException(e);
            } finally {
                segment.close();
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
                throw new Exception("record not found for transaction id " + transactionId);
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

    private abstract static class SegmentBaseCli extends Cli {
        protected Segment segment;
        protected Segment.Index index;
        protected long firstTransactionId;
        protected Path segmentFile;
        protected Path indexFile;
        protected Path controlFile;
        protected int partitionId;

        private SegmentBaseCli(String[] args) {
            super(args);
            firstTransactionId = 0;
        }

        protected void init(String segmentFilePath) throws IOException, StorageException {
            parseStorageDirectory(segmentFilePath);
            ControlFile cf = loadControlFile();
            segment = loadSegmentFile(cf);
            index = segment.index;
        }

        private ControlFile loadControlFile() throws IOException, StorageException {
            try {
                return new ControlFile(null, controlFile, -1, false);
            } catch (Exception e) {
                System.out.println("Error: Failed to load control file.");
                throw e;
            }
        }

        private Segment loadSegmentFile(ControlFile cf) throws StorageException {
            try {
                return new Segment(cf.key, segmentFile, indexFile, cf.getPartitionInfo(partitionId), -1);
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
         * @throws FileNotFoundException
         */
        @SuppressFBWarnings(
                value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
                justification = "null pointer checked")
        private void parseStorageDirectory(String segmentFilePath) throws FileNotFoundException {
            segmentFile = Paths.get(segmentFilePath);
            indexFile = Paths.get(segmentFilePath.replaceAll("seg$", "idx"));

            Path parent = segmentFile.getParent();
            try {
                if (parent == null) {
                    throw new FileNotFoundException(String.format("Path: %s does not have a parent", segmentFile));
                } else if (parent.getParent() == null) {
                    throw new FileNotFoundException(String.format("Path: %s does not have a parent", parent));
                } else if (parent.getFileName() == null) {
                    throw new FileNotFoundException(String.format("Path: %s has zero elements", parent));
                } else {
                    controlFile = parent.getParent().resolve(("waltz-storage.ctl"));
                    partitionId = Integer.parseInt(parent.getFileName().toString());
                }
            } catch (FileNotFoundException e) {
                throw new SubCommandFailedException("Failed to parse waltz storage directory.");
            }
        }
    }

    public static void main(String[] args) {
        new SegmentCli(args, false).processCmd();
    }
}
