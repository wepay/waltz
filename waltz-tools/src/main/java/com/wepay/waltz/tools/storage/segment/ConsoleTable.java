package com.wepay.waltz.tools.storage.segment;

import java.util.ArrayList;
import java.util.List;

public class ConsoleTable {

    private static final int TABLE_PADDING = 6;
    private static final int TRANSACTION_RECORD_ROW_SIZE = 67;
    private static final char SEPARATOR_CHAR = '-';

    private List<String> headers;
    private List<List<String>> table;
    private List<Integer> maxColSizeList;
    private boolean dumpTransactionRecord;

    public ConsoleTable(List<String> headers, List<List<String>> content, boolean dumpTransactionRecord) {
        this.headers = headers;
        this.table = content;
        this.maxColSizeList = new ArrayList<>();
        for (int i = 0; i < headers.size(); i++) {
            maxColSizeList.add(headers.get(i).length());
        }
        this.dumpTransactionRecord = dumpTransactionRecord;
        calcMaxLengthAll();
    }

    public void printTable() {
        StringBuilder sb = new StringBuilder();
        StringBuilder rowSepSb = new StringBuilder();
        StringBuilder multiRowPaddingSb = new StringBuilder();
        String padding = "";
        String rowSeparator;
        String multiRowPadding;

        // Create padding string containing just containing spaces
        for (int i = 0; i < TABLE_PADDING; i++) {
            padding += " ";
        }

        // Create the row separator
        if (dumpTransactionRecord) {
            maxColSizeList.set(maxColSizeList.size() - 1, TRANSACTION_RECORD_ROW_SIZE);
        }
        for (int i = 0; i < maxColSizeList.size(); i++) {
            for (int j = 0; j < maxColSizeList.get(i) + (TABLE_PADDING * 2); j++) {
                rowSepSb.append(SEPARATOR_CHAR);
            }
        }
        rowSeparator = rowSepSb.toString();

        // Create hex string left padding
        for (int i = 0; i < maxColSizeList.size() - 1; i++) {
            for (int j = 0; j < maxColSizeList.get(i) + (TABLE_PADDING * 2); j++) {
                multiRowPaddingSb.append(" ");
            }
        }
        multiRowPaddingSb.append(padding);
        multiRowPadding = multiRowPaddingSb.toString();

        // Start append
        sb.append(rowSeparator);
        sb.append("\n");

        // Header
        for (int i = 0; i < headers.size(); i++) {
            sb.append(padding);
            String format = "%-" + (maxColSizeList.get(i) + TABLE_PADDING) + "s";
            sb.append(String.format(format, headers.get(i)));
        }
        sb.append("\n");
        sb.append(rowSeparator);
        sb.append("\n");

        // Body
        for (int i = 0; i < table.size(); i++) {
            List<String> tempRow = table.get(i);
            for (int j = 0; j < tempRow.size() - 1; j++) {
                sb.append(padding);
                String format = "%-" + (maxColSizeList.get(j) + TABLE_PADDING) + "s";
                sb.append(String.format(format, tempRow.get(j).replaceAll("\t", "  ")));
            }

            String[] hexString = tempRow.get(tempRow.size() - 1).split("\n");
            sb.append(padding);
            sb.append(hexString[0]);
            for (int k = 1; k < hexString.length; k++) {
                sb.append("\n");
                sb.append(multiRowPadding);
                sb.append(hexString[k]);
            }
            sb.append("\n");
            sb.append(rowSeparator);
            sb.append("\n");
        }
        System.out.println(sb.toString());
    }

    private void calcMaxLengthAll() {
        for (int i = 0; i < table.size(); i++) {
            List<String> row = table.get(i);
            for (int j = 0; j < row.size(); j++) {
                int curLength = row.get(j).length();
                if (curLength > maxColSizeList.get(j)) {
                    maxColSizeList.set(j, curLength);
                }
            }
        }
    }
}
