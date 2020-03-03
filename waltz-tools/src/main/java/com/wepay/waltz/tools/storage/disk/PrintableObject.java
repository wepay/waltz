package com.wepay.waltz.tools.storage.disk;

import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

public class PrintableObject {

    private static final int BYTES_PER_ROW = 16;
    private static final int LOW_BITS_MASK = 0x0f;
    private static final int HIGH_BITS_MASK = 0xf0;
    private static final char[] HEX_TABLE = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'a', 'b', 'c', 'd', 'e', 'f'
    };
    private final byte[] data;

    public PrintableObject(byte[] data) {
        if (data == null) {
            throw new NullPointerException();
        }
        this.data = data.clone();
    }

    @Override
    public String toString() {
        byte byteValue;
        StringBuffer str = new StringBuffer(data.length * 3);

        for (int i = 0; i < data.length; i += BYTES_PER_ROW) {
            String offset = Integer.toHexString(i);

            // "0" left pad offset field so it is always 8 char's long.
            for (int offLen = offset.length(); offLen < 8; offLen++) {
                str.append("0");
            }
            str.append(offset);
            str.append(":");

            // dump hex version of 16 bytes per line.
            for (int j = 0; (j < BYTES_PER_ROW) && ((i + j) < data.length); j++) {
                byteValue = data[i + j];

                // add spaces between every 2 bytes.
                if ((j % 2) == 0) {
                    str.append(" ");
                }

                // dump a single byte.
                byte highNibble = (byte) ((byteValue & HIGH_BITS_MASK) >>> 4);
                byte lowNibble  = (byte) (byteValue & LOW_BITS_MASK);

                str.append(HEX_TABLE[highNibble]);
                str.append(HEX_TABLE[lowNibble]);
            }

            // dump ascii version of 16 bytes
            str.append("  ");
            for (int j = 0; (j < BYTES_PER_ROW) && ((i + j) < data.length); j++) {
                char charValue = (char) data[i + j];
                CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder();
                if (asciiEncoder.canEncode(charValue)) {
                    str.append(String.valueOf(charValue));
                } else {
                    str.append(".");
                }

            }
            str.append("\n");
        }
        return str.toString();
    }
}
