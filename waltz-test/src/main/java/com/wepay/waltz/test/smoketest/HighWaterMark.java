package com.wepay.waltz.test.smoketest;

import com.wepay.zktools.util.State;

public class HighWaterMark extends State<Long> {

    HighWaterMark(long highWaterMark) {
        super(highWaterMark);
    }

}
