package com.wepay.riff.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Logging {

    private Logging() {
    }

    public static Logger getLogger(Class<?> clazz) {
        return LoggerFactory.getLogger(clazz);
    }

}
