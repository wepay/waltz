package com.wepay.riff.config;

import java.util.Optional;

public interface Config {

    Object get(String key);
    Optional<Object> getOpt(String key);
    Object getObject(String key);
    void setObject(String key, Object obj);

}
