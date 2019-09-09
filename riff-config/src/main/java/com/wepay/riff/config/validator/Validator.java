package com.wepay.riff.config.validator;

import com.wepay.riff.config.ConfigException;

/**
 * Validator allows developers to check whether a config value is acceptable. If a ConfigException is thrown,
 * config parsing will fail, and the user will be notified accordingly.
 */
public interface Validator {
    /**
     * Check a config value for a given key, and throw an exception if the value is unacceptable.
     *
     * @param key The key of the config.
     * @param value The value of the config.
     * @throws ConfigException Thrown if the config value is invalid.
     */
    void validate(String key, Object value) throws ConfigException;
}
