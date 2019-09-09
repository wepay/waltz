package com.wepay.riff.config.validator;

import com.wepay.riff.config.ConfigException;

import java.util.HashSet;
import java.util.Set;

/**
 * Validator that checks if two values are the same for different configs. This is useful for things such as validating
 * that no two ports are configured to be bound to the same port number.
 */
public class UniqueValidator implements Validator {
    private final Set<Object> values;
    private final boolean printValue;

    public UniqueValidator() {
        this(true);
    }

    /**
     * @param printValue If true, ConfigException will include the raw value payload in the message. Should be set to
     *                   false for secret configs like passwords.
     */
    public UniqueValidator(boolean printValue) {
        this.values = new HashSet<>();
        this.printValue = printValue;
    }

    @Override
    public void validate(String key, Object value) throws ConfigException {
        if (!values.add(value)) {
            StringBuilder msgBuilder = new StringBuilder().append("Validation failed for ").append(key);

            if (printValue) {
                msgBuilder.append(": value '").append(value).append("' is already taken");
            }

            throw new ConfigException(msgBuilder.toString());
        }
    }
}
