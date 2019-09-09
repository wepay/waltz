package com.wepay.riff.config.validator;

import com.wepay.riff.config.ConfigException;

import java.util.function.Function;

/**
 * A helper class for a common validation pattern where you just want to pass a method that takes the value and
 * returns a boolean (true if config is good, and false if bad). A common example would be something like:<br>
 * <br>
 * <code>parser.withValidator("testValidator"::equals, "value must equal 'testValidator'"));</code>
 */
public class BooleanValidator implements Validator {
    private final Function<Object, Boolean> func;
    private final String errorMessage;

    /**
     * @param func Function to apply to the config. Parameter is config value, return type is true if config is good
     *             and false if it's bad.
     * @param errorMessage Message to throw in ConfigException if config value is bad.
     */
    public BooleanValidator(Function<Object, Boolean> func, String errorMessage) {
        this.func = func;
        this.errorMessage = errorMessage;
    }

    @Override
    public void validate(String key, Object value) throws ConfigException {
        if (!this.func.apply(value)) {
            throw new ConfigException("Validation failed for " + key + ": " + errorMessage);
        }
    }
}
