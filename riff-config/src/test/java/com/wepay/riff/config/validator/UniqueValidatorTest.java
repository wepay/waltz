package com.wepay.riff.config.validator;

import com.wepay.riff.config.ConfigException;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UniqueValidatorTest {
    @Test
    public void testValidator() {
        Validator validator = new UniqueValidator(true);

        // Should not throw exception
        validator.validate("test", "dupe");

        // Should not throw exception
        validator.validate("test", "test2");

        // Should not throw exception
        validator.validate("test3", "test3");

        try {
            // Should throw exception since 'dupe' was already used
            validator.validate("test4", "dupe");
            fail();
        } catch (ConfigException ex) {
            assertTrue(ex.getMessage().contains("test4"));
            assertTrue(ex.getMessage().contains("dupe"));
        }
    }

    @Test
    public void testValidatorHidesValue() {
        Validator validator = new UniqueValidator(false);

        // Should not throw exception
        validator.validate("test", "password");

        try {
            // Should throw exception
            validator.validate("test2", "password");
            fail();
        } catch (ConfigException ex) {
            assertTrue(ex.getMessage().contains("test"));
            assertTrue(!ex.getMessage().contains("password"));
        }
    }
}
