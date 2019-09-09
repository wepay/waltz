package com.wepay.riff.config.validator;

import com.wepay.riff.config.ConfigException;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BooleanValidatorTest {
    @Test
    public void testBooleanValidator() {
        Validator goodValidator = new BooleanValidator((value) -> true, "good msg");
        Validator badValidator = new BooleanValidator((value) -> false, "bad msg");

        // Should not throw exception
        goodValidator.validate("test", "test");

        try {
            badValidator.validate("test", "test");
            fail();
        } catch (ConfigException ex) {
            assertTrue(ex.getMessage().contains("bad msg"));
        }
    }
}
