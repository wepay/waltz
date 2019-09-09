package com.wepay.riff.config;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({"MagicNumber"})
public class AbstractConfigTest {

    public static class TestConfig extends AbstractConfig {
        private static final HashMap<String, Parser> parsers = new HashMap<>();
        static {
            parsers.put("str.val", stringParser);
            parsers.put("str.with.default", stringParser
                    .withDefault("defaultValue"));
            parsers.put("str.with.validator", stringParser
                    .withValidator("testValidator"::equals, "value must equal 'testValidator'"));
            parsers.put("str.with.default.and.validator", stringParser
                    .withDefault("defaultValue")
                    .withValidator((key, value) -> {
                        if (!"defaultValue".equals(value) && !"testValidator".equals(value)) {
                            throw new ConfigException("Validation failed for " + key + ": value must be 'defaultValue' or 'testValidator'");
                        }
                    }));

            parsers.put("int.val", intParser);
            parsers.put("int.with.default", intParser.withDefault(9999));

            parsers.put("long.val", longParser);
            parsers.put("long.with.default", longParser.withDefault(10L));

            parsers.put("uuid.val", uuidParser);

            parsers.put("boolean.val", booleanParser);
            parsers.put("boolean.with.default", booleanParser.withDefault(false));
        }

        TestConfig(String configPrefix, Map<Object, Object> map) {
            super(configPrefix, map, parsers);
        }
    }

    public static class TestConfig2 extends AbstractConfig {
        private static final HashMap<String, Parser> parsers = new HashMap<>();
        static {
            parsers.put("X.a.A", stringParser);
            parsers.put("X.a.B", stringParser);
            parsers.put("X.b", stringParser);
            parsers.put("X.c", stringParser);
            parsers.put("Y", stringParser);
            parsers.put("Z", stringParser);
        }

        TestConfig2(Map<Object, Object> map) {
            super("", map, parsers);
        }
    }

    @Test
    public void testParsers() {
        Map<Object, Object> map = new HashMap<>();
        UUID key = UUID.randomUUID();

        map.put("str.val", "stringValue");
        map.put("int.val", "1000");
        map.put("long.val", "2000");
        map.put("uuid.val", key.toString());
        map.put("boolean.val", "true");

        TestConfig config = new TestConfig("", map);
        Object value;

        value = config.get("str.val");
        assertTrue(value instanceof String);
        assertEquals("stringValue", value);

        value = config.get("int.val");
        assertTrue(value instanceof Integer);
        assertEquals(1000, value);

        value = config.get("long.val");
        assertTrue(value instanceof Long);
        assertEquals(2000L, value);

        value = config.get("uuid.val");
        assertTrue(value instanceof UUID);
        assertEquals(key, value);

        value = config.get("boolean.val");
        assertTrue(value instanceof Boolean);
        assertEquals(true, value);
    }

    @Test
    public void testIntegerToLongConversion() {
        Map<Object, Object> map = new HashMap<>();
        map.put("long.val", 1000);
        map.put("long.with.default", 1000);

        TestConfig config = new TestConfig("", map);
        Object value;

        value = map.get("long.val");
        assertTrue(value instanceof Integer);
        assertEquals(1000, value);

        value = config.get("long.val");
        assertTrue(value instanceof Long);
        assertEquals(1000L, value);

        value = map.get("long.with.default");
        assertTrue(value instanceof Integer);
        assertEquals(1000, value);

        value = config.get("long.with.default");
        assertTrue(value instanceof Long);
        assertEquals(1000L, value);
    }

    @Test
    public void testDefaultValues() {
        TestConfig config = new TestConfig("", Collections.emptyMap());
        Object value;

        try {
            config.get("str.val");
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        try {
            config.get("int.val");
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        try {
            config.get("long.val");
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        value = config.get("str.with.default");
        assertTrue(value instanceof String);
        assertEquals("defaultValue", value);

        value = config.get("int.with.default");
        assertTrue(value instanceof Integer);
        assertEquals(9999, value);

        value = config.get("long.with.default");
        assertTrue(value instanceof Long);
        assertEquals(10L, value);

        value = config.get("boolean.with.default");
        assertTrue(value instanceof Boolean);
        assertEquals(false, value);
    }

    @Test
    public void testValidator() {
        Map<Object, Object> map = new HashMap<>();
        map.put("str.with.validator", "testValidator");

        TestConfig config = new TestConfig("", map);
        Object value;

        // Normal validator
        value = config.get("str.with.validator");
        assertTrue(value instanceof String);
        assertEquals("testValidator", value);

        // Validator with default and no defined value
        value = config.get("str.with.default.and.validator");
        assertTrue(value instanceof String);
        assertEquals("defaultValue", value);

        // Validator with default and a defined value
        map = new HashMap<>();
        map.put("str.with.default.and.validator", "testValidator");
        config = new TestConfig("", map);

        value = config.get("str.with.default.and.validator");
        assertTrue(value instanceof String);
        assertEquals("testValidator", value);

        // Bad value for a validator
        map = new HashMap<>();
        map.put("str.with.validator", "badValue");

        try {
            new TestConfig("", map);
            fail();
        } catch (ConfigException ex) {
            // OK
        }

        // Bad value for a validator with a good default value
        map = new HashMap<>();
        map.put("str.with.default.and.validator", "anotherBadValue");

        try {
            new TestConfig("", map);
            fail();
        } catch (ConfigException ex) {
            // OK
        }
    }

    @Test
    public void testGetMissingValue() {
        TestConfig config = new TestConfig("", Collections.emptyMap());

        try {
            config.get("str.val");
            fail();
        } catch (Exception ex) {
            // OK
        }

        try {
            config.get("int.val");
            fail();
        } catch (Exception ex) {
            // OK
        }
    }

    @Test
    public void testGetOpt() {
        TestConfig config = new TestConfig("", Collections.emptyMap());
        Optional<Object> value;

        value = config.getOpt("str.val");
        assertFalse(value.isPresent());

        value = config.getOpt("str.with.default");
        assertTrue(value.isPresent());
        assertEquals("defaultValue", value.get());

        value = config.getOpt("int.val");
        assertFalse(value.isPresent());

        value = config.getOpt("int.with.default");
        assertTrue(value.isPresent());
        assertEquals(9999, value.get());
    }

    @Test
    public void testConfigPrefix() {
        String configPrefix = "test.";
        Map<Object, Object> map = new HashMap<>();
        map.put(configPrefix + "str.val", "stringValue");
        map.put(configPrefix + "int.val", "1000");
        map.put("dummy", "dummy");

        TestConfig config = new TestConfig(configPrefix, map);
        Object value;

        value = config.get("str.val");
        assertTrue(value instanceof String);
        assertEquals("stringValue", value);

        value = config.get("int.val");
        assertTrue(value instanceof Integer);
        assertEquals(1000, value);

        try {
            config.get("dummy");
            fail();
        } catch (Exception ex) {
            // OK
        }

        try {
            config.get("test.dummy");
            fail();
        } catch (Exception ex) {
            // OK
        }
    }

    @Test
    public void testConfigPrefixWithNestedMap() {
        String configPrefix = "nested.map.";
        Map<Object, Object> map0 = new HashMap<>();
        Map<Object, Object> map1 = new HashMap<>();
        Map<Object, Object> map2 = new HashMap<>();

        map2.put("str", Collections.singletonMap("val", "stringValue"));
        map2.put("int", Collections.singletonMap("val", "1000"));

        map1.put("map", map2);
        map1.put("dummy", "dummyValue");

        map0.put("nested", map1);
        map0.put("dummy", "dummyValue");

        TestConfig config = new TestConfig(configPrefix, map0);
        Object value;

        value = config.get("str.val");
        assertTrue(value instanceof String);
        assertEquals("stringValue", value);

        value = config.get("int.val");
        assertTrue(value instanceof Integer);
        assertEquals(1000, value);

        try {
            config.get("dummy");
            fail();
        } catch (Exception ex) {
            // OK
        }
        try {
            config.get("nested.dummy");
            fail();
        } catch (Exception ex) {
            // OK
        }
    }

    @Test
    public void testNestedMap() {
        Map<Object, Object> map0 = new HashMap<>();
        Map<Object, Object> map1 = new HashMap<>();
        Map<Object, Object> map2 = new HashMap<>();
        List<Object> list = Arrays.asList("p", "q", "r");
        TestConfig2 config;

        //
        // X:
        //    a:
        //       A: value
        //
        map2.put("A", "value");
        map1.put("a", map2);
        map0.put("X", map1);

        config = new TestConfig2(map0);
        assertEquals(config.get("X.a.A"), "value");

        map0.clear();
        map1.clear();
        map2.clear();

        //
        // X.a:
        //    A: value
        //
        map1.put("A", "value");
        map0.put("X.a", map1);

        config = new TestConfig2(map0);
        assertEquals(config.get("X.a.A"), "value");

        map0.clear();
        map1.clear();
        map2.clear();

        //
        // X:
        //    a.A: value
        //
        map1.put("a.A", "value");
        map0.put("X", map1);

        config = new TestConfig2(map0);
        assertEquals(config.get("X.a.A"), "value");

        //
        // X:
        //    a:
        //       A: [p, q, r]
        //       B: s
        //    b: D
        //    c: E
        // Y: d
        // Z: e
        //
        map0.clear();
        map1.clear();
        map2.clear();

        map2.put("A", list);
        map2.put("B", "s");

        map1.put("a", map2);
        map1.put("b", "D");
        map1.put("c", "E");

        map0.put("X", map1);
        map0.put("Y", "d");
        map0.put("Z", "e");

        config = new TestConfig2(map0);
        assertEquals(config.get("X.a.A"), Arrays.asList("p", "q", "r"));
        assertEquals(config.get("X.a.B"), "s");
        assertEquals(config.get("X.b"), "D");
        assertEquals(config.get("X.c"), "E");
        assertEquals(config.get("Y"), "d");
        assertEquals(config.get("Z"), "e");
    }

}
