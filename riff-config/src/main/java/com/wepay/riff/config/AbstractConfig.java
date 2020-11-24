package com.wepay.riff.config;

import com.wepay.riff.config.validator.BooleanValidator;
import com.wepay.riff.config.validator.Validator;
import com.wepay.riff.util.Logging;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

public abstract class AbstractConfig implements Config {

    private static final Logger logger = Logging.getLogger(AbstractConfig.class);

    // Parsers
    protected static final Parser stringParser = new Parser(null);
    protected static final Parser intParser = new Parser(Integer::parseInt);
    protected static final Parser longParser = new LongParser();
    protected static final Parser uuidParser = new Parser(UUID::fromString);
    protected static final Parser booleanParser = new Parser(Boolean::parseBoolean);

    protected final String configPrefix;
    protected final Map<Object, Object> configValues;
    protected final Map<String, Object> parameterValues;
    protected final Map<String, Parser> parsers;

    public AbstractConfig(String configPrefix, Map<Object, Object> map, Map<String, Parser> parsers) {
        this.configPrefix = configPrefix;
        this.configValues = map;
        this.parsers = parsers;
        this.parameterValues = new HashMap<>();

        load(configValues, "");
    }

    @SuppressWarnings("unchecked")
    private void load(Map<Object, Object> nestedMap, String parentKey) {
        for (Map.Entry<Object, Object> entry : nestedMap.entrySet()) {
            String prefixedKey = parentKey + entry.getKey().toString();
            Object value = entry.getValue();

            Parser parser = null;
            String key = null;
            if (prefixedKey.startsWith(configPrefix)) {
                key = prefixedKey.substring(configPrefix.length());
                parser = parsers.get(key);
            }

            if (parser != null) {
                // We have a parser. It means that this is a config parameter.
                parameterValues.put(key, value);

                parser.validate(key, value);

            } else if (value instanceof Map) {
                // Descend into a nested map
                load((Map<Object, Object>) value, prefixedKey + ".");
            }
            // else ignore
        }
    }

    public Object get(String key) {
        Optional<Object> opt = getOpt(key);

        if (opt.isPresent()) {
            return opt.get();
        } else {
            logger.error("parameter not found: " + configPrefix + key);
            throw new ConfigException("parameter not found: " + configPrefix + key);
        }
    }

    public Optional<Object> getOpt(String key) {
        Parser parser = parsers.get(key);

        if (parser == null) {
            logger.error("unknown config parameter: " + configPrefix + key);
            throw new ConfigException("unknown config parameter: " + configPrefix + key);
        }

        try {
            return Optional.ofNullable(parser.parse(parameterValues.get(key)));
        } catch (Exception ex) {
            logger.error("failed to parse: " + configPrefix + key, ex);
            throw new ConfigException("failed to parse: " + configPrefix + key, ex);
        }
    }

    public Object getObject(String key) {
        return parameterValues.get(configPrefix + key);
    }

    public void setObject(String key, Object obj) {
        parameterValues.put(configPrefix + key, obj);
        Parser parser = parsers.get(key);

        if (parser != null) {
            parser.validate(key, obj);
        }
    }

    protected static class Parser {

        final Function<String, Object> specParserFunction;
        final Object defaultValue;
        final Validator validator;

        public Parser(Function<String, Object> specParserFunction) {
            this(specParserFunction, null, null);
        }

        private Parser(Function<String, Object> specParserFunction, Object defaultValue, Validator validator) {
            this.specParserFunction = specParserFunction;
            this.defaultValue = defaultValue;
            this.validator = validator;
        }

        public Object parse(Object spec) {
            if (spec == null) {
                return defaultValue;
            }

            try {
                if (specParserFunction != null && spec instanceof String) {
                    return specParserFunction.apply((String) spec);
                } else {
                    return spec;
                }
            } catch (Throwable cause) {
                throw new ConfigException("parsing failed", cause);
            }
        }

        public void validate(String key, Object spec) throws ConfigException {
            if (validator != null) {
                validator.validate(key, parse(spec));
            }
        }

        public Parser withDefault(Object defaultValue) {
            return new Parser(specParserFunction, defaultValue, validator);
        }

        public Parser withValidator(Validator validator) { return new Parser(specParserFunction, defaultValue, validator); }

        public Parser withValidator(Function<Object, Boolean> func, String errorMessage) { return new Parser(specParserFunction, defaultValue, new BooleanValidator(func, errorMessage)); }
    }

    private static class LongParser extends Parser {

        LongParser() {
            super(Long::parseLong);
        }

        LongParser(Object defaultValue, Validator validator) {
            super(Long::parseLong, defaultValue, validator);
        }

        @Override
        public Object parse(Object spec) {
            return spec instanceof Integer ? ((Integer) spec).longValue() : super.parse(spec);
        }

        @Override
        public Parser withDefault(Object defaultValue) {
            return new LongParser(defaultValue, validator);
        }

        public Parser withValidator(Validator validator) { return new LongParser(defaultValue, validator); }

        public Parser withValidator(Function<Object, Boolean> func, String errorMessage) { return new LongParser(defaultValue, new BooleanValidator(func, errorMessage)); }
    }

}

