package com.wepay.waltz.storage.server.health;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wepay.waltz.storage.WaltzStorage;

import java.util.HashMap;
import java.util.Map;

public class Healthcheck {

    private final WaltzStorage waltzStorage;

    public Healthcheck(WaltzStorage waltzStorage) {
        this.waltzStorage = waltzStorage;
    }

    public boolean isHealthy() {
        return true;
    }

    public String getMessage() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> health = new HashMap<>();

        health.put("storage", Boolean.toString(isHealthy()));

        return mapper.writeValueAsString(health);
    }
}
