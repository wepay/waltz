package com.wepay.waltz.demo;

import com.wepay.waltz.client.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DemoSerializer implements Serializer<Map<String, String>> {

    public static final DemoSerializer INSTANCE = new DemoSerializer();

    public byte[] serialize(Map<String, String> map) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             OutputStreamWriter writer = new OutputStreamWriter(baos, StandardCharsets.UTF_8)) {
            Iterator<Map.Entry<String, String>> iter = map.entrySet().iterator();
            Map.Entry<String, String> entry;

            if (iter.hasNext()) {
                entry = iter.next();
                writer.write(entry.getKey());
                writer.write(":");
                writer.write(entry.getValue());
            }

            while (iter.hasNext()) {
                entry = iter.next();
                writer.write("\t");
                writer.write(entry.getKey());
                writer.write(":");
                writer.write(entry.getValue());
            }
            writer.flush();

            return baos.toByteArray();

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Map<String, String> deserialize(byte[] bytes) {
        String string = new String(bytes, StandardCharsets.UTF_8);
        HashMap<String, String> map = new HashMap<>();

        String[] entries = string.split("\t");
        for (String entry : entries) {
            String[] kv = entry.split(":", 2);
            map.put(kv[0], kv[1]);
        }

        return map;
    }
}
