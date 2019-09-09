package com.wepay.waltz.client;

/**
 * An interface for converting objects to bytes and vice-versa.
 *
 * @param <T> Type to be serialized from/de-serialized to.
 */
public interface Serializer<T> {

    /**
     * Converts {@code data} into a {@code byte} array.
     *
     * @param data the typed data to serialize.
     * @return serialized data as an array of bytes.
     */
    byte[] serialize(T data);

    /**
     * Converts {@code bytes} to an object of type {@code T}.
     *
     * @param bytes an array of bytes to de-serialize
     * @return the serialized object of type {@code T}.
     */
    T deserialize(byte[] bytes);

}
