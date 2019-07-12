package com.wepay.waltz.client;

public interface Serializer<T> {

    byte[] serialize(T data);

    T deserialize(byte[] bytes);

}
