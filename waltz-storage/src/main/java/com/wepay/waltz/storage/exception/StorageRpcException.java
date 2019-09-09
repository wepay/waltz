package com.wepay.waltz.storage.exception;

import com.wepay.riff.network.MessageAttributeReader;
import com.wepay.riff.network.MessageAttributeWriter;

public class StorageRpcException extends Exception {

    public StorageRpcException(String msg) {
        super(msg);
    }

    public StorageRpcException(Throwable cause) {
        super(cause.toString());

        setStackTrace(cause.getStackTrace());
    }

    public void writeTo(MessageAttributeWriter writer) {
        StackTraceElement[] stackTraceElements = getStackTrace();

        writer.writeString(getMessage());
        writer.writeInt(stackTraceElements.length);
        for (StackTraceElement elem: stackTraceElements) {
            writer.writeString(elem.getClassName());
            writer.writeString(elem.getMethodName());
            writer.writeString(elem.getFileName());
            writer.writeInt(elem.getLineNumber());
        }
    }

    public static StorageRpcException readFrom(MessageAttributeReader reader) {
        String message = reader.readString();
        StackTraceElement[] stackTraceElements = new StackTraceElement[reader.readInt()];
        for (int i = 0; i < stackTraceElements.length; i++) {
            String className = reader.readString();
            String methodName = reader.readString();
            String fileName = reader.readString();
            int lineNumber = reader.readInt();
            stackTraceElements[i] = new StackTraceElement(className, methodName, fileName, lineNumber);
        }
        StorageRpcException exception = new StorageRpcException(message);
        exception.setStackTrace(stackTraceElements);

        return exception;
    }

}
