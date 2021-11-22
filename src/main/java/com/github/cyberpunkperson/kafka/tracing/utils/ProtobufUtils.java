package com.github.cyberpunkperson.kafka.tracing.utils;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.util.UUID;

import static com.google.protobuf.ByteString.copyFrom;

public class ProtobufUtils {

    public static ByteString uuidToByteString(UUID uuid) {
        if (uuid == null) {
            return null;
        }
        ByteBuffer uuidByteBuffer = ByteBuffer.wrap(new byte[16]);
        uuidByteBuffer.putLong(uuid.getMostSignificantBits());
        uuidByteBuffer.putLong(uuid.getLeastSignificantBits());
        return copyFrom(uuidByteBuffer.array());
    }

    public static UUID byteStringToUUID(ByteString uuidStr) {
        if (uuidStr == null || uuidStr.isEmpty()) {
            return null;
        }
        ByteBuffer byteBuffer = uuidStr.asReadOnlyByteBuffer();
        return new UUID(byteBuffer.getLong(), byteBuffer.getLong());
    }

    public static String byteStringToUUIDString(ByteString uuidStr) {
        return byteStringToUUID(uuidStr).toString();
    }
}
