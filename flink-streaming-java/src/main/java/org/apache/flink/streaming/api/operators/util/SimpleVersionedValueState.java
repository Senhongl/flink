package org.apache.flink.streaming.api.operators.util;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.io.IOException;

public class SimpleVersionedValueState<T> implements ValueState<T> {

    private final ValueState<byte[]> rawState;

    private final SimpleVersionedSerializer<T> serializer;

    public SimpleVersionedValueState(
            ValueState<byte[]> rawState, SimpleVersionedSerializer<T> serializer) {
        this.rawState = rawState;
        this.serializer = serializer;
    }

    @Override
    public void clear() {
        rawState.clear();
    }

    @Override
    public T value() throws IOException {
        if (rawState.value() == null) { // default value is null
            return null;
        }
        return deserialize(rawState.value());
    }

    @Override
    public void update(@Nullable T value) throws IOException {
        rawState.update(serialize(value));
    }

    // ------------------------------------------------------------------------
    //  utils
    // ------------------------------------------------------------------------

    private byte[] serialize(T value) throws IOException {
        return SimpleVersionedSerialization.writeVersionAndSerialize(serializer, value);
    }

    private T deserialize(byte[] bytes) {
        try {
            return SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);
        } catch (IOException e) {
            throw new FlinkRuntimeException("Failed to deserialize value", e);
        }
    }
}
