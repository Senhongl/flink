package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class KeyGroupAssignerStateSerializer
        implements SimpleVersionedSerializer<Tuple2<String, Integer>> {

    private static int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(Tuple2<String, Integer> mapping) throws IOException {
        String splitId = mapping.f0;
        int keyGroup = mapping.f1;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bos)) {
            out.writeUTF(splitId);
            out.writeInt(keyGroup);
            out.flush();
            return bos.toByteArray();
        }
    }

    @Override
    public Tuple2<String, Integer> deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bis)) {
            String splitId = in.readUTF();
            int keyGroup = in.readInt();
            return new Tuple2<>(splitId, keyGroup);
        }
    }
}
