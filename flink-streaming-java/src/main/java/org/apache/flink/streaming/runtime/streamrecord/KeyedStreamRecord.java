/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Internal;

/**
 * A stream record containing the keyed information.
 *
 * @param <T> The type encapsulated with the stream record.
 */
@Internal
public class KeyedStreamRecord<T> extends StreamRecord<T> {
    private int keyGroup;

    public KeyedStreamRecord(T value) {
        super(value);
        this.keyGroup = -1;
    }

    public KeyedStreamRecord(T value, long timestamp) {
        super(value, timestamp);
        this.keyGroup = -1;
    }

    public KeyedStreamRecord(T value, int keyGroup) {
        super(value);
        this.keyGroup = keyGroup;
    }

    @Override
    public KeyedStreamRecord<T> copy(T valueCopy) {
        KeyedStreamRecord<T> copy = new KeyedStreamRecord<>(valueCopy);
        copy.setTimestamp(this.getTimestamp());
        copy.setKeyGroup(this.keyGroup);
        return copy;
    }

    @Override
    public void copyTo(T valueCopy, StreamRecord<T> target) {
        assert target instanceof KeyedStreamRecord;
        target.replace(valueCopy);
        target.setTimestamp(this.getTimestamp());
        ((KeyedStreamRecord<T>) target).setKeyGroup(this.keyGroup);
    }

    public void setKeyGroup(int keyGroup) {
        this.keyGroup = keyGroup;
    }

    public int getKeyGroup() {
        return keyGroup;
    }
}
