/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class CalculateAverage_kkrugler {

    private static final String FILE = "./measurements.txt";

    private static final int NUM_THREADS = 20;
    private static final int BUFFER_SIZE = 128 * 1024 * 1024;

    private static final int NUM_STATION_NAMES = 10_000;

    private static final byte SEMI_COLON = (byte) ';';
    private static final byte DECIMAL = (byte) '.';
    private static final byte NEW_LINE = (byte) '\n';

    private static final int MAX_STATION_NAME_LENGTH = 100;
    private static final int MAX_VALUE_LENGTH = "-99.9".length();
    private static final int MAX_LINE_LENGTH = MAX_STATION_NAME_LENGTH + ";".length()
            + MAX_VALUE_LENGTH + "\n".length();

    private static record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    // TODO - use long = actual * 10
    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    public static void main(String[] args) throws IOException {
        // System.out.print("Press return when profiling is ready...");
        // readInputLine();

        Map<String, MeasurementAggregator> globalMap = new HashMap<>(NUM_STATION_NAMES);

        final Path filePath = Paths.get(FILE);
        final CountDownLatch latch = new CountDownLatch(NUM_THREADS);
        final AtomicLong curOffset = new AtomicLong(0);

        for (int i = 0; i < NUM_THREADS; i++) {
            Thread t = new Thread(new Runnable() {

                @Override
                public void run() {
                    NIOFileReader reader = null;
                    try {
                        reader = new NIOFileReader(filePath);
                        StationMap map = new StationMap(NUM_STATION_NAMES);

                        while (reader.process(curOffset.getAndAdd(BUFFER_SIZE), map)) {

                        }

                        // TODO - use array of maps, combine at end.
                        synchronized (globalMap) {
                            for (Entry<StationMap.StationNameKey, MeasurementAggregator> e : map.entrySet()) {
                                String stationName = e.getKey().getNameAsString();

                                MeasurementAggregator curAgg = globalMap.get(stationName);
                                MeasurementAggregator newAgg = e.getValue();
                                if (curAgg == null) {
                                    globalMap.put(stationName, newAgg);
                                }
                                else {
                                    curAgg.min = Math.min(curAgg.min, newAgg.min);
                                    curAgg.max = Math.max(curAgg.max, newAgg.max);
                                    curAgg.sum += newAgg.sum;
                                    curAgg.count += newAgg.count;
                                }
                            }
                        }
                    }
                    catch (IOException e) {
                        throw new RuntimeException("", e);
                    }
                    finally {
                        latch.countDown();
                        safeClose(reader);
                    }
                }
            }, "NIO Thread #" + i);

            t.start();
        }

        try {
            latch.await();

            // Create sorted output, based on station name.
            TreeMap<String, ResultRow> results = new TreeMap<>();
            for (Entry<String, MeasurementAggregator> e : globalMap.entrySet()) {
                MeasurementAggregator ma = e.getValue();
                results.put(e.getKey(), new ResultRow(ma.min, ma.sum / ma.count, ma.max));
            }

            System.out.println(results);
        }
        catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

    }

    private static class StationMap {
        private Map<StationNameKey, MeasurementAggregator> map;

        private byte[] nameBuffer;
        private int curNameStart;
        private int curNameEnd;
        private int curNameHashCode;

        private StationNameKey tempKey;

        public StationMap(int capacity) {
            this.map = new HashMap<>(capacity);

            nameBuffer = new byte[MAX_STATION_NAME_LENGTH * NUM_STATION_NAMES];
            curNameStart = 0;

            resetCurName();

            tempKey = new StationNameKey();
        }

        public Set<Entry<StationNameKey, MeasurementAggregator>> entrySet() {
            return map.entrySet();
        }

        public void addNameByte(byte b) {
            nameBuffer[curNameEnd++] = b;

            int h = (int) b;

            curNameHashCode += h & 0x0FFL;
            curNameHashCode += (curNameHashCode << 10);
            curNameHashCode ^= (curNameHashCode >> 6);
        }

        public void initTempKey() {
            tempKey.offset = curNameStart;
            tempKey.length = curNameEnd - curNameStart;
            tempKey.hashCode = curNameHashCode;

            tempKey.hashCode += (tempKey.hashCode << 3);
            tempKey.hashCode ^= (tempKey.hashCode >> 11);
            tempKey.hashCode += (tempKey.hashCode << 15);
        }

        public MeasurementAggregator getWithCurName() {
            initTempKey();

            return map.get(tempKey);
        }

        public void putWithCurName(MeasurementAggregator curResult) {
            initTempKey();

            StationNameKey newKey = new StationNameKey(tempKey);
            map.put(newKey, curResult);

            curNameStart = curNameEnd;
            curNameHashCode = 0;
        }

        public void resetCurName() {
            curNameEnd = curNameStart;
            curNameHashCode = 0;
        }

        public class StationNameKey {
            private int offset;
            private int length;
            private int hashCode;

            public StationNameKey() {
            }

            public StationNameKey(StationNameKey base) {
                this.offset = base.offset;
                this.length = base.length;
                this.hashCode = base.hashCode;
            }

            public String getNameAsString() {
                return new String(nameBuffer, offset, length, StandardCharsets.UTF_8);
            }

            @Override
            public int hashCode() {
                return hashCode;
            }

            @Override
            public boolean equals(Object obj) {
                StationNameKey other = (StationNameKey) obj;

                return hashCode == other.hashCode
                        && length == other.length;
                // && Arrays.equals(nameBuffer, offset, offset + length, nameBuffer, other.offset, other.offset + other.length);
            }
        }

    }

    private static class NIOFileReader implements Closeable {

        private SeekableByteChannel channel;
        // TODO - use buffer.get(), not buffer.get(offset)
        private ByteBuffer directBuffer;
        private byte[] buffer;

        public NIOFileReader(Path filePath) throws IOException {
            channel = Files.newByteChannel(filePath, StandardOpenOption.READ);

            // We try to read MAX_LINE_LENGTH more bytes, so that we can always
            // process the "last" entry, which will always extend into the next
            // block (unless we're the very last block). This works because we
            // always skip the first entry, unless we're the first block.
            directBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE + MAX_LINE_LENGTH);
            buffer = new byte[BUFFER_SIZE + MAX_LINE_LENGTH];
        }

        public boolean process(long startOffset, StationMap map) throws IOException {
            if (startOffset >= channel.size()) {
                return false;
            }

            channel.position(startOffset);
            long bytesRead = channel.read(directBuffer);
            directBuffer.rewind();

            // Our limit is either set by the true end of file, or we don't want
            // to read another entry once we've processed the next entry in the buffer.
            long readLimit = Math.min(bytesRead, BUFFER_SIZE + 1);

            directBuffer.get(buffer, 0, (int) bytesRead);

            int readPointer = 0;
            byte curByte = 0;
            if (startOffset > 0) {
                // Skip first entry if we're not the first block. We assume that
                // we have enough data for at least one entry.
                while ((curByte = buffer[readPointer++]) != NEW_LINE) {
                    // Skip the first entry in our block
                }
            }

            while (readPointer < readLimit) {
                while ((curByte = buffer[readPointer++]) != SEMI_COLON) {
                    map.addNameByte(curByte);
                }

                double curValue = 0;
                curByte = buffer[readPointer++];
                double sign = 1.0;
                if (curByte == (byte) '-') {
                    sign = -1.0;
                }
                else {
                    curValue = curByte - (byte) '0';
                }

                while ((curByte = buffer[readPointer++]) != DECIMAL) {
                    curValue = (curValue * 10.0) + (curByte - (byte) '0');
                }

                curValue += (buffer[readPointer++] - (byte) '0') / 10.0;
                curValue *= sign;

                // Get rid of newline
                readPointer++;

                MeasurementAggregator curResult = map.getWithCurName();
                if (curResult == null) {
                    curResult = new MeasurementAggregator();
                    map.putWithCurName(curResult);
                }
                else {
                    // We can reset our offset into the buffer, since we don't need
                    // to save the new station name.
                    map.resetCurName();
                }

                curResult.min = Math.min(curResult.min, curValue);
                curResult.max = Math.max(curResult.max, curValue);
                curResult.sum += curValue;
                curResult.count += 1;
            }

            return true;
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

    }

    private static String readInputLine() {
        try {
            return new BufferedReader(new InputStreamReader(System.in)).readLine();
        }
        catch (IOException e) {
            throw new RuntimeException("Unexpected exception!", e);
        }
    }

    private static void safeClose(Closeable obj) {
        if (obj != null) {
            try {
                obj.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
