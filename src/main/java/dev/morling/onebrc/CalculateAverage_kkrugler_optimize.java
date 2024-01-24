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

public class CalculateAverage_kkrugler_optimize {

    static final int NUM_TRIALS = 10;

    public static void main(String[] args) {
        int minThreads = Integer.parseInt(args[0]);
        int maxThreads = Integer.parseInt(args[1]);
        int minBlockSizeMB = Integer.parseInt(args[2]);
        int maxBlockSizeMB = Integer.parseInt(args[3]);

        for (int numThreads = minThreads; numThreads <= maxThreads; numThreads += 2) {
            for (int blockSizeMB = minBlockSizeMB; blockSizeMB <= maxBlockSizeMB; blockSizeMB *= 2) {
                long totalTime = 0;
                for (int trial = 0; trial < NUM_TRIALS; trial++) {
                    String[] testArgs = new String[]{ Integer.toString(numThreads), Integer.toString(blockSizeMB) };

                    long startTimeMS = System.currentTimeMillis();
                    CalculateAverage_kkrugler.main(testArgs);
                    long endTimeMS = System.currentTimeMillis();
                    totalTime += (endTimeMS - startTimeMS);
                }

                System.out.format("%d/%d: %d\n", numThreads, blockSizeMB, totalTime / NUM_TRIALS);
            }
        }

    }

}
