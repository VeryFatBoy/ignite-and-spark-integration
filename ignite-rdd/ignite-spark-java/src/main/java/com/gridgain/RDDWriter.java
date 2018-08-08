/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gridgain;

import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class RDDWriter {
    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("RDDWriter")
                .setMaster("local")
                .set("spark.executor.instances", "2");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Logger.getRootLogger().setLevel(Level.OFF);
        Logger.getLogger("org.apache.ignite").setLevel(Level.OFF);

        JavaIgniteContext<Integer, Integer> igniteContext = new JavaIgniteContext<Integer, Integer>(
                sparkContext, "/path_to_ignite_home/examples/config/spark/example-shared-rdd.xml", true);

        JavaIgniteRDD<Integer, Integer> sharedRDD = igniteContext.<Integer, Integer>fromCache("sharedRDD");

        List<Integer> data = new ArrayList<>(20);

        for (int i = 1001; i <= 1020; i++) {
            data.add(i);
        }

        JavaRDD<Integer> javaRDD = sparkContext.<Integer>parallelize(data);

        sharedRDD.savePairs(javaRDD.<Integer, Integer>mapToPair(new PairFunction<Integer, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(Integer val) throws Exception {
                return new Tuple2<Integer, Integer>(val, val);
            }
        }));

        igniteContext.close(true);

        sparkContext.close();
    }
}