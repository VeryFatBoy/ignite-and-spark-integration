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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import scala.Tuple2;

public class RDDReader {
    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("RDDReader")
                .setMaster("local")
                .set("spark.executor.instances", "2");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Logger.getRootLogger().setLevel(Level.OFF);
        Logger.getLogger("org.apache.ignite").setLevel(Level.OFF);

        JavaIgniteContext<Integer, Integer> igniteContext = new JavaIgniteContext<Integer, Integer>(
                sparkContext, "/path_to_ignite_home/examples/config/spark/example-shared-rdd.xml", true);

        JavaIgniteRDD<Integer, Integer> sharedRDD = igniteContext.<Integer, Integer>fromCache("sharedRDD");

        JavaPairRDD<Integer, Integer> greaterThanFiveHundred =
                sharedRDD.filter(new Function<Tuple2<Integer, Integer>, Boolean>() {
                    public Boolean call(Tuple2<Integer, Integer> tuple) throws Exception {
                        return tuple._2() > 500;
                    }
                });

        System.out.println("The count is " + greaterThanFiveHundred.count());

        System.out.println(">>> Executing SQL query over Ignite Shared RDD...");

        Dataset df = sharedRDD.sql("select _val from Integer where _val > 10 and _val < 100 limit 10");

        df.show();

        igniteContext.close(true);

        sparkContext.close();
    }
}