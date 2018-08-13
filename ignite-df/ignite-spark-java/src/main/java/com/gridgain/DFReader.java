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

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DFReader {

    private static final String CONFIG = "config/example-ignite.xml";

    private static final String CACHE_NAME = "sharedDF";

    public static void main(String args[]) {
        Ignite ignite = Ignition.start(CONFIG);

        SparkSession spark = SparkSession
                .builder()
                .appName("DFReader")
                .master("local")
                .config("spark.executor.instances", "2")
                .getOrCreate();

        Logger.getRootLogger().setLevel(Level.OFF);
        Logger.getLogger("org.apache.ignite").setLevel(Level.OFF);

        System.out.println("Reading data from Ignite table.");

        Dataset<Row> peopleDF = spark.read()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "people")
                .load();

        peopleDF.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people WHERE id > 0 AND id < 6");
        sqlDF.show();

        System.out.println("Done!");

        Ignition.stop(false);
    }
}