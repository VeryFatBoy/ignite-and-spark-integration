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

import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;

public class DFWriter {

    private static final String CONFIG = "config/example-ignite.xml";

    private static final String CACHE_NAME = "sharedDF";

    public static void main(String args[]) {
        Ignite ignite = Ignition.start(CONFIG);

        SparkSession spark = SparkSession
                .builder()
                .appName("DFWriter")
                .master("local")
                .config("spark.executor.instances", "2")
                .getOrCreate();

        Logger.getRootLogger().setLevel(Level.OFF);
        Logger.getLogger("org.apache.ignite").setLevel(Level.OFF);

        Dataset<Row> peopleDF = spark.read().json(
                resolveIgnitePath("resources/people.json").getAbsolutePath());

        System.out.println("JSON file contents:");

        peopleDF.show();

        System.out.println("Writing DataFrame to Ignite.");

        peopleDF.write()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "people")
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "id")
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(), "template=replicated")
                .save();

        System.out.println("Done!");

        Ignition.stop(false);
    }
}