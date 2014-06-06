/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.test.storm;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import org.apache.pig.impl.io.FileLocalizer;

import org.apache.pig.backend.executionengine.ExecException;

import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import junit.framework.TestCase;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import storm.trident.testing.LRUMemoryMapState;

import backtype.storm.testing.FixedTupleSpout;
import backtype.storm.topology.base.BaseRichSpout;
/*
 * Testcase aimed at testing pig with large file sizes and filter and group functions
*/
@RunWith(JUnit4.class)
public class TestStream extends TestCase {
    
    private final Log log = LogFactory.getLog(getClass());

    PigServer pig;
    String test_tuples;
    
    @Override
    @Before
    public void setUp() throws Exception{
    	pig = new PigServer("storm");    	
    }
    
    @Test
    public void testWCHist () throws Exception {
    	
    	Properties props = pig.getPigContext().getProperties();
    	
    	props.setProperty("pig.streaming.topology.name", "word_hist");
    	props.setProperty("pig.streaming.run.test.cluster", "true");
//    	props.setProperty("pig.streaming.run.test.cluster.wait_time", "60000");
//    	props.setProperty("pig.streaming.debug", "true");
    	
    	// storm.trident.testing.FixedBatchSpout
    	// backtype.storm.testing.FixedTupleSpout
    	pig.registerQuery("x = LOAD '/dev/null' USING " +
    			"org.apache.pig.impl.storm.SpoutWrapper(" +
    				"'org.apache.pig.test.storm.TestSentenceSpout') AS (sentence:chararray);");

    	// STREAM is asynchronous is how it returns results, we don't have enough to make it work in this case.
//    	pig.registerQuery("x = STREAM x THROUGH `tr -d '[:punct:]'` AS (sentence:chararray);");

    	pig.registerQuery("x = FOREACH x GENERATE FLATTEN(TOKENIZE(sentence));");
    	pig.registerQuery("x = FOREACH x GENERATE LOWER($0) AS word;");
//    	props.setProperty("count_gr_store_opts", "{\"StateFactory\":\"edu.umd.estuary.storm.trident.state.RedisState\", \"StaticMethod\": \"fromJSONArgs\", \"args\": [{\"servers\": \"localhost\", \"dbNum\": 3, \"expiration\": 300, \"serializer\":\"org.apache.pig.impl.storm.state.PigSerializer\", \"key_serializer\":\"org.apache.pig.impl.storm.state.PigTextSerializer\"}]}");
    	
    	pig.registerQuery("count_gr = GROUP x BY word;");
    	pig.registerQuery("count = FOREACH count_gr GENERATE group AS word, COUNT(x) AS wc;");
    	pig.registerQuery("hist_gr = GROUP count BY wc;");
    	pig.registerQuery("hist = FOREACH hist_gr GENERATE group AS wc, COUNT(count) AS freq;");
    	
    	pig.registerQuery("q = GROUP x BY 1;");
    	pig.registerQuery("c = FOREACH q GENERATE COUNT(x);");
//    	pig.registerQuery("STORE c INTO '/dev/null/1';");
    	
    	pig.registerQuery("STORE hist INTO '/dev/null/1';");
//    	pig.registerQuery("STORE x INTO '/dev/null/1';");
//    	pig.registerQuery("STORE count_gr INTO '/dev/null/1';");
//    	pig.registerQuery("STORE count INTO '/dev/null/1';");
    	
//    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
////    	pig.explain("count_gr", new PrintStream(baos));  	
//    	pig.explain("c", new PrintStream(baos));  	
//    	System.err.print(new String(baos.toByteArray()));
            
    	
    }

}
