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
import org.apache.pig.test.MiniCluster;
import org.apache.pig.test.Util;

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
import java.io.FileWriter;
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
    private static MiniCluster cluster;
    private static final String STOPWORDS_FILE = "stop_words.txt";
	private static final String[] STOPWORDS = {
		"a", "able", "about", "across", "after", "all", "almost", "also", 
		"am", "among", "an", "and", "any", "are", "as", "at", "be", "because", 
		"been", "but", "by", "can", "cannot", "could", "dear", "did", "do", 
		"does", "either", "else", "ever", "every", "for", "from", "get", 
		"got", "had", "has", "have", "he", "her", "hers", "him", "his", 
		"how", "however", "i", "if", "in", "into", "is", "it", "its", 
		"just", "least", "let", "like", "likely", "may", "me", "might", 
		"most", "must", "my", "neither", "no", "nor", "not", "of", "off", 
		"often", "on", "only", "or", "other", "our", "own", "rather", "said", 
		"say", "says", "she", "should", "since", "so", "some", "than", "that", 
		"the", "their", "them", "then", "there", "these", "they", "this", "tis", 
		"to", "too", "twas", "us", "wants", "was", "we", "were", "what", "when", 
		"where", "which", "while", "who", "whom", "why", "will", "with", "would", 
		"yet", "you", "your", "que", "lol", "dont"};
    
    PigServer pig;
    String test_tuples;
	private Properties props;
    static boolean runMiniCluster = false;
	
    
    @Override
    @Before
    public void setUp() throws Exception {
    	System.setProperty("hadoop.log.dir", "build/test/logs");
    	
    	if (runMiniCluster) {
    		cluster = MiniCluster.buildCluster();
    		// Write out a stop list.    	
    		Util.createInputFile(cluster, STOPWORDS_FILE, STOPWORDS);
        	pig = new PigServer(ExecType.STORM, cluster.getProperties());
    	} else {
        	pig = new PigServer("storm-local");
    	}
    	
    	props = pig.getPigContext().getProperties();    	
    	props.setProperty("pig.streaming.run.test.cluster", "true");
//    	props.setProperty("pig.streaming.run.test.cluster.wait_time", "60000");
//    	props.setProperty("pig.streaming.debug", "true");
    	
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
    	if (runMiniCluster) {
    		cluster.shutDown();
    	}
    }
    
    @After
    public void tearDown() throws Exception {
    	if (runMiniCluster) {
    		Util.deleteFile(cluster, STOPWORDS_FILE);
    	}
    }
    
    public void explain(String alias) throws IOException {
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
    	pig.explain(alias, new PrintStream(baos));  	
    	System.err.print(new String(baos.toByteArray()));    	
    }
    
    @Test
    public void testUnion() throws Exception {
    	pig.registerQuery("x = LOAD '/dev/null/0' USING " +
    			"org.apache.pig.impl.storm.SpoutWrapper(" +
    				"'org.apache.pig.test.storm.TestSentenceSpout') AS (sentence:chararray);");
    	pig.registerQuery("x = FOREACH x GENERATE FLATTEN(TOKENIZE(sentence));");
    	pig.registerQuery("x = FOREACH x GENERATE LOWER($0) AS word;");

    	pig.registerQuery("y = LOAD '/dev/null/1' USING " +
    			"org.apache.pig.impl.storm.SpoutWrapper(" +
    				"'org.apache.pig.test.storm.TestSentenceSpout') AS (sentence:chararray);");
    	
    	pig.registerQuery("q = UNION x,y;");
    	
    	pig.registerQuery("y = FOREACH y GENERATE FLATTEN(TOKENIZE(sentence));");
    	pig.registerQuery("y = FOREACH y GENERATE LOWER($0) AS word;");
    	
    	pig.registerQuery("z = UNION x,y;");
    	pig.registerQuery("r = UNION q,z;");

//    	pig.registerQuery("STORE r INTO 'fake/pathr';");
//    	explain("r");
    }
    
    @Test
    public void testJoin() throws Exception {
    	// Create the input file ourselves.
    	File stopfile = new File(STOPWORDS_FILE);
//    	System.out.println("f:" + stopfile.getAbsolutePath());
    	FileWriter fh = new FileWriter(stopfile);
    	for (String w : STOPWORDS) {
//    		System.out.println("w:" + w);
    		fh.write(w);
    		fh.write("\n");
    	}
    	fh.close();
    	
    	pig.registerQuery("x = LOAD '/dev/null/0' USING " +
    			"org.apache.pig.impl.storm.SpoutWrapper(" +
    				"'org.apache.pig.test.storm.TestSentenceSpout') AS (sentence:chararray);");
    	pig.registerQuery("x = FOREACH x GENERATE FLATTEN(TOKENIZE(sentence));");
    	pig.registerQuery("x = FOREACH x GENERATE LOWER($0) AS word, 'x';");

    	pig.registerQuery("y = LOAD '/dev/null/1' USING " +
    			"org.apache.pig.impl.storm.SpoutWrapper(" +
    				"'org.apache.pig.test.storm.TestSentenceSpout2') AS (sentence:chararray);");
    	pig.registerQuery("y = FOREACH y GENERATE FLATTEN(TOKENIZE(sentence));");
    	pig.registerQuery("y = FOREACH y GENERATE LOWER($0) AS word, 'y';");

    	pig.registerQuery("stoplist = LOAD '" + STOPWORDS_FILE + "' AS (stopword:chararray);");

    	pig.registerQuery("wordsr = JOIN x BY word, stoplist BY stopword USING 'replicated';");
//    	explain("wordsr");
//    	pig.registerQuery("STORE wordsr INTO 'fake_path';");
    	
    	pig.registerQuery("words = JOIN x BY word, stoplist BY stopword;");
//    	pig.registerQuery("STORE words INTO 'fake_path';");
//    	explain("words");

//    	pig.registerQuery("x = FILTER x BY $0 == 'the';");
//    	pig.registerQuery("y = FILTER y BY $0 == 'the';");
    	pig.registerQuery("silly = JOIN x BY word, y BY word;");
//    	pig.registerQuery("STORE x INTO 'fake_path';");
//    	explain("silly");    	
//    	pig.registerQuery("STORE silly INTO 'fake_path';");
    	
    	stopfile.delete();
    }
    
    @Test
    public void testWCHist () throws Exception {
    	// storm.trident.testing.FixedBatchSpout
    	// backtype.storm.testing.FixedTupleSpout
    	pig.registerQuery("x = LOAD '/dev/null' USING " +
    			"org.apache.pig.impl.storm.SpoutWrapper(" +
    				"'org.apache.pig.test.storm.TestSentenceSpout') AS (sentence:chararray);");

    	// STREAM is asynchronous is how it returns results, we don't have enough to make it work in this case.
//    	pig.registerQuery("x = STREAM x THROUGH `tr -d '[:punct:]'` AS (sentence:chararray);");

    	pig.registerQuery("x = FOREACH x GENERATE FLATTEN(TOKENIZE(sentence));");
    	pig.registerQuery("x = FOREACH x GENERATE LOWER($0) AS word;");
    	
//    	props.setProperty("count_gr_store_opts", "{\"StateFactory\":\"edu.umd.estuary.storm.trident.state.RedisState\", \"StaticMethod\": \"fromJSONArgs\", \"args\": [{\"servers\": \"localhost\", \"dbNum\": 3, \"expiration\": 300, \"serializer\":\"org.apache.pig.impl.storm.state.GZPigSerializer\", \"key_serializer\":\"org.apache.pig.impl.storm.state.PigTextSerializer\"}]}");
//    	props.setProperty("count_gr_store_opts", "{\"StateFactory\":\"edu.umd.estuary.storm.trident.state.RedisState\", \"StaticMethod\": \"fromJSONArgs\", \"args\": [{\"servers\": \"localhost\", \"dbNum\": 3, \"serializer\":\"org.apache.pig.impl.storm.state.PigSerializer\", \"key_serializer\":\"org.apache.pig.impl.storm.state.PigTextSerializer\"}]}");
    	pig.registerQuery("count_gr = GROUP x BY word;");
    	pig.registerQuery("count = FOREACH count_gr GENERATE group AS word, COUNT(x) AS wc;");
    	
//    	props.setProperty("hist_gr_store_opts", "{\"StateFactory\":\"edu.umd.estuary.storm.trident.state.RedisState\", \"StaticMethod\": \"fromJSONArgs\", \"args\": [{\"servers\": \"localhost\", \"dbNum\": 4, \"expiration\": 300, \"serializer\":\"org.apache.pig.impl.storm.state.PigSerializer\", \"key_serializer\":\"org.apache.pig.impl.storm.state.PigTextSerializer\"}]}");
    	pig.registerQuery("hist_gr = GROUP count BY wc;");
    	pig.registerQuery("hist = FOREACH hist_gr GENERATE group AS wc, COUNT(count) AS freq;");
    	pig.registerQuery("hist = FILTER hist BY freq > 0;");
    	
    	pig.registerQuery("q = GROUP x BY 1;");
    	pig.registerQuery("c = FOREACH q GENERATE COUNT(x);");
//    	pig.registerQuery("STORE c INTO '/dev/null/1';");
    	
//    	pig.registerQuery("STORE hist INTO '/dev/null/1';");
//    	pig.registerQuery("STORE x INTO '/dev/null/1';");
//    	pig.registerQuery("STORE count_gr INTO '/dev/null/1';");
//    	pig.registerQuery("STORE count INTO '/dev/null/1';");
    	
//    	explain("hist");
    }

    @Test
    public void testWindow() throws Exception {
    	pig.registerQuery("x = LOAD '/dev/null' USING " +
    			"org.apache.pig.impl.storm.SpoutWrapper(" +
    				"'org.apache.pig.test.storm.TestSentenceSpout') AS (sentence:chararray);");
    	
    	pig.registerQuery("x = FOREACH x GENERATE FLATTEN(TOKENIZE(sentence));");
    	pig.registerQuery("x = FOREACH x GENERATE LOWER($0) AS word;");
    	pig.registerQuery("x = FILTER x BY word == 'the';");
    	props.setProperty("count_gr_window_opts", "{\"0\":2}");
    	pig.registerQuery("count_gr = GROUP x BY word;");
    	pig.registerQuery("count = FOREACH count_gr GENERATE group AS word, COUNT(x) AS wc;");
//    	props.setProperty("hist_gr_window_opts", "{\"0\":2}");
//    	props.setProperty("hist_gr_store_opts", "{\"StateFactory\":\"edu.umd.estuary.storm.trident.state.RedisState\", \"StaticMethod\": \"fromJSONArgs\", \"args\": [{\"servers\": \"localhost\", \"dbNum\": 4, \"expiration\": 300, \"serializer\":\"org.apache.pig.impl.storm.state.PigSerializer\", \"key_serializer\":\"org.apache.pig.impl.storm.state.PigTextSerializer\"}]}");
    	pig.registerQuery("hist_gr = GROUP count BY wc;");
    	pig.registerQuery("hist = FOREACH hist_gr GENERATE group AS wc, COUNT(count) AS freq;");
    	pig.registerQuery("hist = FILTER hist BY freq > 0;");

//    	explain("count");
    	pig.registerQuery("STORE count INTO '/dev/null/1';");
//    	explain("hist");
//    	pig.registerQuery("STORE hist INTO '/dev/null/1';");
    	
    }
}
