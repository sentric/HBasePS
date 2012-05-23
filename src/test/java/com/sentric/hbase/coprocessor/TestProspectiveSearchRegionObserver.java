/**
 * Copyright 2012 Sentric AG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sentric.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.sentric.hbase.coprocessor.ProspectiveSearchRegionObserver;
import ch.sentric.hbase.table.AccountTable;
import ch.sentric.hbase.table.ArticleTable;
import ch.sentric.hbase.table.ReportTable;

/**
 * 
 */
public class TestProspectiveSearchRegionObserver {

    private static final HBaseTestingUtility TEST_UTIL;
    private static Map<String, Map<String,String>> ACCOUNTS;
    
    static {
	final Configuration conf = new Configuration();
	conf.addResource("hbase-default-test.xml");
	conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY, ProspectiveSearchRegionObserver.class.getName());
	conf.set("solr.home", "${user.dir}/solr");
	TEST_UTIL = new HBaseTestingUtility(conf);
	
	ACCOUNTS = new HashMap<String, Map<String,String>>(0);
	
	ACCOUNTS.put("acc01", new HashMap<String, String>(0));
	ACCOUNTS.get("acc01").put("agent1", "baseball AND summer");
	ACCOUNTS.get("acc01").put("agent2", "apache AND lucene");
	
	ACCOUNTS.put("acc02", new HashMap<String, String>(0));
	ACCOUNTS.get("acc02").put("agent2", "hockey AND winter");

	ACCOUNTS.put("acc03", new HashMap<String, String>(0));
	ACCOUNTS.get("acc03").put("agent3", "tennis");
    }
    
    private static final byte[] row1 = Bytes.toBytes("r1");
    private static final byte[] dummyContent = Bytes.toBytes("baseball is played during summer months.");

    private static void fillAccountTable(Map<String, Map<String, String>> accounts) throws Exception {
        HTable tbl = new HTable(TEST_UTIL.getConfiguration(), AccountTable.NAME);
        List<Put> puts = new ArrayList<Put>();
        for(Entry<String, Map<String, String>> entry : accounts.entrySet()) {
            for (Map.Entry<String, String> e: entry.getValue().entrySet()) {
        	Put put = new Put(Bytes.toBytes(entry.getKey()));
        	put.add(AccountTable.AGENT_FAMILIY, Bytes.toBytes(e.getKey()), Bytes.toBytes(e.getValue()));
        	puts.add(put);
            }
            
        }
        tbl.put(puts);
        tbl.close();
    }

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
	long startInNsec = System.nanoTime();
	TEST_UTIL.startMiniCluster(1);
	TEST_UTIL.createTable(ArticleTable.NAME, ArticleTable.ARTICLE_FAMILIY);
	TEST_UTIL.createTable(AccountTable.NAME, AccountTable.AGENT_FAMILIY);
	TEST_UTIL.createTable(ReportTable.NAME, ReportTable.DOC_FAMILIY);
	fillAccountTable(ACCOUNTS);
	long upInNsec = System.nanoTime();
	float delta = (float)(upInNsec - startInNsec)/1000000000;
	System.out.printf("Cluster up in %f seconds\n", delta);
    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
	long stopInNsec = System.nanoTime();
	TEST_UTIL.shutdownMiniCluster();
	long downInNsec = System.nanoTime();
	float delta = (float)(downInNsec - stopInNsec)/1000000000;
	System.out.printf("Cluster down in %f seconds\n", delta);
    }

    @Test
    public void writeArticleShouldMatchAgent() throws Exception {
	HTable t = new HTable(TEST_UTIL.getConfiguration(), Bytes.toString(ArticleTable.NAME));
	Put p = new Put(row1);
	p.add(ArticleTable.ARTICLE_FAMILIY, ArticleTable.ARTICLE_QUALIFIER, dummyContent);
	t.put(p);
	checkRowAndDelete(t, row1, 1);
	checkRowAndDelete(new HTable(TEST_UTIL.getConfiguration(), Bytes.toString(ReportTable.NAME)), assembleRowKey("acc01", "agent1", p.getTimeStamp()), 1);
    }
    
    @Test
    public void writeArticleShouldNotMatch() throws Exception {
	HTable t = new HTable(TEST_UTIL.getConfiguration(), Bytes.toString(ArticleTable.NAME));
	Put p = new Put(row1);
	p.add(ArticleTable.ARTICLE_FAMILIY, ArticleTable.ARTICLE_QUALIFIER, Bytes.toBytes("test"));
	t.put(p);
	checkRowAndDelete(t, row1, 1);
	check(new HTable(TEST_UTIL.getConfiguration(), Bytes.toString(ReportTable.NAME)));
    }
    
    private byte[] assembleRowKey(String account, String agent, long ts) {
	String rowKey = account + "/" +  agent + "/" + Long.toString(ts);
	return Bytes.toBytes(rowKey);
    }

    private void check(HTable t) throws IOException {
	Scan scan = new Scan();
	ResultScanner scanner = t.getScanner(scan);
	Iterator<Result> results = scanner.iterator();
	if (results.hasNext()) {
	    fail();
	}
    }
    
    private void checkRowAndDelete(HTable t, byte[] row, int count)
	    throws IOException {
	Get g = new Get(row);
	Result r = t.get(g);
	assertEquals(count, r.size());
	Delete d = new Delete(row);
	t.delete(d);
    }

}
