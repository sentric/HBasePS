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
package ch.sentric.hbase.service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import ch.sentric.hbase.table.AccountTable;

/**
 * 
 */
public class QueryDaoImpl implements QueryDao<String> {
    private final Log LOG = LogFactory.getLog(QueryDaoImpl.class);
    private final ResourceManager rm;

    public QueryDaoImpl(ResourceManager rm) {
	this.rm = rm;
    }

    @Override
    public Map<String, String> getQueries() throws IOException {
	final Map<String, String> queries = new HashMap<String, String>(0);
	HTable table = this.rm.getTable(AccountTable.NAME);

	Scan scan = new Scan();
	ResultScanner scanner = table.getScanner(scan);

	Iterator<Result> results = scanner.iterator();
	int errors = 0;
	while (results.hasNext()) {
	    Result result = results.next();
	    if (!result.isEmpty()) {
		try {
		    String accountName = Bytes.toString(result.getRow());
		    NavigableMap<byte[], NavigableMap<byte[], byte[]>> noVersionMap = result.getNoVersionMap();

		    for( Entry<byte[], NavigableMap<byte[], byte[]>> entry : noVersionMap.entrySet()) {
			
			NavigableMap<byte[], byte[]> agents = entry.getValue();
			for (Entry<byte[], byte[]> agent : agents.entrySet()) {
			    String agentName = Bytes.toString(agent.getKey()); // qualifier
			    String agentQuery = Bytes.toString(agent.getValue()); // value
			    queries.put(accountName + "/" + agentName, agentQuery);
			}
			
		    }

		} catch (Exception e) {
		    errors++;
		}
	    }
	}

	if (errors > 0) {
	    LOG.error(String
		    .format("Encountered %d errors in getUsers", errors));
	}
	this.rm.putTable(table);
	return queries;
    }

}
