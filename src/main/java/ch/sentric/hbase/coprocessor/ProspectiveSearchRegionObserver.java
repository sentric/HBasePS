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
package ch.sentric.hbase.coprocessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

import ch.sentric.hbase.prospective.Percolator;
import ch.sentric.hbase.prospective.Response;
import ch.sentric.hbase.service.QueryDao;
import ch.sentric.hbase.service.QueryDaoImpl;
import ch.sentric.hbase.service.ResourceManager;
import ch.sentric.hbase.table.ArticleTable;
import ch.sentric.hbase.table.ReportTable;

/**
 * 
 */
public class ProspectiveSearchRegionObserver extends BaseRegionObserver {
    public static final Log LOG = LogFactory.getLog(HRegion.class);
    private CoreContainer cores;
    private ResourceManager rm;
    private QueryDao<String> queryDao;
    private Percolator<String> percolator;
    
    private Map<String, String> queries = new HashMap<String, String>(0);
    
    private SolrCore getSolrCore(final String name) {
	return cores.getCore(name);
    }
    
    private Document buildDocument(final Put put) {
	
	Document doc = new Document();
	final List<KeyValue> list = put.get(ArticleTable.ARTICLE_FAMILIY, ArticleTable.ARTICLE_QUALIFIER);
	for (KeyValue kv : list) {
	    String content = Bytes.toString(kv.getValue());
	    doc.add(new Field("text", content, Field.Store.YES, Field.Index.ANALYZED));
	}

	return doc;
    }
    

    private void initCore(final String solrHome, String coreName) {
	LOG.debug("init Solr Core");

	if (solrHome != null) {
	    System.setProperty("solr.solr.home", solrHome);
	}

	System.setProperty("solr.velocity.enabled", "false");

	final String factoryProp = System.getProperty("solr.directoryFactory");
	if (factoryProp == null) {
	    System.setProperty("solr.directoryFactory",
		    "solr.RAMDirectoryFactory");
	}

	boolean abortOnConfigurationError = true;
	final CoreContainer.Initializer init = new CoreContainer.Initializer();
	String errorMsg = StringUtils.EMPTY;
	try {
	    this.cores = init.initialize();
	    abortOnConfigurationError = init.isAbortOnConfigurationError();
	    LOG.debug("user.dir=" + System.getProperty("user.dir"));
	} catch (Throwable t) {
	    LOG.error("Could not start Solr. Check solr/home property", t);
	    errorMsg = t.getMessage();
	    SolrConfig.severeErrors.add(t);
	    SolrCore.log(t);
	}

	// Optionally abort if we found a sever error
	if (abortOnConfigurationError && SolrConfig.severeErrors.size() > 0) {
	    LOG.error("Severe errors in solr configuration.");
	    throw new IllegalStateException(errorMsg);
	}

	LOG.debug("init Solr Core done");
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
	LOG.debug("inside postOpen hook");
	String solrHome = e.getEnvironment().getConfiguration().get("solr.home");
	LOG.info("SolrHome from env: " + solrHome);
	
	initCore(solrHome, StringUtils.EMPTY);

	LOG.info("Load resources...");
	try {
	    Configuration conf = e.getEnvironment().getConfiguration();
	    this.rm = ResourceManager.getInstance(conf);
	    this.rm.init();
	    this.queryDao = new QueryDaoImpl(this.rm);
	    this.percolator = new Percolator<String>(getSolrCore(StringUtils.EMPTY).getSchema().getAnalyzer());
	} catch (IOException ex) {
	    LOG.error("Error instantiating resource manager", ex);
	    throw new IllegalStateException(ex);
	}
    }

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> e,
	    boolean abortRequested) {
	LOG.debug("inside preClose hook");
	if (this.cores != null) {
	    this.cores.shutdown();
	    this.cores = null;
	}

	if (this.rm != null) {
	    this.rm.shutdown();
	    this.rm = null;
	}
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e,
	    Put put, WALEdit edit, boolean writeToWAL) throws IOException {

	LOG.debug("inside postPut hook");
	
	if (Bytes.compareTo(ArticleTable.NAME, e.getEnvironment().getRegion().getTableDesc().getName()) == 0) {
	    LOG.debug("Load agents...");
	   
	    // TODO: use caching: see http://ehcache.org/documentation/code-samples
	    this.queries = this.queryDao.getQueries();
	    
	    try {
		final Map<String, Query> parsedQueries = this.parseQueries(queries);
		for (Map.Entry<String, Query> entry : parsedQueries.entrySet()) {
		    LOG.debug(entry.getKey() + " -> " + entry.getValue().toString());
		}
		
		final Document doc = buildDocument(put);
		Response<String> result = this.percolator.percolate(doc, parsedQueries);
		
		if (result != null && result.hasMatch()) {
		    HTable tbl = rm.getTable(ReportTable.NAME);
		    for (Map.Entry<String, Query> entry : result.getMatches().entrySet()) {
			LOG.debug("Matched: " + entry.getKey() + " -> " + entry.getValue());
			// TODO: inform mail component about matching queries
			tbl.put(preparePut(entry.getKey() + "/" + Long.toString(put.getTimeStamp()) , put.getRow()));
		    }
		    tbl.close();
		} else {
		    LOG.debug("No query matched the given document");
		}
		
	    } catch (ParseException ex) {
		LOG.error("Error parsing queries", ex);
	    }
	}
	
    }
    
    private Put preparePut(String rowKey, byte[] value) {
	Put p = new Put(Bytes.toBytes(rowKey));
	p.add(ReportTable.DOC_FAMILIY, ReportTable.DOC_QUALIFIER, value);
	return p;
    }

    private Map<String, Query> parseQueries(final Map<String, String> queries)
	    throws ParseException {
	QParser parser;
	final Map<String, Query> parsedQueries = new HashMap<String, Query>(0);
	final SolrQueryRequest request = new LocalSolrQueryRequest(
		getSolrCore(StringUtils.EMPTY),
		new HashMap<String, String[]>());

	for (final Map.Entry<String, String> entry : queries.entrySet()) {
	    parser = QParser.getParser(entry.getValue(),
		    QParserPlugin.DEFAULT_QTYPE, request);
	    try {
		parsedQueries.put(entry.getKey(), parser.parse());
	    } catch (final ParseException e) {
		LOG.warn("Failed to parse query." + e.getMessage());
	    }
	}
	return parsedQueries;
    }

}
