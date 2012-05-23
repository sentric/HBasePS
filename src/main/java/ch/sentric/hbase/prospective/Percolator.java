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
package ch.sentric.hbase.prospective;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.input.CharSequenceReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;



/**
 * It uses {@code CustomMemoryIndex} a fast RAM-only index to test whether a
 * single document matches a set of queries.
 * 
 * @param <T>
 *            the generic ID type
 * 
 */
public class Percolator<T> {
    public static final Log LOG = LogFactory.getLog(Percolator.class);
    private final Analyzer analyzer;

    /**
     * Create a {@code Percolator} instance with the given {@code Analyzer}.
     * 
     * @param analyzer
     *            to find terms in the text and queries
     * @param queries
     *            the parsed queries
     */

    public Percolator(final Analyzer analyzer) {
	this.analyzer = analyzer;
	if (LOG.isDebugEnabled()) {
	    LOG.debug("Percolator initialized.");
	}
    }

     /**
     * Tries to find a set of queries that match the given document.
     * 
     * @param doc
     *            the Lucene document
     * @return the matching queries
     * @throws IOException
     *             if an I/O error occurs
     */
    public Response<T> percolate(final Document doc, final Map<T, Query> queries) throws IOException {
	// first, parse the source doc into a MemoryIndex
	final MemoryIndex memoryIndex = new MemoryIndex();

	for (final Fieldable field : doc.getFields()) {
	    if (!field.isIndexed()) {
		continue;
	    }

	    final TokenStream tokenStream = field.tokenStreamValue();
	    if (tokenStream != null) {
		memoryIndex.addField(field.name(), tokenStream, field.getBoost());
	    } else {
		final Reader reader = field.readerValue();
		if (reader != null) {
		    memoryIndex.addField(field.name(), analyzer.reusableTokenStream(field.name(), reader), field.getBoost());
		} else {
		    final String value = field.stringValue();
		    if (value != null) {
			memoryIndex.addField(field.name(), analyzer.reusableTokenStream(field.name(), new CharSequenceReader(value)), field.getBoost());
		    }
		}
	    }
	}

	// do the search
	final IndexSearcher searcher = memoryIndex.createSearcher();
	final Map<T, Query> matches = new HashMap<T, Query>(0);

	if (queries != null && !queries.isEmpty()) {
	    final ExistsCollector collector = new ExistsCollector();
	    for (final Map.Entry<T, Query> entry : queries.entrySet()) {
		collector.reset();
		searcher.search(entry.getValue(), collector);
		if (collector.exists()) {
		    matches.put(entry.getKey(), entry.getValue());
		}
	    }
	}

	return new Response<T>(matches);
    }
}
