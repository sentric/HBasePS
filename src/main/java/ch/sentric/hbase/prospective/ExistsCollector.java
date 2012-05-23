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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;

/**
 * {@link ExistsCollector} is a concrete subclass of {@link Collector} and is
 * primarily used in the prospective search to memorize whether a single
 * document matches a set of queries.
 */
public class ExistsCollector extends Collector {

    private boolean exists;

    /**
     * Reset the collector to false.
     */
    public void reset() {
	this.exists = false;
    }

    /**
     * Returns true if the collector gathered a matching document, otherwise
     * false.
     * 
     * @return true if a document matched, otherwise false.
     */
    public boolean exists() {
	return this.exists;
    }

    @Override
    public void setScorer(final Scorer scorer) throws IOException {
	this.exists = false;
    }

    @Override
    public void collect(final int doc) throws IOException {
	this.exists = true;
    }

    @Override
    public void setNextReader(final IndexReader reader, final int docBase) throws IOException {

    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
	return true;
    }

}
