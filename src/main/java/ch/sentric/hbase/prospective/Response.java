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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Query;

/**
 * Encapsulates the search result from the in-memory index.
 * 
 * @param <T>
 *            the generic ID type
 * 
 */
public final class Response<T> {

    /**
     * Key: Agent ID, Value: Lucene query.
     */
    private final Map<T, Query> result;

    /**
     * Create a new instance with the given parameter.
     * 
     * @param result
     *            the search result to set
     */
    public Response(final Map<T, Query> result) {
	this.result = result;
    }

    /**
     * Returns the matching agents for a single document.
     * 
     * @return the matching agents
     */
    public Map<T, Query> getMatches() {
	return this.result;
    }

    /**
     * Return the status whether a document matched a search query.
     * 
     * @return true if a match is available, otherwise false
     */
    public boolean hasMatch() {
	return !this.result.isEmpty();
    }

    /**
     * Return the matched agent ID's.
     * 
     * @return the unique ID's
     */
    public List<T> getIds() {
	return new ArrayList<T>(result.keySet());
    }
}
