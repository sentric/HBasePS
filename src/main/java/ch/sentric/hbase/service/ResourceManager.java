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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * This class is implemented as a Singleton, i.e., it is shared across the
 * entire application. There are accessors for all shared services included. <br/>
 * Using the class should be facilitated invoking <u>only</u>
 * <code>getInstance()</code> without any parameters. This will return the
 * globally configured instance.
 */
public class ResourceManager {
    private static final Log LOG = LogFactory.getLog(ResourceManager.class);

    public static final byte[] ONE = new byte[] { 1 };
    public static final byte[] ZERO = new byte[] { 0 };

    private static ResourceManager INSTANCE;
    private final Configuration conf;
    private final HTablePool pool;

    /**
     * Returns the shared instance of this singleton class.
     * 
     * @return The singleton instance.
     * @throws IOException
     *             When creating the remote HBase connection fails.
     */
    public synchronized static ResourceManager getInstance() throws IOException {
	assert (INSTANCE != null);
	return INSTANCE;
    }

    /**
     * Creates a new instance using the provided configuration upon first
     * invocation, otherwise it returns the already existing instance.
     * 
     * @param conf
     *            The HBase configuration to use.
     * @return The new or existing singleton instance.
     * @throws IOException
     *             When creating the remote HBase connection fails.
     */
    public synchronized static ResourceManager getInstance(Configuration conf)
	    throws IOException {
	if (INSTANCE == null) {
	    INSTANCE = new ResourceManager(conf);
	}
	return INSTANCE;
    }

    /**
     * Stops the singleton instance and cleans up the internal reference.
     */
    public synchronized static void stop() {
	if (INSTANCE != null) {
	    INSTANCE = null;
	}
    }

    /**
     * Internal constructor, called by the <code>getInstance()</code> methods.
     * 
     * @param conf
     *            The HBase configuration to use.
     * @throws IOException
     *             When creating the remote HBase connection fails.
     */
    private ResourceManager(Configuration conf) throws IOException {
	this.conf = conf;
	this.pool = new HTablePool(conf, 10);
    }

    /**
     * Delayed initialization of the instance. Should be called once to set up
     * the counters etc.
     * 
     * @throws IOException
     *             When setting up the resources in HBase fails.
     */
    public void init() throws IOException {
    }

    /**
     * Returns the internal <code>HTable</code> pool.
     * 
     * @return The shared table pool.
     */
    public HTablePool getTablePool() {
	return pool;
    }

    /**
     * Returns a single table from the shared table pool. More convenient to use
     * compared to <code>getTablePool()</code>.
     * 
     * @param tableName
     *            The name of the table to retrieve.
     * @return The table reference.
     * @throws IOException
     *             When talking to HBase fails.
     */
    public HTable getTable(byte[] tableName) throws IOException {
	return (HTable) pool.getTable(tableName);
    }

    /**
     * Returns the previously retrieved table to the shared pool. The caller
     * must take care of calling <code>flushTable()</code> if there are any
     * pending mutations.
     * 
     * @param table
     *            The table reference to return to the pool.
     */
    public void putTable(HTable table) throws IOException {
	if (table != null) {
	    table.close();
	}
    }

    /**
     * Returns the currently used configuration.
     * 
     * @return The current configuration.
     */
    public Configuration getConfiguration() {
	return conf;
    }

    public void shutdown() {
	try {
	    this.pool.close();
	} catch (IOException e) {
	    LOG.warn("Could not shutdown the table pool", e);
	}
    }

}