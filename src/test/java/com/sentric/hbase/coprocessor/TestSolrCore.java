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

import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class TestSolrCore {

    @Test
    public void loadSolrCore() throws Exception {
	System.out.println("init Solr Core");

	System.setProperty("solr.velocity.enabled", "false");
	final String factoryProp = System.getProperty("solr.directoryFactory");
	if (factoryProp == null) {
	    System.setProperty("solr.directoryFactory", "solr.RAMDirectoryFactory");
	}

	boolean abortOnConfigurationError = true;
	final CoreContainer.Initializer init = new CoreContainer.Initializer();
	try {
	    final CoreContainer coreContainer = init.initialize();
	    abortOnConfigurationError = init.isAbortOnConfigurationError();
	    System.out.println("user.dir=" + System.getProperty("user.dir"));
	    Assert.assertNotNull(coreContainer.getCore(""));
	} catch (Throwable t) {
	    System.out.println("Could not start Solr. Check solr/home property: " + t.getMessage());
	    SolrConfig.severeErrors.add(t);
	    SolrCore.log(t);
	}

	// Optionally abort if we found a sever error
	if (abortOnConfigurationError && SolrConfig.severeErrors.size() > 0) {
	    System.out.println("Severe errors in solr configuration.");
	    Assert.fail();
	}
	
    }
}
