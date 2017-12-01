/*
 * Copyright (c) 2017, Bart Hanssens <bart.hanssens@fedict.be>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package be.fedict.lodtools.loader;

import be.fedict.lodtools.loader.auth.DummyUser;
import be.fedict.lodtools.loader.auth.UpdateAuth;
import be.fedict.lodtools.loader.health.RdfStoreHealthCheck;
import be.fedict.lodtools.loader.helpers.FileUtil;
import be.fedict.lodtools.loader.helpers.ManagedProcessor;
import be.fedict.lodtools.loader.helpers.ManagedRepositoryManager;
import be.fedict.lodtools.loader.resources.UploadResource;

import io.dropwizard.Application;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.setup.Environment;
import org.eclipse.rdf4j.repository.Repository;

import org.eclipse.rdf4j.repository.manager.RemoteRepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main Dropwizard web application
 * 
 * @author Bart.Hanssens
 */
public class App extends Application<AppConfig> {
	private final static Logger LOG = LoggerFactory.getLogger(App.class);
	
	@Override
	public String getName() {
		return "lod-loader";
	}
	
	@Override
    public void run(AppConfig config, Environment env) {
		
		// Query and JSONLD frame readers
		//QueryReader qr = new QueryReader(config.getQueryRoot());
		
		// repository
		StorageConfig storage = config.getStorageConfig();
		String endpoint = storage.getSparqlPoint();
		RemoteRepositoryManager mgr = 
				(RemoteRepositoryManager) RepositoryProvider.getRepositoryManager(endpoint);
		if (storage.getUsername() != null) {
			mgr.setUsernameAndPassword(storage.getUsername(), storage.getPassword());
			LOG.info("Using username and pasword for storage");
		}
		mgr.initialize();
		
		// Managed resource
		env.lifecycle().manage(new ManagedRepositoryManager(mgr));	

		// Monitoring
		for (Repository repo: mgr.getAllRepositories()) {
			RdfStoreHealthCheck check = new RdfStoreHealthCheck(repo);
			env.healthChecks().register(repo.toString(), check);
		}
		
		// Loader
		env.lifecycle().manage(new ManagedProcessor(mgr, storage.getProcessRoot()));
		
		// Authentication
		AuthConfig auth = config.getAuthConfig();
		env.jersey().register(new AuthDynamicFeature(
				new BasicCredentialAuthFilter.Builder<DummyUser>()
						.setAuthenticator(
							new UpdateAuth(auth.getUsername(), auth.getPassword()))
						.buildAuthFilter()));
		// Upload page/resource
		env.jersey().register(new UploadResource(
										new FileUtil(storage.getProcessRoot())));
	}
	
	/**
	 * Main 
	 * 
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		new App().run(args);
	}
}
