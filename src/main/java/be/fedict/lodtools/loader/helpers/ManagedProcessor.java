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
package be.fedict.lodtools.loader.helpers;

import io.dropwizard.lifecycle.Managed;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Bart.Hanssens
 */
public class ManagedProcessor implements Managed {
	private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(ManagedProcessor.class);
	
	private final RepositoryManager mgr;
	private final String repoName;

	private static class DirProcess implements Runnable {
		private final WatchService serv;
		
		public DirProcess(WatchService serv) {
			this.serv = serv;
		}

		@Override
		public void run() {
			try {
				WatchKey key = serv.take();
				while (key != null) {
					for(WatchEvent e: key.pollEvents()) {
						
					}
					key.reset();
					key = serv.take();
				}
			} catch (InterruptedException ex) {
				LOG.error("Interrupted");
			}
		}
	}
	
	@Override
	public void start() throws Exception {
		Path p = Paths.get(repoName);
		WatchService serv = p.getFileSystem().newWatchService();
		p.register(serv, StandardWatchEventKinds.ENTRY_CREATE);
	}

	@Override
	public void stop() throws Exception {
	}

	/**
	 * Load file
	 * 
	 * @param file file to load
	 * @return true upon success
	 */
	private boolean loadFile(Path file) {
		try(RepositoryConnection con = mgr.getRepository(repoName).getConnection()) {
			con.begin();
			con.add(file.toFile(), "", RDFFormat.NTRIPLES);
			con.commit();
		} catch (RepositoryException|RDFParseException|IOException ex) {
			//
		}
	}
	
	public ManagedProcessor(RepositoryManager mgr, String repoName) {
		this.mgr = mgr;
		this.repoName = repoName;
	}
}
