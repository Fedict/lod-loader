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


import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.rdf4j.repository.Repository;

import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Bart.Hanssens
 */
public class DirProcessor implements Runnable {
	private final static Logger LOG = LoggerFactory.getLogger(DirProcessor.class);

	private final RepositoryManager mgr;
	private final WatchService serv;
	private final Map<WatchKey,Path> keys = new HashMap();
	
	/**
	 * Load file
	 * 
	 * @param file file to load
	 * @return true upon success
	 */
	private boolean loadFile(String repoName, Path file) {
		LOG.info("Loading {} into {}", file, repoName);
		try(RepositoryConnection con = mgr.getRepository(repoName).getConnection()) {
			con.begin();
			con.add(file.toFile(), "", RDFFormat.NTRIPLES);
			con.commit();
			return true;
		} catch (RepositoryException|RDFParseException|IOException ex) {
			LOG.error(ex.getMessage());
			return false;
		}
	}
	
	@Override
	public void run() {
		try {
			LOG.info("Running dir processor");
			WatchKey key = serv.take();
			while (key != null) {
				Path p = keys.get(key);
				if (p == null) {
					continue;
				}
				for(WatchEvent ev: key.pollEvents()) {
					WatchEvent.Kind kind = ev.kind();
					if (kind == StandardWatchEventKinds.OVERFLOW) {
						LOG.error("Overflow");
						continue;
					}
					Path file = p.resolve(((WatchEvent<Path>)ev).context());
					String repoName = p.getFileName().toString();
					loadFile(repoName, file);
				}
				key.reset();
				key = serv.take();
			}
		} catch (InterruptedException ex) {
			LOG.error("Interrupted");
		}
		LOG.info("done watching");
	}
	
	/**
	 * Directory processor
	 * 
	 * @param mgr
	 * @param rootdir
	 * @throws IOException 
	 */
	public DirProcessor(RepositoryManager mgr, String rootdir) throws IOException {
		this.mgr = mgr;
		this.serv = FileSystems.getDefault().newWatchService();
		LOG.info("Getting repo's");
		
		for (Repository repo: mgr.getAllRepositories()) {
			String name = FileUtil.repoName(repo);
			Path p = Paths.get(rootdir, name);
			if (p.toFile().exists() && p.toFile().isDirectory()) {
				WatchKey wk = p.register(serv, StandardWatchEventKinds.ENTRY_CREATE);
				keys.put(wk, p);
				LOG.info("watching {}", p);
			} else {
				LOG.warn("Skipping {}, not a readable directory", name);
			}
		}
	}
}