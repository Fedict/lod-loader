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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.Update;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process files uploaded to a directory
 * 
 * @author Bart.Hanssens
 */
public class DirProcessor implements Runnable {
	private final static Logger LOG = LoggerFactory.getLogger(DirProcessor.class);

	private final ValueFactory F = SimpleValueFactory.getInstance();
	
	private final String dir;
	private final RepositoryManager mgr;
	private final WatchService serv;
	private final Map<WatchKey,Path> keys = new HashMap();
	
	/**
	 * Use CSV file as input for similarly named query file 
	 * (either in the upload zip or as default query for this repository)
	 * 
	 * @param con repository connection
	 * @param file CSV file
	 * @param qryDir default query dir
	 * @throws IOException 
	 */
	private void queryWithFile(RepositoryConnection con, File file, File qryDir) 
															throws IOException {
		LOG.info("Processing CSV file {}", file);

		// Check if there is a query file in the zip, or use the default one
		File qryfile = FileUtil.getQueryFile(file.getParentFile(), file);
		
		if (!qryfile.exists()) {
			LOG.info("No query file {}, trying default one", qryfile);
			qryfile = FileUtil.getQueryFile(qryDir, file);
			if (!qryfile.exists()) {
				LOG.warn("No default query file {}, ignore CSV", qryfile);
				return;
			}
		}
		
		String s = new String(Files.readAllBytes(qryfile.toPath()), StandardCharsets.UTF_8);
		Update upd = con.prepareUpdate(s);
		LOG.info("Query {}", upd);
		
		long size = Files.size(file.toPath());
		if (size == 0) {
			LOG.warn("Zero {}");
		}
		for(String id: Files.readAllLines(file.toPath())) {
			upd.clearBindings();
			upd.setBinding("id", id.startsWith("<") ? F.createIRI(id) 
														: F.createLiteral(id));
			upd.execute();
		}
	}
	
	/**
	 * Load (NTriples) file into RDF Store
	 * 
	 * @param file file to load
	 * @return true upon success
	 */
	private void loadFile(RepositoryConnection con, File file) throws IOException {
		LOG.info("Loading {}", file);
		
		con.add(file, "", RDFFormat.NTRIPLES);
	}
	
	/**
	 * Process contents of a zip file
	 * 
	 * @param repoName
	 * @param tmpfile 
	 */
	private boolean processZip(String repoName, File tmpfile) {
		boolean res = FileUtil.unzip(tmpfile);
		
		if (res == false) {
			LOG.error("Unzip failed");
			return false;
		}
		
		File qryDir = Paths.get(this.dir, repoName, FileUtil.DIR_QUERY).toFile();
		
		File unzipDir = FileUtil.getUnzipDir(tmpfile);
		File[] files = unzipDir.listFiles();
		Arrays.sort(files);
			
		LOG.info("Loading {} files into {}", files.length, repoName);
			
		try(RepositoryConnection con = mgr.getRepository(repoName).getConnection()) {
			if (con == null) {
				LOG.error("No connection to {}", repoName);
				return false;
			}
			
			con.begin();
			for (File f: files) {
				con.isOpen();
				String name = f.getName();
				if (name.endsWith(".nt")) {
					loadFile(con, f);
				} 
				if (name.endsWith(".csv")) {
					queryWithFile(con, f, qryDir);
				}
			}
			con.commit();
			res = true;
			LOG.info("Done loading");
		} catch (RepositoryException|RDFParseException|IOException ex) {
			res = false;
			LOG.error("Failure loading {} : {}", tmpfile.getName(), ex.getMessage());
		}
		return res;
	}
	
	/**
	 * Process a file
	 * 
	 * @param repoName
	 * @param file 
	 */
	private void processFile(String repoName, File file) {
		File tmpfile = FileUtil.getFile(dir, repoName, FileUtil.DIR_PROCESS, file);
		FileUtil.move(file, tmpfile);
			
		if (tmpfile.getName().endsWith(FileUtil.EXT_ZIP)) {
			if (processZip(repoName, tmpfile) == true) {
				File done = FileUtil.getFile(dir, repoName, FileUtil.DIR_DONE, file);
				FileUtil.move(tmpfile, done);
			} else {
				File failed = FileUtil.getFile(dir, repoName, FileUtil.DIR_FAILED, file);
				FileUtil.move(tmpfile, failed);
			}
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
					
					try {
						processFile(repoName, file.toFile());
					} catch (Exception e) {
						LOG.error("Caught exception in dirprocessor");
					}
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
	 * Constructor
	 * 
	 * @param mgr
	 * @param dir
	 * @throws IOException 
	 */
	public DirProcessor(RepositoryManager mgr, String dir) throws IOException {
		this.mgr = mgr;
		this.serv = FileSystems.getDefault().newWatchService();
		this.dir = dir;
		LOG.info("Getting repo's");
		
		for (Repository repo: mgr.getAllRepositories()) {
			String name = FileUtil.repoName(repo);
			Path p = Paths.get(dir, name);
	
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