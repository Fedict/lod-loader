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
import java.io.InputStream;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.http.HTTPRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Bart.Hanssens
 */
public class FileUtil {
	private final static Logger LOG = LoggerFactory.getLogger(FileUtil.class);
	
	private final static DateTimeFormatter DF = 
						DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");

	private final String dir;
	
	/**
	 * Store an uploaded file to a zip
	 * 
	 * @param repo RDF repository
	 * @param is input stream from uploaded file
	 * @return local name of the uploaded file
	 */
	public String store(String repo, InputStream is) {
		String stamp = LocalDateTime.now().format(DF);

		Path file = Paths.get(dir, repo, stamp + ".zip");
		Path upload = Paths.get(dir, repo, "upload", stamp + ".zip");
		
		LOG.info("Uploading {}", file);
		try {
			Files.copy(is, upload, StandardCopyOption.REPLACE_EXISTING);
			Files.move(upload, file, StandardCopyOption.ATOMIC_MOVE);
		} catch (IOException ex) {
			file = null;
			LOG.error("Error creating upload file {}", ex.getMessage());
			} try {
				Files.deleteIfExists(upload);
			} catch (IOException ex) {
		}
		return (file != null) ? file.toString() : null;
	}
	
	/**
	 * Get the path of the directory to unzip to
	 * 
	 * @param f
	 * @return 
	 */
	public static File getUnzipDir(File f) {
		String s = f.getPath();
		if (! s.endsWith(".zip")) {
			return null;
		}
		return new File(s.replace(".zip", ""));
	}
	
	/**
	 * Remove unzipped directory and files
	 * 
	 * @param f
	 * @return 
	 */
	public static boolean remove(File f) {
		LOG.info("Removing {}", f);
		
		try {
			Stream<Path> paths = Files.walk(getUnzipDir(f).toPath())
									.sorted(Comparator.reverseOrder());
			for (Path path: paths.toArray(Path[]::new)) {
				Files.delete(path);
			}
		} catch (IOException ex) {
			LOG.error("Error removing {} : {}", f, ex.getMessage());
			return false;
		}
		return true;
	}
	
	/**
	 * Unzip file to directory
	 * 
	 * @param p
	 * @return 
	 */
	public static boolean unzip(File p) {
		LOG.info("Unzipping {}", p);
		
		File d = getUnzipDir(p);
		
		try (ZipFile f = new ZipFile(p)) {
			Files.createDirectory(d.toPath());
			
			for(ZipEntry e: f.stream().toArray(ZipEntry[]::new)) {
				Path u = Paths.get(d.getAbsolutePath(), e.getName());
				try (InputStream is = f.getInputStream(e)) {
					Files.copy(is, u);
				}
			}
		} catch (IOException ex) {
			LOG.error("Error unzipping {} : {}", p, ex.getMessage());
			remove(d);
			return false;
		}
		return true;
	}

	public static String repoName(Repository repo) {
		String name = ((HTTPRepository) repo).getRepositoryURL();
		String[] split = name.split("/");
		return (split.length > 0) ? split[split.length - 1] : "";
	}
	
	/**
	 * 
	 * @param dir 
	 */
	public FileUtil(String dir) {
		this.dir = dir;
	}
}
