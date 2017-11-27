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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

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
	 * @param is
	 * @return
	 */
	public String store(InputStream is) {
		String stamp = LocalDate.now().format(DF);

		Path upload = Paths.get(dir, stamp + ".upload");
		Path file = Paths.get(dir, stamp + ".zip");
		
		LOG.info("Uploading {}", file);
		
		try {
			Files.copy(is, upload);
			Files.move(upload, file);
		} catch (IOException ex) {
			LOG.error("Error creating or moving file, {}", ex.getMessage());
		} finally {
			try {
				Files.deleteIfExists(upload);
			} catch (IOException ex) {
			}
		}
		return Files.exists(file) ? file.toString() : null;
	}
	
	/**
	 * Get the path of the directory to unzip to
	 * 
	 * @param p
	 * @return 
	 */
	private static Path getUnzipDir(Path p) {
		String s = p.getFileName().toString();
		if (! s.endsWith("zip")) {
			return null;
		}
		return Paths.get(s.replace(".zip", ""));
	}
	
	/**
	 * Remove unzipped directory and files
	 * 
	 * @param p
	 * @return 
	 */
	public static boolean remove(Path p) {
		LOG.info("Removing {}", p);
		
		try {
			Files.walk(getUnzipDir(p)).map(Path::toFile)
									.sorted(Comparator.reverseOrder())
									.forEach(Files::delete);
		} catch (IOException ex) {
			LOG.error("Error removing {} : {}", p, ex.getMessage());
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
	public static boolean unzip(Path p) {
		LOG.info("Unzipping {}", p);
		
		Path d = getUnzipDir(p);
		
		try (ZipFile f = new ZipFile(p.toFile())) {
			for(ZipEntry e: (ZipEntry[]) f.stream().toArray()) {
				Path u = Paths.get(d.toFile().getAbsolutePath(), e.getName());
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

	/**
	 * 
	 * @param dir 
	 */
	public FileUtil(String dir) {
		this.dir = dir;
	}
}
