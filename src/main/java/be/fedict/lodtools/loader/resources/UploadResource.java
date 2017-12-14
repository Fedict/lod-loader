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
package be.fedict.lodtools.loader.resources;

import be.fedict.lodtools.loader.helpers.FileUtil;
import java.io.File;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.security.PermitAll;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 *
 * @author Bart.Hanssens
 */
@Path("/_upload")
public class UploadResource {
	private final static Map<String,Status> STATUS = new HashMap<>();
	private final FileUtil util;
	
	static {
		STATUS.put(FileUtil.DIR_DONE, Status.OK);
		STATUS.put(FileUtil.DIR_FAILED , Status.INTERNAL_SERVER_ERROR);
		STATUS.put(FileUtil.DIR_PROCESS, Status.ACCEPTED);
	}
	
	@PermitAll
	@POST
	@Path("/load/{repo}/{file}")
	@Consumes("application/zip")
	public Response upload(@PathParam("repo") String repo, 
							@PathParam("file") String name, InputStream is) {
		String p  = util.store(repo, is, name);
		return (p != null) ? Response.accepted().build()
							: Response.serverError().build();
	}
	
	@PermitAll
	@GET
	@Path("/status/{repo}/{file}")
	public Response status(@PathParam("repo") String repo, 
							@PathParam("file") String file) {
		File f = new File(file);
		String root = util.getDir();
		
		for(String dir: STATUS.keySet()) {
			if (FileUtil.getFile(root, repo, dir, f).exists()) {
				return Response.status(STATUS.get(dir)).build();
			}
		}
		return Response.status(Status.NOT_FOUND).build();
	}
	
	/**
	 * Constructor
	 * 
	 * @param util 
	 */
	public UploadResource(FileUtil util) {
		this.util = util;
	}
}
