package com.ibm.hursley.kappa.servlets;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.ibm.hursley.kappa.kafka.Producer;

@Path("/import")
public class Import {
	
	private final Logger logger = Logger.getLogger(Import.class);
	private Producer producer = null;
	
	public Import() {
		producer = new Producer();
	}

	@POST
	@Produces(MediaType.TEXT_PLAIN)
	public String incrementCount() {
		producer.addMessage("Count Request");
		return "OK";
	}
	
}
