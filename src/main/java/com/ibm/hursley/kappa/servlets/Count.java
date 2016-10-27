package com.ibm.hursley.kappa.servlets;


import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.ibm.hursley.kappa.kafka.KappaQueries;
import com.ibm.hursley.kappa.kafka.Producer;

@Path("/count")
public class Count {
	
	private KappaQueries kappaQueries = null;
	private Producer producer = null;
	private final Logger logger = Logger.getLogger(Count.class);
	
	public Count(){
		kappaQueries = new KappaQueries();
		producer = new Producer();
	}
	
	
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	public String incrementCount() {
		producer.addMessage("Count Request");
		return "OK";
	}
	
	// This method is called if TEXT_PLAIN is request
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String sayPlainTextHello() {
		kappaQueries.runQuery("count all");
		
		return "RUNNING";
		
	}


	@Override
	protected void finalize() throws Throwable {
		kappaQueries.shutdown();
		super.finalize();
	}

}
