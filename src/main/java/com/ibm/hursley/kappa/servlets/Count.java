package com.ibm.hursley.kappa.servlets;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.ibm.hursley.kappa.kafka.Producer;

@Path("/count")
public class Count {
	
	Producer producer = null;
	
	public Count(){
		producer = new Producer();
	}
	
	// This method is called if TEXT_PLAIN is request
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String sayPlainTextHello() {
		producer.addMessage("Count Request");
		return "Count servlet";
	}

}
