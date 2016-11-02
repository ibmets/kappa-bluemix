package com.ibm.hursley.kappa.servlets;


import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.ibm.hursley.kappa.kafka.KappaQueries;
import com.ibm.hursley.kappa.kafka.KappaQuery;

@Path("/count")
public class Count {
	
	private KappaQueries kappaQueries = null;

	private final Logger logger = Logger.getLogger(Count.class);
	
	public Count(){
		kappaQueries = new KappaQueries();
	}
	
	
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	public String addQuery() {
		KappaQuery kappaQuery = kappaQueries.runQuery("count all");
		return kappaQuery.getHash();
	}
	
	
	@Path("/{queryhash}")
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String getQueryResults(@PathParam("queryhash") String queryHash) {
		System.out.println("get data for hash: " + queryHash);
		KappaQuery kappaQuery = kappaQueries.getQuery(queryHash);
		
		if(kappaQuery != null){
			return kappaQuery.getResult();
		}
		else{
			return "0";
		}
	}


	@Override
	protected void finalize() throws Throwable {
		//kappaQueries.shutdown();
		super.finalize();
	}

}
