package com.ibm.hursley.kappa.servlets;


import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ibm.hursley.kappa.kafka.KappaQueries;
import com.ibm.hursley.kappa.kafka.KappaQuery;

@Path("/query")
public class Query {
	
	private KappaQueries kappaQueries = null;

	private final Logger logger = Logger.getLogger(Query.class);
	
	public Query(){
		kappaQueries = new KappaQueries();
	}
	
	
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	public String addQuery(String body) {
		KappaQuery kappaQuery = null;
		if(body != null && body.length() > 0){
			logger.log(Level.INFO, "Received filtered default query filter: "+ body);
			kappaQuery = kappaQueries.runQuery("count", body);
		}
		else{
			logger.log(Level.INFO, "Received unfiltered count query");
			kappaQuery = kappaQueries.runQuery("count", null);
		}
		return kappaQuery.getHash();
	}
	
	
	@POST
	@Path("/{type}")
	@Produces(MediaType.TEXT_PLAIN)
	public String addQueryWithType(@PathParam("type") String queryType, String body) {
		KappaQuery kappaQuery = null;
		if(queryType !=null && queryType.length() > 0 && body != null && body.length() > 0){
			logger.log(Level.INFO, "Received filtered query: " + queryType + " filter:"+ body);
			kappaQuery = kappaQueries.runQuery(queryType, body);
		}
		else if(queryType !=null && queryType.length() > 0){
			kappaQuery = kappaQueries.runQuery(queryType, null);
		}
		else{
			logger.log(Level.INFO, "Received unfiltered count query");
			kappaQuery = kappaQueries.runQuery("count", null);
		}
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
