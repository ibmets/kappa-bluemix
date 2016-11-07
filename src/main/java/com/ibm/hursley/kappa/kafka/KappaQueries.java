package com.ibm.hursley.kappa.kafka;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ibm.hursley.kappa.bluemix.Bluemix;
import com.ibm.hursley.kappa.queries.CountRecords;

public class KappaQueries {
	
	private final Logger logger = Logger.getLogger(KappaQueries.class);
	private static Hashtable<String, KappaQuery> queries = null;
	private static String clientId = null;
	
	
	public KappaQueries(){
		this.init();
	}
	
	
	private void init(){
		if(KappaQueries.queries == null){
			this.initQueries();
		}
		if(KappaQueries.clientId == null){
			this.createClientId();
		}
	}
	
	
	private synchronized void initQueries(){
		if(KappaQueries.queries == null){
			logger.log(Level.INFO, "cretaing hashtable to store queries");
			KappaQueries.queries = new Hashtable<>();
		}
		
	}
	
	private synchronized void createClientId(){
		if(KappaQueries.clientId == null){
			logger.log(Level.INFO, "creatig new client id for consumer");
			SecureRandom random = new SecureRandom();
			KappaQueries.clientId = new BigInteger(130, random).toString(32);
		}
	}
	
	public static String getClientId(){
		return KappaQueries.clientId;
	}
	
	public synchronized KappaQuery runQuery(String query, String filter){
		String hash = KappaQuery.generateHash(query, filter);
		
		if(KappaQueries.queries.containsKey(hash)){
			logger.log(Level.INFO, "Query: " + query + " already exists in hashtable");
			final KappaQuery kappaQuery = KappaQueries.queries.get(hash);
			
			// might not be running yet (if polling for result before started)
			if(!kappaQuery.isRunning()){
				logger.log(Level.INFO, " but it has not yet been started");
				kappaQuery.setQuery(query, filter);
				Thread startupThread = new Thread() {
			        public void run() {
			        	kappaQuery.initKafka();
			            kappaQuery.start();
			        }
			    };
			    startupThread.start();
			}
			
			return kappaQuery;
		}
		else{
			final KappaQuery kappaQuery = getQueryHandler(query, filter);
			logger.log(Level.INFO, "Query: " + query + " does not exist in hashtable");
			KappaQueries.queries.put(hash, kappaQuery);
			Thread startupThread = new Thread() {
		        public void run() {
		        	kappaQuery.initKafka();
		            kappaQuery.start();
		        }
		    };
		    startupThread.start();
		    return kappaQuery;
		}

	}
	
	public KappaQuery getQuery(String hash){
		if(KappaQueries.queries.containsKey(hash)){
			return KappaQueries.queries.get(hash);
		}
		else{
			logger.log(Level.ERROR, "A query for hash " + hash + " has not been created");
			return null;
		}
	}
	
	
	public void shutdown(){
		Iterator<String> i = KappaQueries.queries.keySet().iterator();
		while(i.hasNext()){
			String key = i.next();
			KappaQuery kappaQuery = KappaQueries.queries.get(key);
			kappaQuery.shutdown();
		}
	}
	
	private KappaQuery getQueryHandler(String query, String filter){
		
		if(query != null && query.equalsIgnoreCase("count")){
			final KappaQuery kappaQuery = new CountRecords(query, filter);
			return kappaQuery;
		}
		else{
			final KappaQuery kappaQuery = new KappaQuery(query, filter);
			return kappaQuery;
		}
		
	}
	
}
