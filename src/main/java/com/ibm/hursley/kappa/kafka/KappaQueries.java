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
		KappaQueries.queries = new Hashtable<>();
	}
	
	private synchronized void createClientId(){
		SecureRandom random = new SecureRandom();
		KappaQueries.clientId = new BigInteger(130, random).toString(32);
	}
	
	public static String getClientId(){
		return KappaQueries.clientId;
	}
	
	public synchronized KappaQuery runQuery(String query){
		String hash = KappaQuery.generateHash(query);
		
		if(KappaQueries.queries.containsKey(hash)){
			logger.log(Level.INFO, "Query: " + query + " already exists in hashtable");
			return KappaQueries.queries.get(hash);
		}
		else{
			final KappaQuery kappaQuery = new KappaQuery(query);
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
	
	
	public void shutdown(){
		Iterator<String> i = KappaQueries.queries.keySet().iterator();
		while(i.hasNext()){
			String key = i.next();
			KappaQuery kappaQuery = KappaQueries.queries.get(key);
			kappaQuery.shutdown();
		}
	}
	
}
