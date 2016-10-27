package com.ibm.hursley.kappa.kafka;

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
	
	
	public KappaQueries(){
		this.init();
	}
	
	
	private void init(){
		if(KappaQueries.queries == null){
			this.initQueries();
		}
	}
	
	
	private synchronized void initQueries(){
		KappaQueries.queries = new Hashtable<>();
	}
	
	public void runQuery(String query){
		String hash = KappaQuery.generateHash(query);
		if(KappaQueries.queries.containsKey(hash)){
			logger.log(Level.INFO, "Query: " + query + " already exists in hashtable");
		}
		else{
			logger.log(Level.INFO, "Query: " + query + " does not exist in hashtable");
			KappaQuery kappaQuery = new KappaQuery(query);
			KappaQueries.queries.put(hash, kappaQuery);
			kappaQuery.start();
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
	
	/*
	private void createConsumer(){
		Date now = new Date();
		Properties consumerProperties = Bluemix.getConsumerConfiguration();
		consumerProperties.setProperty("client.id", consumerProperties.getProperty("client.id") + "-" + now.getTime());
		System.out.println("consumer ID " + consumerProperties.getProperty("client.id"));
		KappaQueries.kafkaConsumer = new KafkaConsumer<>(Bluemix.getConsumerConfiguration());
	}
	

	public synchronized KafkaConsumer<String, byte[]> getConsumer(String id){
		if(KappaQueries.kafkaConsumer == null){
			createConsumer();
		}
		return KappaQueries.kafkaConsumer;
	}
	*/
	
}
