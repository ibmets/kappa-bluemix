package com.ibm.hursley.kappa.kafka;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ibm.hursley.kappa.bluemix.Bluemix;

public class KappaQuery extends Thread{
	
	private final Logger logger = Logger.getLogger(KappaQuery.class);
	
	private String query = null;
	private String hash = null;
	private boolean running = true;
	private KafkaConsumer<String, byte[]> kafkaConsumer = null;

	
	public KappaQuery(String query){
		this.query = query;
		this.hash = KappaQuery.generateHash(query);
		this.initKafka();
	}
	
	
	public String getHash(){
		return this.hash;
	}
	
	
	public static String generateHash(String query){
		String hash = null;
		if(query != null){
			hash = query.hashCode() + "";
		}
		return hash;
	}
	
	
	private void initKafka(){
		// create kafka consumer
		Properties consumerProperties = (Properties) Bluemix.getConsumerConfiguration().clone();
		consumerProperties.setProperty("client.id", "kappa-bluemix-"+this.hash);
		logger.log(Level.INFO, "creating new consumer with ID: " + consumerProperties.getProperty("client.id"));
		this.kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		
		// set topics
		ArrayList<String> topicList = new ArrayList<String>();
		topicList.add("search");
		this.kafkaConsumer.subscribe(topicList);
		
		if(consumerProperties.containsKey("enable.auto.commit") && consumerProperties.getProperty("enable.auto.commit").trim().equalsIgnoreCase("true")){
			this.resetStream();
		}
		
	}

		
	public void shutdown(){
		this.running = false;
	}
	
	
	private void resetStream(){
		kafkaConsumer.poll(5000);
	    ArrayList<TopicPartition> topicPartions = new ArrayList<>();
	    topicPartions.add(new TopicPartition("search",0));
	    kafkaConsumer.seekToBeginning(topicPartions);
	}
	
	
	@Override
	public void run() {
		logger.log(Level.INFO, "Running query");
		int messageCount = 0;
		while(running){
			Iterator<ConsumerRecord<String, byte[]>> it = this.kafkaConsumer.poll(5000).iterator();
			while (it.hasNext()) {
				messageCount++;
				ConsumerRecord<String, byte[]> record = it.next();
				String message = new String(record.value(), Charset.forName("UTF-8"));
				logger.log(Level.INFO, "Message: " + message.toString());
			}
		
			logger.log(Level.INFO, "running, kafka count: " + messageCount);
		}
		
		kafkaConsumer.close();
		logger.log(Level.INFO,"shutting down kafka consumer");
		
	}
}
