package com.ibm.hursley.kappa.kafka;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ibm.hursley.kappa.bluemix.Bluemix;

public class KappaProducer {
	
	private final Logger logger = Logger.getLogger(KappaProducer.class);
	private KafkaProducer<String, byte[]> kafkaProducer = null;
	
	private String clientId = null;
	
	public KappaProducer(){
		this.clientId = KappaProducer.createClientId();
	}
	
	public static String createClientId(){
		SecureRandom random = new SecureRandom();
		return new BigInteger(130, random).toString(32);
	}
	
	public String getClientId(){
		return this.clientId;
	}
	
	public KafkaProducer<String, byte[]> getProducer(){
		if(this.kafkaProducer == null){
			Properties producerProperties = (Properties) Bluemix.getProducerConfiguration().clone();
			producerProperties.setProperty("client.id", this.getClientId());
			this.kafkaProducer = new KafkaProducer<>(producerProperties);
		}	
		return this.kafkaProducer;
	}
	
	
	public void addMessage(String message){
		KafkaProducer<String, byte[]> kafkaProducer = getProducer();
		try{
			kafkaProducer.send(new ProducerRecord<String, byte[]>("search", message.getBytes()),new Callback() {
				@Override
				public void onCompletion(RecordMetadata meta, Exception e) {
					if(meta != null){
						logger.log(Level.INFO, "Added to message hub topic:" +meta.topic() + " offset:" + meta.offset() + " partition:" + meta.partition());
					}
					if(e != null){
						logger.log(Level.ERROR, e.getMessage());
					}
				}
			});
			
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
}
