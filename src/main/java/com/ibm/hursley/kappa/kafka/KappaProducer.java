package com.ibm.hursley.kappa.kafka;

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
	private static KafkaProducer<String, byte[]> kafkaProducer = null;
	
	public KappaProducer(){
		if(KappaProducer.kafkaProducer == null){
			this.init();
		}
	}
	
	private synchronized void init(){
		if(KappaProducer.kafkaProducer == null){
			logger.log(Level.INFO, "initialising Kafka producer");
			Properties producerProperties = (Properties) Bluemix.getProducerConfiguration().clone();
			KappaProducer.kafkaProducer = new KafkaProducer<>(producerProperties);
		}
	}
	
	
	private KafkaProducer<String, byte[]> getProducer(){
		return KappaProducer.kafkaProducer;
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
