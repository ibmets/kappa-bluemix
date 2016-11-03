package com.ibm.hursley.kappa.servlets;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.ibm.hursley.kappa.kafka.KappaProducer;

@Path("/import")
public class Import {
	
	private final Logger logger = Logger.getLogger(Import.class);
	private KappaProducer producer = null;
	
	public Import() {
		producer = new KappaProducer();
	}

	@POST
	@Produces(MediaType.TEXT_PLAIN)
	public String importJson(String content){
		
		if(content!=null && content.length() > 0){
			try{
				JSONObject jsonObject = new JSONObject(content);
				if(jsonObject != null){
					handleJsonObject(jsonObject);
				}
			}
			catch(Exception e){
				try{
					JSONArray jsonArray = new JSONArray(content);
					if(jsonArray != null){
						for(int i=0; i < jsonArray.length(); i++){
							this.handleJsonObject(jsonArray.getJSONObject(i));
						}
					}
				}
				catch(Exception e2 ){
					e.printStackTrace();
				}
			}	
		}
		
		
		return "OK";
		
	}
	
	
	private void handleJsonObject(JSONObject jsonObject){
		producer.addMessage(jsonObject.toString(0));
		
	}
	
}
