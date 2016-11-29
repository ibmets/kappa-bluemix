package com.ibm.hursley.kappa.bluemix;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Iterator;
import java.util.Properties;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messagehub.samples.env.MessageHubCredentials;
import com.messagehub.samples.env.MessageHubEnvironment;

public class Bluemix {
	
	private static final Logger logger = Logger.getLogger(Bluemix.class); 
    private static String bootstrapServers = null;
    private static String adminApiUrl = null;
    private static String apiKey = null;
    private static boolean initialised = false;
    
    private static Properties producerProperties = null;
    private static Properties consumerProperties = null;
    
    public static final String TOPIC = "kappa-index";
    public static final int PARTITIONS = 5;
    public static final String RETENTION = "2592000000"; //MS MAX
    
    
    private static void init(){
    	if(Bluemix.initialised){
    		return;
    	}
    	
    	// Retrieve kafka-Host, rest-Host and API key from Message Hub
        // VCAP_SERVICES.
        // Set JAAS configuration property.
        if (System.getProperty("java.security.auth.login.config") == null) {
            System.setProperty("java.security.auth.login.config", "");
        }
        
    	// Arguments parsed via VCAP_SERVICES environment variable.
        // Retrieve VCAP json through Bluemix system environment variable
        // "VCAP_SERVICES"
        String vcapServices = System.getenv("VCAP_SERVICES");
        ObjectMapper mapper = new ObjectMapper();

        logger.log(Level.WARN, "VCAP_SERVICES: \n" + vcapServices);

        if (vcapServices != null) {
            try {
                // Parse VCAP_SERVICES into Jackson JsonNode, then map the
                // 'messagehub' entry
                // to an instance of MessageHubEnvironment.
                JsonNode vcapServicesJson = mapper.readValue(vcapServices, JsonNode.class);
                ObjectMapper envMapper = new ObjectMapper();
                String vcapKey = null;
                Iterator<String> it = vcapServicesJson.fieldNames();

                // Find the Message Hub service bound to this application.
                while (it.hasNext() && vcapKey == null) {
                    String potentialKey = it.next();

                    if (potentialKey.startsWith("messagehub")) {
                        logger.log(Level.INFO, "Using the '" + potentialKey + "' key from VCAP_SERVICES.");
                        vcapKey = potentialKey;
                    }
                }

                if (vcapKey == null) {
                    logger.log(Level.ERROR,"Error while parsing VCAP_SERVICES: A Message Hub service instance is not bound to this application.");
                    return;
                }

                MessageHubEnvironment messageHubEnvironment = envMapper.readValue(vcapServicesJson.get(vcapKey).get(0).toString(),MessageHubEnvironment.class);
                MessageHubCredentials credentials = messageHubEnvironment.getCredentials();

                replaceUsernameAndPassword(credentials.getUser(), credentials.getPassword());
                
                if(credentials.getKafkaBrokersSasl() != null && credentials.getKafkaBrokersSasl().length > 0){
                	bootstrapServers = "";
            		for (int i=0; i<credentials.getKafkaBrokersSasl().length; i++){
            			if(i<1){
            				bootstrapServers = credentials.getKafkaBrokersSasl()[i];
            			}
            			else{
            				bootstrapServers = bootstrapServers + "," + credentials.getKafkaBrokersSasl()[i];
            			}
            		}
                }
                
                if(credentials.getKafkaAdminUrl() != null && credentials.getKafkaAdminUrl().length() > 0){
                	Bluemix.adminApiUrl = credentials.getKafkaAdminUrl();
                    logger.log(Level.INFO, "Setting rest admin to " + Bluemix.adminApiUrl);
                }
                
                if(credentials.getApiKey() != null && credentials.getApiKey().length() > 0){
                	Bluemix.apiKey = credentials.getApiKey();
                	logger.log(Level.INFO, "Setting api key url to " + Bluemix.apiKey);
                }
                
            } 
            catch (final Exception e) {
            	e.printStackTrace();
                return;
            }
        } 
        else {
            logger.log(Level.ERROR, "VCAP_SERVICES environment variable is null.");
            return;
        }
        
        Bluemix.initialised = true;
    }
    
    
    /**
     * Adding in credentials for MessageHub auth
     */
    private static void replaceUsernameAndPassword(String username, String password) {
        try {
            File xmlFile = new File(System.getProperty("server.config.dir") + File.separator + "server.xml");
            
            System.out.println("editing server.xml:" + xmlFile.toString());
            
            BufferedReader br = new BufferedReader(new FileReader(xmlFile));
            String newline = System.getProperty("line.separator");
            StringBuffer sb = new StringBuffer("");
            String line = null;

            // read in a line at at time
            while ((line = br.readLine()) != null) {
                if (line.indexOf("#USERNAME") != -1) {
                    logger.log(Level.WARN, "Replacing placeholder username and password");
                    line = line.replaceAll("#USERNAME", username);
                    line = line.replaceAll("#PASSWORD", password);
                }
                sb.append(line).append(newline); // append line to new variable
            }
            br.close();

            // write out file again
            logger.log(Level.WARN, "Writing server.xml back to disk");
            BufferedWriter bw = new BufferedWriter(new FileWriter(xmlFile));
            bw.write(sb.toString());
            bw.close();
        } catch (IOException e) {
            logger.log(Level.ERROR, "Couldnt edit server.xml: " + e.getMessage());
        }
    }
    
    
	/**
     * Retrieve client configuration information, using a properties file, for connecting to secure Kafka.
     * 
     * @param broker
     *            {String} A string representing a list of brokers the producer can contact.
     * @param apiKey
     *            {String} The API key of the Bluemix Message Hub service.
     * @param isProducer
     *            {Boolean} Flag used to determine whether or not the configuration is for a producer.
     * @return {Properties} A properties object which stores the client configuration info.
     */
    private static Properties getClientConfiguration(boolean isProducer) {
    	init();
    	
        Properties props = new Properties();
        InputStream propsStream;

        if (isProducer) {
            propsStream = Bluemix.class.getResourceAsStream("/resources/producer.properties");
        } 
        else {
            propsStream = Bluemix.class.getResourceAsStream("/resources/consumer.properties");
        }

        try {
            logger.log(Level.WARN, "Reading properties file from: " + propsStream.toString());
            //propsStream = new FileInputStream(fileName);
            props.load(propsStream);
            propsStream.close();
        } 
        catch (IOException e) {
            logger.log(Level.ERROR, e);
            return props;
        }

        props.put("bootstrap.servers", bootstrapServers);

        if (!props.containsKey("ssl.truststore.location") || props.getProperty("ssl.truststore.location").length() == 0) {
            props.put("ssl.truststore.location", "/home/vcap/app/.java/jre/lib/security/cacerts");
        }

        return props;
    }
    
    
    public static Properties getProducerConfiguration(){
    	if(producerProperties == null){
    		producerProperties = getClientConfiguration(true);
    	}
    	return producerProperties;
    }
    
    
    public static Properties getConsumerConfiguration(){
    	if(consumerProperties == null){
    		consumerProperties = getClientConfiguration(false);
    	}
    	return consumerProperties;
    }
    
    
    public static void runInitialSetup(){
  
    	URL url = null;
    	int responseCode = 0;
		try {
			url = new URL(Bluemix.adminApiUrl);
			url = new URL(url.getProtocol(), url.getHost(), url.getPort(), "/admin/topics", null);
			
			HttpsURLConnection connection = null;
	        
	        // Create secure connection to the REST URL.
            SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
            sslContext.init(null, null, null);

            connection = (HttpsURLConnection) url.openConnection();
            connection.setSSLSocketFactory(sslContext.getSocketFactory());
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");

            // Apply headers, in this case, the API key and Kafka content type.
            connection.setRequestProperty("X-Auth-Token",Bluemix.apiKey);
            connection.setRequestProperty("Content-Type", "application/json");

            // Send the request, writing the body data
            // to the output stream.
            JSONObject topicOptions = new JSONObject();
            JSONObject topicOptionsConfig = new JSONObject();
            topicOptions.put("name", Bluemix.TOPIC);
            topicOptions.put("partitions", Bluemix.PARTITIONS);
            topicOptions.put("configs", topicOptionsConfig);
            topicOptionsConfig.put("retentionMs", Bluemix.RETENTION); // MAX
            
            DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
            wr.writeBytes(topicOptions.toString());
            wr.close();

            responseCode = connection.getResponseCode();
            InputStream is = connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            StringBuilder response = new StringBuilder();
            String line;

            while ((line = rd.readLine()) != null) {
                response.append(line);
                response.append('\r');
            }

            rd.close();
            
            logger.log(Level.INFO, "Creating topic, response: " + responseCode + " " +responseCode);
		} 
		catch (Exception e) {
			if(responseCode == 422){
				logger.log(Level.INFO, "Topic already exists, nothing to do.");
			}
			else{
				logger.log(Level.ERROR, "Error creating topic: " + e.toString());
			}
		}
    	
    }
    
	
}
