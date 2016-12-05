FROM registry.eu-gb.bluemix.net/ibmliberty:webProfile7
#COPY src/main/wlp/local/server.xml /config/
COPY target/bluemix/defaultServer/apps/kappa-bluemix.war /config/dropins/
COPY wlp/usr/servers/defaultServer/server.env /config/
RUN installUtility install defaultServer  --acceptLicense
