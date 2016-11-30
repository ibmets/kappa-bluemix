require('dotenv').load();

var cfenv = require("cfenv")
var MessageHub = require('message-hub-rest');
var io = require( 'socket.io-client' );
var express = require('express');

var topicName = 'kappa-index';
var appEnv = cfenv.getAppEnv();
var instance = new MessageHub(appEnv.services);

var timer = false;
var queue = [];

// create a new express server to make Bluemix realise the application is running
var app = express();
app.use(express.static(__dirname + '/public'));
var appEnv = cfenv.getAppEnv();
app.listen(appEnv.port, '0.0.0.0', function() {
  console.log("server starting on " + appEnv.url);
});

function processQueue(){
  var queueSize = queue.length;
  var toSend = [];
  for(var i=0; i < queueSize; i++){
    toSend.push(queue.shift());
  }

  if(toSend && toSend.length > 0){
    var list = new MessageHub.MessageList(toSend);

    instance.produce(topicName, list.messages)
    .then(function() {
      // here for doing stuff when produced
      console.log('sent ' + toSend.length + " messages");
    })
    .fail(function(error) {
      console.log(error);
      throw new Error(error);
    });
  }
}

var socket = io.connect('https://stream.wikimedia.org/rc');

socket.on('connect', function(){
  socket.emit('subscribe', 'en.wikipedia.org');
});

socket.on('change', function ( data ) {
  if(data){
    console.log(data.title)
    queue.push(data);
  }
});

timer = setInterval(processQueue, 5000);
