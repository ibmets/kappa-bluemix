//var socket = new WebSocket("ws://localhost:9080/kappa-bluemix/ws/query/1350186768");
var socket = null;
var messagesToProcess = [];

function queryCount(){
  resetWs(function(){
    $('#count_answer').html('processing');
    $.ajax({
      type: "POST",
      url: 'rest/query',
      data: $('#filter').val(),
      dataType: 'text',
      success: function(data){
        if(data){
          socket = new WebSocket(getWsUrl()+data);
          socket.onmessage = function (event) {;
            messagesToProcess.push(event.data);
            updateAnswer();
          }
        }
      }
    });
  });
}



function updateAnswer(){
  $('#count_answer').html(messagesToProcess.shift());
}

function queryTflLocations(){
  resetWs(function(){
    $('#tfl_locations_answer').html('processing');
    $.ajax({
      type: "POST",
      url: 'rest/query/tfl-locations',
      data: $('#tfl_locations_filter').val(),
      dataType: 'text',
      success: function(data){
        if(data){
          socket = new WebSocket(getWsUrl()+data);
          socket.onmessage = function (event) {;
            messagesToProcess.push(event.data);
            updateTflLocationsAnswer();
          }
        }
      }
    });
  });
}

function updateTflLocationsAnswer(){
  $('#tfl_locations_answer').html(messagesToProcess.shift());
}

function getWsUrl(){
  var loc = window.location, new_uri;
  if (loc.protocol === "https:") {
    new_uri = "wss:";
  }
  else {
    new_uri = "ws:";
  }
  new_uri += "//" + loc.host;
  new_uri += loc.pathname + "ws/query/";
  return new_uri;
}

function resetWs(callback){
  if(socket){
    socket.onclose = function(){
      messagesToProcess = [];
      callback();
    };
    socket.close();
  }
  else{
    callback();
  }
}

$(document).ready(function() {
    //console.log( "ready!" );
});
