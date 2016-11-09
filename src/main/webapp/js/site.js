//var socket = new WebSocket("ws://localhost:9080/kappa-bluemix/ws/count/1350186768");
var socket = null;

function queryCount(){
  $('#count_answer').html('processing');
  $.ajax({
    type: "POST",
    url: 'rest/count',
    data: $('#filter').val(),
    dataType: 'text',
    success: function(data){
      if(data){
        var wsUrl
        socket = new WebSocket(getWsUrl()+data);
        socket.onmessage = function (event) {;
          $('#count_answer').html(event.data);
        }
      }
    }
  });
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
  new_uri += loc.pathname + "ws/count/";
  return new_uri;
}

$(document).ready(function() {
    //console.log( "ready!" );
});
