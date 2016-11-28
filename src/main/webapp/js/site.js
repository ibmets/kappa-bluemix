var socket = null;
var messagesToProcess = [];


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
            updateCountAnswer();
          }
        }
      }
    });
  });
}


function updateCountAnswer(){
  $('#count_answer').html(messagesToProcess.shift());
}


function querySearch(){
  resetWs(function(){
    $('#search_answer').html('processing');
    $.ajax({
      type: "POST",
      url: 'rest/query/search',
      data: $('#search_filter').val(),
      dataType: 'text',
      success: function(data){
        if(data){
          socket = new WebSocket(getWsUrl()+data);
          socket.onmessage = function (event) {;
            messagesToProcess.push(event.data);
            updateSearchAnswer();
          }
        }
      }
    });
  });
}


function updateSearchAnswer(){
  $('#search_answer').html(messagesToProcess.shift());
}


function queryGroup(){
  resetWs(function(){
    $('#group_answer').html('processing');
    $.ajax({
      type: "POST",
      url: 'rest/query/group',
      data: $('#group_filter').val(),
      dataType: 'text',
      success: function(data){
        if(data){
          socket = new WebSocket(getWsUrl()+data);
          socket.onmessage = function (event) {;
            messagesToProcess.push(event.data);
            updateGroupAnswer();
          }
        }
      }
    });
  });
}


function updateGroupAnswer(){
  $('#group_answer').html(messagesToProcess.shift());
}


$(document).ready(function() {
    console.log( "ready!" );
});
