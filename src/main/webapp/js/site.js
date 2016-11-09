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
        socket = new WebSocket("ws://localhost:9080/kappa-bluemix/ws/count/"+data);
        socket.onmessage = function (event) {;
          $('#count_answer').html(event.data);
        }
      }
    }
  });
}


$(document).ready(function() {
    //console.log( "ready!" );
});
