var socket = null;
var messagesToProcess = [];

function getWsUrl(){
  var loc = window.location, new_uri;
  if (loc.protocol === 'https:') {
    new_uri = 'wss:';
  }
  else {
    new_uri = 'ws:';
  }
  new_uri += '//' + loc.host;
  new_uri += loc.pathname + 'ws/query/';
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

// from http://stackoverflow.com/questions/4810841/how-can-i-pretty-print-json-using-javascript
function syntaxHighlight(json) {
    json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
        var cls = 'number';
        if (/^"/.test(match)) {
            if (/:$/.test(match)) {
                cls = 'key';
            } else {
                cls = 'string';
            }
        } else if (/true|false/.test(match)) {
            cls = 'boolean';
        } else if (/null/.test(match)) {
            cls = 'null';
        }
        return '<span class="' + cls + '">' + match + '</span>';
    });
}

function runQuery(){
  var queryType = $('#query_type');
  var filterData = $('#filter').text();

  updateFilter();


  console.log('filter: ' + filterData);

  resetWs(function(){
    $('#count_answer').html('processing');
    $.ajax({
      type: 'POST',
      url: 'rest/query/'+queryType.val(),
      data: filterData,
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


function updateFilter(){
  var formattedJSON = JSON.stringify(JSON.parse($('#filter').text()), null, 2);
  $('#filter').html(syntaxHighlight(formattedJSON));
}

function updateAnswer(){
  var formattedJSON = JSON.stringify(JSON.parse(messagesToProcess.shift()), null, 2);
  $('#result').html(syntaxHighlight(formattedJSON));
}


$( document ).ready(function(){
  updateFilter()
});
