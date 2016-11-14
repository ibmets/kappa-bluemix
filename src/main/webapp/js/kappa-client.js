var kappaClient = (function () {

  var sockets = [];

  var startQuery = function(url, data, callback){
    console.log('startQuery: ' + url);
  }

  return {
    startQuery: startQuery,
  }
})();
