var app = require('http').createServer(handler),
  fs = require('fs'),
  io = require('socket.io').listen(app),
  ZookeeperWatcher = require('zookeeper-watcher'),
  zk_client = require('node-zookeeper-client').createClient('localhost:2181'),
  zk = new ZookeeperWatcher({
    hosts: ['localhost:2181'],
    root: '/'
  }),
  stringify = require('json-stringify-safe');

// creating the server ( localhost:8000 ) 
app.listen(8000);

// on server started we can load our client.html page
function handler(req, res) {
  fs.readFile(__dirname + '/client.html', function(err, data) {
    if (err) {
      console.log(err);
      res.writeHead(500);
      return res.end('Error loading client.html');
    }
    res.writeHead(200);
    res.end(data);
  });
}

// creating a new websocket to keep the content updated without any AJAX request
io.sockets.on('connection', function(socket) {
  console.log('Socket connected');
  console.log(__dirname);

  zk_client.connect();
  // wait for client to be connected
  zk_client.once('connected', function () {
    console.log('Connected to the server');
    listChildren(zk_client, '/completed', socket);
  });
});

function listChildren(client, path, socket) {
  client.getChildren(
    path,
    function (event) {
      console.log('Got event: %s', event);
    },
    function (err, children, stat) {
      'use strict';
      var funcs = [];
      if (err) {
        console.log('Failed to get children of %s due to: %s.', path, err);
        return;
      }
      console.log('Children of %s are: %j.', path, children);
      socket.volatile.emit('children', children);
      for (var i = 0; i < children.length; i++) {
        funcs[i] = function (index) {
          return function () {
            zk.watch('/completed/' + children[index], function (err, value, zstat) {
              var child = children[index];
              console.log(child);
              var json = {
                'name': child,
                'value': value.toString('utf-8')
              };
              socket.volatile.emit('notification', json);
            });
          };
        }(i);
      }

      for (var j = 0; j < children.length; j++) {
        funcs[j]();
      }
    }
  );
}
