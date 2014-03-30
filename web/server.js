var app = require('http').createServer(handler),
    io = require('socket.io').listen(app),
    zk_watcher = require('zookeeper-wathcer'),
    zk = new ZookeeperWatcher({
      hosts: ['localhost:2181'],
      root: '/'
    });

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
  console.log(__dirname);
  // wait for client to be connected
  zk.once('connected', function () {
    console.log('Connected to the server');
    zk.watch('/completed/worker1', function (err, value, zstat) {
      console.log(arguments);
    });
  });
  
});
