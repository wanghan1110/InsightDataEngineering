'use strict';

var config = require('./config.json')
  , username = config['user']
  , apiKey = config['apiKey']
  , token = config['token']
  , Plotly = require('../.')(username, apiKey)
  , Signal = require('random-signal')

var data = {
    'x':[]   // empty arrays since we will be streaming our data to into these arrays
  , 'y':[]
  , 'type':'scatter'
  , 'mode':'lines+markers'
  , marker: {
      color: "rgba(31, 119, 180, 0.96)"
  }
  , line: {
      color:"rgba(31, 119, 180, 0.31)"
  }
  , stream: {
      "token": token
    , "maxpoints": 200
  }
}

var graphOptions = {
    "filename": "streamSimpleSensor"
  , "fileopt": "overwrite"
  , "layout": {
      "title": "Drone Coordinates"
  }
  , "world_readable": true
}

Plotly.plot(data, graphOptions, function (err, resp) {
    if (err) return console.log("ERROR", err)

    console.log(resp)

    var plotlystream = Plotly.stream(token, function () {})
    var signalstream = Signal({tdelta: 100}) //


    plotlystream.on("error", function (err) {
        signalstream.destroy()
    })

    // Okay - stream to our plot!
    signalstream.pipe(plotlystream)
});


