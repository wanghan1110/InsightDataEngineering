var trace1 = {
  x: [center_json["key-0"][0],center_json["key-1"][0],center_json["key-2"][0],center_json["key-3"][0],center_json["key-4"][0]],
  y: [center_json["key-0"][1],center_json["key-1"][1],center_json["key-2"][1],center_json["key-3"][1],center_json["key-4"][1]],
  mode: 'markers+text',
  type: 'scatter',
  name: 'Drone location',
  text: ['Drone-1', 'Drone-2', 'Drone-3', 'Drone-4', 'Drone-5'],
  textposition: 'top center',
  textfont: {
    family:  'Raleway, sans-serif'
  },
  marker: { size: 12 }
};


var x_points = [];
  for (i = 1; i < 121; i++) {
    x_points.push(people_json[i][0]);
  };
  var y_points = [];
  for (i = 1; i < 121; i++) {
    y_points.push(people_json[i][1]);
  };

var trace2 = {          
  x: x_points,
  y: y_points,
  mode: 'markers',
  type: 'scatter',
  name: 'People location',
  marker: { size: 12 }
};

var data = [ trace1, trace2 ];

var layout = {
  legend: {
    y: 0.5,
    yref: 'paper',
    font: {
      family: 'Arial, sans-serif',
      size: 20,
      color: 'grey',
    }
  },
  title:'Drone locations'
};

var plot = Plotly.newPlot('myDiv', data, layout);

setInterval(
  (function(){
    $.getJSON('/_realtimecenter', function(data_people) {
          var data = data_people[0]
          var people = data_people[1]
          var trace1 = {
              x: [data["key-0"][0],data["key-1"][0],data["key-2"][0],data["key-3"][0],data["key-4"][0]],
              y: [data["key-0"][1],data["key-1"][1],data["key-2"][1],data["key-3"][1],data["key-4"][1]],
              mode: 'markers+text',
              type: 'scatter',
              name: 'Drone location',
              text: ['Drone-1', 'Drone-2', 'Drone-3', 'Drone-4', 'Drone-5'],
              textposition: 'top center',
              textfont: {
                family:  'Raleway, sans-serif'
              },
              marker: { size: 16 }
            };

          console.log(trace1)

          var x_pts = [];
              for (i = 1; i < 121; i++) {
                x_pts.push(people[i][0]);
              };
          var y_pts = [];
              for (i = 1; i < 121; i++) {
                y_pts.push(people[i][1]);
              };

          var trace2 = {       
              x: x_pts,
              y: y_pts,
              mode: 'markers',
              type: 'scatter',
              name: 'People location',
              marker: { size: 12 }
            };

          console.log(trace2);

          var data = [ trace1,trace2 ];
          var layout = { 
                    legend: {
                      y: 0.5,
                      yref: 'paper',
                      font: {
                        family: 'Arial, sans-serif',
                        size: 20,
                        color: 'grey',
                      }
                    },
                    title:'Drone locations'
                  };
          Plotly.newPlot('myDiv',data,layout);
      });
  

  }), 2000);
