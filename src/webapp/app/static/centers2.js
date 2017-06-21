var trace1 = {
  // x: [110.99994262, 111.00023554, 110.99962277, 110.52239454, 109.9999985],
  // y: [13.99999538, 14.00000168, 13.99999132, 12.99999743, 13.99999445],
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

var data = [ trace1 ];

var layout = { 
  // xaxis: {
  //   range: [ 109, 111.2 ] 
  // },
  // yaxis: {
  //   range: [12.00, 14.2]
  // },
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
  //get new data
  (function(){
    $.getJSON('/_realtimecenter', function(data) {
      console.log(data);

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
              marker: { size: 12 }
            };
          console.log(trace1);

          var data = [ trace1 ];

          var layout = { 
                    // xaxis: {
                    //   range: [ 108, 113 ] 
                    // },
                    // yaxis: {
                    //   range: [11.00, 15.2]
                    // },
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
