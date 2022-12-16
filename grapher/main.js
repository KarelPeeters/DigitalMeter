window.addEventListener("DOMContentLoaded", () => {
    const plot = document.getElementById("plot");

    console.log("Creating test plot")
    Plotly.newPlot(plot, [{x: [], y: []}], {margin: {t: 0}});

    console.log("Opening socket")
    const websocket = new WebSocket("ws://" + location.hostname + ":8001");

    console.log("Adding event lister")
    websocket.addEventListener("message", ({data}) => {
        console.log("Received message " + data)
        let data_json = JSON.parse(data);

        const new_data_t = []
        const new_data_y = []

        for (const value of data_json) {
            new_data_t.push(new Date(value["t"] * 1000))
            new_data_y.push(value["y"])
        }

        // Plotly.relayout(plot, view);
        Plotly.extendTraces(plot, {x: [new_data_t], y: [new_data_y]}, [0])
    });
});

