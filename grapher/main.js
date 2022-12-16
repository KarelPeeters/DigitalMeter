window.addEventListener("DOMContentLoaded", () => {
    // TODO re-open socket if anything goes wrong
    console.log("Opening socket")
    const websocket = new WebSocket("ws://" + location.hostname + ":8001");

    const data_t = [];
    const data_y_all = {};

    console.log("Adding event lister")
    websocket.addEventListener("message", ({data}) => {
        on_message(data, data_t, data_y_all);
    });
});

function on_message(data, data_t, data_y_all) {
    console.log("Received message " + data)
    let data_json = JSON.parse(data);

    // for each sub-message
    let keys = [];
    for (const item of data_json) {
        // set info, the last one will win
        let info = document.getElementById("info")
        info.innerHTML = item["info"];

        // collect plot data
        data_t.push(new Date(item["t"] * 1000))

        let item_y_all = item["y_all"];
        keys = Object.keys(item_y_all);

        for (const [key, y] of Object.entries(item_y_all)) {
            if (!(key in data_y_all)) {
                data_y_all[key] = [];
            }
            data_y_all[key].push(y)
        }
    }

    // remove old data
    const max_time_diff_ms = 60 * 1000;
    let last = Date.now();

    if (data_t.length >= 1) {
        last = data_t[data_t.length - 1];
        let index = data_t.findIndex(element => {
            return (last - element) < max_time_diff_ms;
        });

        if (index >= 0) {
            data_t.splice(0, index);
            for (const key of keys) {
                data_y_all[key].splice(0, index);
            }
        }
    }

    let lines = []
    for (const key of keys) {
        lines.push({
            x: data_t,
            y: data_y_all[key],
            name: key,
        })
    }

    const plot = document.getElementById("plot");
    Plotly.newPlot(plot, lines, {margin: {t: 0}});
    Plotly.relayout(plot, {"xaxis": {"type": "date", range: [last - max_time_diff_ms, last]}})
}
