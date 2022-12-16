class State {
    constructor() {
        this.data_t = []
        this.data_y_all = []

        this.socket = null;
        this.timeout = null;

        this.reset_socket()
    }

    reset_socket() {
        if (this.socket != null) {
            console.log("Closing old socket")
            this.socket.close()
        }

        console.log("Creating new socket")
        this.socket = new WebSocket("ws://" + location.hostname + ":8001");
        this.socket.addEventListener("message", message => this.on_message(message));

        this.reset_timeout()
    }

    reset_timeout() {
        console.log("Resetting timeout");
        if (this.timeout  != null) {
            clearTimeout(this.timeout);
        }
        this.timeout = setTimeout(() => this.on_timeout(), 5*1000);
    }

    on_timeout() {
        console.log("Timeout");
        this.reset_socket()
    }

    on_message(message) {
        console.log("Received message '" + message.data + "'");
        on_message(message.data, this.data_t, this.data_y_all);
    }
}

let state;
window.addEventListener("DOMContentLoaded", () => {
    console.log("Creating state");
    state = new State();
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
