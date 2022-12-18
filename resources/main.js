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
        if (this.timeout != null) {
            clearTimeout(this.timeout);
        }
        this.timeout = setTimeout(() => this.on_timeout(), 5 * 1000);
    }

    on_timeout() {
        console.log("Timeout");
        this.reset_socket()
    }

    on_message(message) {
        this.reset_timeout();
        console.log("Received message '" + message.data + "'");
        on_message(message.data, this.data_t, this.data_y_all);
    }
}

let state;
window.addEventListener("DOMContentLoaded", () => {
    console.log("Creating state");
    state = new State();
});

function on_message(message_data, state_data_t, state_data_y_all) {
    let data_json = JSON.parse(message_data);
    let first_time = state_data_t.length === 0;

    // for each sub-message
    let keys = [];
    for (const item of data_json) {
        // set info, the last one will win
        let info = document.getElementById("info")
        info.innerHTML = item["info"];

        // collect plot data
        state_data_t.push(new Date(item["t"] * 1000))

        let item_y_all = item["y_all"];
        keys = Object.keys(item_y_all);

        for (const [key, y] of Object.entries(item_y_all)) {
            if (!(key in state_data_y_all)) {
                state_data_y_all[key] = [];
            }
            state_data_y_all[key].push(y)
        }
    }

    // remove old data
    const max_time_diff_ms = 60 * 1000;
    let last = Date.now();

    if (state_data_t.length >= 1) {
        last = state_data_t[state_data_t.length - 1];
        let index = state_data_t.findIndex(element => {
            return (last - element) < max_time_diff_ms;
        });

        if (index >= 0) {
            state_data_t.splice(0, index);
            for (const key of keys) {
                state_data_y_all[key].splice(0, index);
            }
        }
    }

    // finally update the plot
    const plot = document.getElementById("plot");

    let data = []
    for (const key of keys) {
        data.push({
            x: state_data_t,
            y: state_data_y_all[key],
            name: key,
            mode: "lines",
        })
    }

    let layout = {
        margin: {t: 0},
        xaxis: {type: "date", range: [last - max_time_diff_ms, last]},
    }

    Plotly.newPlot(plot, data, layout);
}
