class State {
    constructor() {
        this.data = new DataState()

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
        on_message(message.data, this.data);
    }
}

let state;
window.addEventListener("DOMContentLoaded", () => {
    console.log("Creating state");
    state = new State();
});

class DataState {
    constructor() {
        this.history_window_size = 10

        this.last_timestamp = 0
        this.data_t = []
        this.data_y_all = []
    }

    push_series(series) {
        let data_t = this.data_t;
        let data_y_all = this.data_y_all;

        // append data to state
        for (let i = 0; i < series["timestamps"].length; i++) {

            let ts = new Date(series["timestamps"][i] * 1000);
            data_t.push(ts);

            // remember latest timestamp
            this.last_timestamp = ts;

            for (const [key, values] of Object.entries(series["values"])) {
                if (!(key in data_y_all)) {
                    data_y_all[key] = [];
                }
                data_y_all[key].push(values[i]);
            }
        }

        // remove old data
        if (data_t.length >= 1) {
            let index = data_t.findIndex(element => {
                return (this.last_timestamp - element) < this.history_window_size * 1000;
            });

            if (index >= 0) {
                data_t.splice(0, index);
                for (const key of Object.keys(data_y_all)) {
                    data_y_all[key].splice(0, index);
                }
            }
        }

        for (const array of Object.values(data_y_all)) {
            console.assert(data_t.length === array.length);
        }
    }

    plot_args() {
        let plot_data = []

        for (const key of Object.keys(this.data_y_all)) {
            plot_data.push({
                x: this.data_t,
                y: this.data_y_all[key],
                name: key,
                mode: "lines",
            })
        }

        let plot_layout = {
            margin: {t: 0},
            xaxis: {type: "date", range: [this.last_timestamp - this.history_window_size * 1000, this.last_timestamp]},
        }

        return {plot_data, plot_layout}
    }
}

function on_message(msg_str, data) {
    let msg_json = JSON.parse(msg_str);
    let msg_type = msg_json["type"];


    if (msg_type === "initial" || msg_type === "update") {
        if ("history_window_size" in msg_json) {
            data.history_window_size = msg_json["history_window_size"];
        }

        // store the data
        data.push_series(msg_json["series"]);

        // update the plot
        let {plot_data, plot_layout} = data.plot_args();
        const plot = document.getElementById("plot");
        Plotly.newPlot(plot, plot_data, plot_layout);
    } else {
        console.log("Unknown message type " + msg_type)
    }
}
