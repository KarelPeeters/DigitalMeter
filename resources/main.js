class State {
    constructor() {
        this.multi_series = new MultiSeries()

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
        on_message(this.multi_series, message.data)
    }
}

let state;
window.addEventListener("DOMContentLoaded", () => {
    console.log("Creating state");
    state = new State();
});

class Series {
    constructor() {
        this.window_size = 0
        this.timestamps = []
        this.all_values = []
        this.data_revision = 0
    }

    push_update(series_data) {
        let timestamps = this.timestamps;
        let all_values = this.all_values;
        this.window_size = series_data["window_size"];

        // append data to state
        for (let i = 0; i < series_data["timestamps"].length; i++) {

            let ts = new Date(series_data["timestamps"][i] * 1000);
            timestamps.push(ts);

            // remember latest timestamp
            this.last_timestamp = ts;

            for (const [key, values] of Object.entries(series_data["values"])) {
                if (!(key in all_values)) {
                    all_values[key] = [];
                }
                all_values[key].push(values[i]);
            }
        }

        // remove old data
        if (timestamps.length >= 1) {
            let index = timestamps.findIndex(element => {
                return (this.last_timestamp - element) < this.window_size * 1000;
            });

            // check if we actually found an index (!= -1) and that we leave one value (>= 1)
            if (index >= 1) {
                index -= 1;
                timestamps.splice(0, index);
                for (const key of Object.keys(all_values)) {
                    all_values[key].splice(0, index);
                }
            }
        }

        for (const array of Object.values(all_values)) {
            console.assert(timestamps.length === array.length);
        }
    }

    plot_obj() {
        let obj = {
            data: [],
            layout: {},
            config: {},
            frames: [],
        }

        // add data lines
        for (const key of Object.keys(this.all_values)) {
            obj.data.push({
                x: this.timestamps,
                y: this.all_values[key],
                name: key,
                mode: "lines",
            })
        }

        obj.layout = {
            datarevision: this.data_revision,
            margin: {t: 0},
            xaxis: {type: "date", range: [this.last_timestamp - this.window_size * 1000, this.last_timestamp]},
        };

        return obj;
    }
}

class MultiSeries {
    constructor() {
        this.all_series = {}
    }

    clear() {
        this.all_series = {}
    }

    push_update(all_series_data) {
        for (const [key, series_data] of Object.entries(all_series_data)) {
            if (!(key in this.all_series)) {
                this.all_series[key] = new Series();
            }
            this.all_series[key].push_update(series_data)
        }
    }
}

function on_message(multi_series, msg_str) {
    let msg_json = JSON.parse(msg_str);
    let msg_type = msg_json["type"];

    if (msg_type === "initial" || msg_type === "update") {
        if (msg_type === "initial") {
            multi_series.clear();
        }

        // store the data
        multi_series.push_update(msg_json["series"]);

        // plot the data
        for (const [key, series] of Object.entries(multi_series.all_series)) {
            let plot_id = "plot_" + key;

            // create the plot if necessary
            let plot = document.getElementById(plot_id);
            let first_time = false;
            if (plot === null) {
                plot = document.createElement("div")
                plot.setAttribute("id", plot_id)
                document.getElementById("plots").appendChild(plot)
                first_time = true;
            }

            // update the plot
            let plot_obj = series.plot_obj();

            if (first_time) {
                // noinspection JSUnresolvedFunction
                Plotly.newPlot(plot, plot_obj);
            } else {
                // plot_obj.config["transition"] = {duration: 1000}
                Plotly.react(plot, plot_obj);
            }
        }

    } else {
        console.log("Unknown message type " + msg_type)
    }
}
