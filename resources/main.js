class State {
    constructor() {
        this.multi_series = new MultiSeries()

        this.plot_style = "split"
        document.getElementById("radio_split").addEventListener("change", e => this.on_plot_mode_changed(e))
        document.getElementById("radio_total").addEventListener("change", e => this.on_plot_mode_changed(e))

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
        let should_update = on_message(this.multi_series, message.data)

        if (should_update) {
            update_plots(this.multi_series, this.plot_style)
        }
    }

    on_plot_mode_changed(e) {
        let old_value = this.plot_style;
        this.plot_style = e.target.value

        console.log("Style changed to", this, this.plot_style)

        if (this.plot_style !== old_value) {
            update_plots(this.multi_series, this.plot_style)
        }
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
        this.bucket_size = 0

        this.timestamps = []
        this.all_values = []
        this.data_revision = 0

        this.last_timestamp_int = 0
        this.last_timestamp_date = new Date(0);
    }

    push_update(series_data) {
        let timestamps = this.timestamps;
        let all_values = this.all_values;

        this.window_size = series_data["window_size"];
        this.bucket_size = series_data["bucket_size"];

        // append data to state
        for (let i = 0; i < series_data["timestamps"].length; i++) {
            let ts_int = series_data["timestamps"][i];

            // add padding values if necessary
            if (this.last_timestamp_int !== 0) {
                for (let j = this.last_timestamp_int + this.bucket_size; j < ts_int; j += this.bucket_size) {
                    timestamps.push(new Date(j * 1000));
                    for (let values of Object.values(all_values)) {
                        values.push(NaN);
                    }
                }
            }

            // add the real values
            let ts_date = new Date(ts_int * 1000);
            this.last_timestamp_date = ts_date;
            this.last_timestamp_int = ts_int;
            timestamps.push(ts_date);

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
                return (this.last_timestamp_date - element) < this.window_size * 1000;
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

    plot_obj(plot_style) {
        // data
        let data = []

        if (plot_style === "split") {
            for (const key of Object.keys(this.all_values)) {
                data.push({
                    x: this.timestamps,
                    y: this.all_values[key],
                    name: key,
                    mode: "lines",
                })
            }
        } else if (plot_style === "total") {
            let y_total = new Array(this.timestamps.length).fill(0);

            for (const key of Object.keys(this.all_values)) {
                if (y_total === undefined) {
                    y_total = this.all_values[key].clone()
                } else {
                    for (let i = 0; i < y_total.length; i++) {
                        y_total[i] += this.all_values[key][i]
                    }
                }
            }

            if (y_total !== undefined) {
                data.push({
                    x: this.timestamps,
                    y: y_total,
                    name: "total",
                    mode: "lines",
                })
            }
        } else {
            console.log("Unknown plot_style", plot_style)
        }

        // layout
        this.data_revision += 1;
        let layout = {
            datarevision: this.data_revision,
            margin: {t: 0},
            showlegend: true,
            xaxis: {
                type: "date",
                range: [this.last_timestamp_date - this.window_size * 1000, this.last_timestamp_date]
            },
        };

        // config
        let config = {
            staticPlot: true,
        };

        return {
            data: data,
            layout: layout,
            config: config,
            frames: [],
        };
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
        return true;
    } else {
        console.log("Unknown message type", msg_type)
        return false;
    }
}

function update_plots(multi_series, plot_style) {
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
        let plot_obj = series.plot_obj(plot_style);

        if (first_time) {
            // noinspection JSUnresolvedFunction
            Plotly.newPlot(plot, plot_obj);
        } else {
            // plot_obj.config["transition"] = {duration: 1000}
            Plotly.react(plot, plot_obj);
        }
    }
}