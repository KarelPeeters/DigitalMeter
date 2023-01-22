class State {
    constructor() {
        this.multi_series = new MultiSeries()

        this.plot_style = new PlotStyle(
            document.getElementById("radio_split"),
            document.getElementById("radio_total"),
            document.getElementById("check_include_zero"),
            () => this.on_plot_style_changed(),
        )

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

    on_plot_style_changed() {
        update_plots(this.multi_series, this.plot_style)
    }
}

let state;
window.addEventListener("DOMContentLoaded", () => {
    console.log("Creating state");
    state = new State();
});

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
