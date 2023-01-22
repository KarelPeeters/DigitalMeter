class State {
    constructor() {
        this.plot = document.getElementById("plot")
        this.plot_style = new PlotStyle(
            document.getElementById("radio_split"),
            document.getElementById("radio_total"),
            document.getElementById("check_include_zero"),
            () => this.update_plot(),
        )
        this.series = new Series()
        this.first_plot_update = true

        this.form = document.getElementById("form")
        this.input_start = document.getElementById("start")
        this.input_end = document.getElementById("end")
        this.input_resolution = document.getElementById("resolution")

        // set reasonable initial values: last day, one sample per minute
        this.tz_offset_ms = (new Date()).getTimezoneOffset() * (60 * 1000)
        this.input_start.value = (new Date(Date.now() - this.tz_offset_ms - 24 * 3600 * 1000)).toISOString().slice(0, -8)
        this.input_end.value = (new Date(Date.now() - this.tz_offset_ms)).toISOString().slice(0, -8)
        this.input_resolution.value = 60

        // more event listeners
        document.getElementById("button_preview").addEventListener("click", (_) => this.preview());
        document.getElementById("button_download").addEventListener("click", (_) => this.download());
    }

    preview() {
        console.log("Preview")
        let url = this.download_url_for_inputs("json")
        if (url === undefined) return

        window
            .fetch(url)
            .then((response) => response.json())
            .then((data) => {
                console.log("Preview data received")
                this.series = new Series()
                this.series.push_update(data, false);
                this.update_plot()
            })
    }

    download() {
        console.log("Download")
        let url = this.download_url_for_inputs("csv")
        if (url === undefined) return

        // from https://stackoverflow.com/a/49917066/5517612
        const a = document.createElement("a")
        a.href = url;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    }

    download_url_for_inputs(type) {
        if (!this.form.reportValidity()) {
            return undefined;
        }
        let start = (this.input_start.valueAsDate.getTime() + this.tz_offset_ms) / 1000.
        let end = (this.input_end.valueAsDate.getTime() + this.tz_offset_ms) / 1000.

        let params = new URLSearchParams({
            "bucket_size": this.input_resolution.value,
            "oldest": start,
            "newest": end,
        })

        return "../download/samples_custom." + type + "?" + params
    }

    update_plot() {
        let plot_obj = this.series.plot_obj(this.plot_style);
        if (this.first_plot_update) {
            // noinspection JSUnresolvedFunction
            Plotly.newPlot(this.plot, plot_obj);
        } else {
            Plotly.react(this.plot, plot_obj);
        }
    }
}

// noinspection JSUnusedGlobalSymbols
const state = new State()
