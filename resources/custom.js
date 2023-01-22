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
        this.input_start = document.getElementById("input_start")
        this.input_end = document.getElementById("input_end")
        this.input_resolution = document.getElementById("input_res")
        this.input_format = new RadioGroup("input_format", getCookie("download_format", "csv"))
        this.output_expected_samples = document.getElementById("output_expected_samples")

        this.button_preview = document.getElementById("button_preview");
        this.button_download = document.getElementById("button_download");

        // set reasonable initial values: last day, one sample per minute
        this.tz_offset_ms = (new Date()).getTimezoneOffset() * (60 * 1000)
        this.input_start.value = (new Date(Date.now() - this.tz_offset_ms - 24 * 3600 * 1000)).toISOString().slice(0, -8)
        this.input_end.value = (new Date(Date.now() - this.tz_offset_ms)).toISOString().slice(0, -8)
        this.input_resolution.value = 60

        for (const element of [this.input_start, this.input_end, this.input_resolution]) {
            element.addEventListener("change", () => this.on_input_change())
        }

        // more event listeners
        this.button_preview.addEventListener("click", (_) => this.preview());
        this.button_download.addEventListener("click", (_) => this.download());

        this.input_format.addChangeListener((format) => {
            setCookie("download_format", format);
        })

        // initial update
        this.on_input_change()
    }

    on_input_change() {
        // check input validness
        if (this.download_url_for_inputs("json") === undefined) {
            this.output_expected_samples.innerText = ""

            this.button_preview.disabled = true
            this.button_download.disabled = true
        } else {
            const delta_sec = (this.input_end.valueAsDate - this.input_start.valueAsDate) / 1000;
            const samples = delta_sec / this.input_resolution.value;

            this.output_expected_samples.innerText = Math.ceil(samples).toString()

            const positive = samples >= 0
            const small = samples <= 1e6
            this.button_preview.disabled = !(positive && small);
            this.button_download.disabled = !positive
        }
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

        let param_dict = {
            "bucket_size": this.input_resolution.value,
            "oldest": start,
            "newest": end,
        }
        if (type === "csv") {
            param_dict.format = this.input_format.value
        }

        // noinspection JSCheckFunctionSignatures
        let params = new URLSearchParams(param_dict)
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
