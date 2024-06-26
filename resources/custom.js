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
        this.input_quantity = new RadioGroup("input_quantity", "power")
        this.output_expected_samples = document.getElementById("output_expected_samples")
        let input_elements = [this.input_start, this.input_end, this.input_resolution, this.input_format, this.input_quantity];

        this.previews_running = 0
        this.spinner = document.getElementById("spinner")

        this.button_preview = document.getElementById("button_preview");
        this.button_download = document.getElementById("button_download");

        // set reasonable initial values: last day, one sample per minute
        this.tz_offset_ms = (new Date()).getTimezoneOffset() * (60 * 1000)
        this.input_start.value = (new Date(Date.now() - this.tz_offset_ms - 24 * 3600 * 1000)).toISOString().slice(0, -8)
        this.input_end.value = (new Date(Date.now() - this.tz_offset_ms)).toISOString().slice(0, -8)
        this.input_resolution.value = 60

        for (const element of input_elements) {
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
        console.log("Input changed");

        const is_gas = this.input_quantity.value === "gas";
        this.input_resolution.disabled = is_gas;

        // check input validness
        if (this.download_url_for_inputs("json") === undefined) {
            this.output_expected_samples.innerText = ""

            this.button_preview.disabled = true
            this.button_download.disabled = true
        } else {
            const delta_sec = (this.getTime(this.input_end) - this.getTime(this.input_start)) / 1000;
            let samples;

            if (is_gas) {
                // approximate seconds between gas samples
                samples = delta_sec / 300;
            } else {
                samples = delta_sec / this.input_resolution.value;
            }

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

        this.previews_running++;
        this.spinner.style.visibility = 'visible'

        // TODO proper error handling
        window
            .fetch(url)
            .then((response) => response.json())
            .then((data) => {
                console.log("Preview data received")

                this.previews_running--;
                if (this.previews_running === 0) {
                    this.spinner.style.visibility='hidden'
                }

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
        let start = this.getTime(this.input_start) / 1000.0
        let end = this.getTime(this.input_end) / 1000.0

        const is_gas = this.input_quantity.value === "gas";

        let param_dict = {
            "oldest": start,
            "newest": end,
            "quantity": this.input_quantity.value,
            "bucket_size": is_gas ? null : this.input_resolution.value,
        }
        if (type === "csv") {
            param_dict.format = this.input_format.value
        }

        // noinspection JSCheckFunctionSignatures
        let params = new URLSearchParams(param_dict)
        return "../download/samples_custom." + type + "?" + params
    }

    update_plot() {
        let plot_obj = this.series.plot_obj(this.plot_style, true);
        if (this.first_plot_update) {
            // noinspection JSUnresolvedFunction
            Plotly.newPlot(this.plot, plot_obj);
        } else {
            // noinspection JSUnresolvedFunction
            Plotly.react(this.plot, plot_obj);
        }
    }

    getTime(field) {
        // workaround for valueAsDate not working on chromium-based browsers for some reason
        let date = field.valueAsDate;
        if (date !== null) {
            return date.getTime() + this.tz_offset_ms;
        }
        return new Date(field.value).getTime()
    }
}

// noinspection JSUnusedGlobalSymbols,JSUnusedLocalSymbols
const state = new State()
