class RadioGroup {
    constructor(name, value) {
        this.name = name
        this.value = value
    }

    get value() {
        let element = document.querySelector('input[name="' + this.name + '"]:checked');
        if (element === null) {
            console.log("Invalid name for radio group", this.name)
            return;
        }
        return element.value
    }

    set value(value) {
        let element = document.querySelector('input[name="' + this.name + '"][value="' + value + '"]');
        if (element === null) {
            console.log("Invalid name or value for radio group", this.name, value)
            return;
        }
        element.checked = true
    }

    addChangeListener(f) {
        for (const element of document.querySelectorAll('input[name="' + this.name + '"]')) {
            element.addEventListener("change", e => f(e.target.value))
        }
    }

    addEventListener(event, f) {
        if (event !== "change") {
            console.log("Only change event supported for radio groups")
            return
        }
        this.addChangeListener(f)
    }
}

class PlotStyle {
    constructor(radio_split, radio_total, check_zero, on_change) {
        this.split_kind = getCookie("plot_style", "split")
        this.include_zero = getCookie("include_zero", false) === "true"

        this.radio_split = new RadioGroup("split_kind", this.split_kind)

        this.check_zero = check_zero
        this.check_zero.checked = this.include_zero

        this.radio_split.addChangeListener(split_kind => {
            this.on_plot_setting_changed(split_kind, this.include_zero)
        })
        this.check_zero.addEventListener("change", () => {
            this.on_plot_setting_changed(this.split_kind, this.check_zero.checked)
        })

        this.on_change = on_change
    }

    on_plot_setting_changed(split_kind, include_zero) {
        const changed = split_kind !== this.split_kind || include_zero !== this.include_zero;
        if (!changed) return
        console.log("Style changed to", this.split_kind, this.include_zero)

        // save values
        this.split_kind = split_kind
        this.include_zero = include_zero
        setCookie("plot_style", split_kind)
        setCookie("include_zero", include_zero)

        // queue callback
        setTimeout(this.on_change, 0)
    }
}

function getCookie(key, default_value) {
    // from https://stackoverflow.com/a/25490531/5517612
    let result = document.cookie.match('(^|;)\\s*' + key + '\\s*=\\s*([^;]+)')?.pop()

    if (result === undefined) {
        return default_value;
    }
    return result;
}

function setCookie(key, value) {
    document.cookie = key + "=" + value + ";path=/;SameSite=Lax";
}

class Series {
    constructor() {
        this.window_size = 0
        this.bucket_size = 0
        this.unit_label = ""
        this.kind = ""

        this.timestamps = []
        this.all_values = {}
        this.data_revision = 0

        this.last_timestamp_int = 0
        this.last_timestamp_date = new Date(0);
    }

    push_update(series_data) {
        let timestamps = this.timestamps;
        let all_values = this.all_values;

        this.window_size = series_data["window_size"];
        this.bucket_size = series_data["bucket_size"];
        this.unit_label = series_data["unit_label"];
        this.kind = series_data["kind"];

        // append data to state
        for (let i = 0; i < series_data["timestamps"].length; i++) {
            let ts_int = series_data["timestamps"][i];

            // add padding values if necessary
            if (this.bucket_size !== null && this.last_timestamp_int !== 0) {
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
        if (this.window_size !== null && timestamps.length >= 1) {
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

    plot_obj(plot_style, interactive = false) {
        // data
        let data = []

        if (plot_style.split_kind === "split") {
            for (const key of Object.keys(this.all_values)) {
                data.push({
                    x: this.timestamps,
                    y: this.all_values[key],
                    name: key,
                    mode: "lines",
                })
            }
        } else if (plot_style.split_kind === "total") {
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
            console.log("Invalid split_kind", plot_style.split_kind)
            return
        }

        // layout
        this.data_revision += 1;
        let layout = {
            datarevision: this.data_revision,
            margin: {t: 0},
            showlegend: true,
            xaxis: {
                type: "date",
            },
            yaxis: {
                title: {
                    text: this.unit_label,
                }
            }
        };

        if (this.window_size !== null) {
            layout.xaxis.range = [this.last_timestamp_date - this.window_size * 1000, this.last_timestamp_date]
        }
        if (plot_style.include_zero && this.kind !== "gas") {
            layout.yaxis.rangemode = "tozero"
        }

        // config
        let config = {
            staticPlot: !interactive,
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
