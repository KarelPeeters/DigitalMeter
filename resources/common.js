class PlotStyle {
    constructor(radio_split, radio_total, check_zero, on_change) {
        this.split_kind = getCookie("plot_style", "split")
        this.include_zero = getCookie("include_zero", false) === "true"

        this.radio_split = radio_split
        this.radio_total = radio_total
        this.check_zero = check_zero

        this.radio_split.addEventListener("change", e => this.on_plot_setting_changed(e))
        this.radio_total.addEventListener("change", e => this.on_plot_setting_changed(e))
        this.check_zero.addEventListener("change", e => this.on_plot_setting_changed(e))

        this.on_change = on_change
    }

    on_plot_setting_changed(e) {
        let old_split_kind = this.split_kind
        let old_zero = this.include_zero

        if (e.target === this.radio_total || e.target === this.radio_split) {
            this.split_kind = e.target.value
            setCookie("split_kind", this.split_kind)
        } else if (e.target === this.check_zero) {
            this.include_zero = e.target.checked
            setCookie("include_zero", this.include_zero)
        } else {
            console.log("Unexpected event target", e, e.target);
        }

        console.log("Style changed to", this.split_kind, this.include_zero)

        if (this.split_kind !== old_split_kind || this.include_zero !== old_zero) {
            // queue callback
            setTimeout(this.on_change, 0)
        }
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
