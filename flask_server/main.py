from flask import Flask, Response

app = Flask(__name__, static_url_path="")


@app.route("/download/<group>.csv")
def download_csv(group):
    def generate():
        yield f"CSV file with group {group}:\n"
        for i in range(int(1e9)):
            print(f"yielding {i}")
            yield f"{i}, 5, 6, 7, 9,\n"

    return app.response_class(generate(), mimetype="text/csv")


@app.route("/")
def root():
    return app.send_static_file("index.html")


@app.after_request
def add_headers(response: Response):
    response.cache_control.no_cache = True
    response.cache_control.no_store = True
    response.cache_control.must_revalidate = True
    response.expires = 0
    return response


def main():
    # TODO add host=0.0.0.0 to open to public
    app.run(port=8000)


if __name__ == '__main__':
    main()
