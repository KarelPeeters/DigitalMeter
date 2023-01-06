from typing import Optional

from flask import Flask, Response

from server.data import Database

app = Flask(__name__, static_url_path="", static_folder="../resources")
database: Optional[Database] = None


@app.route("/download/<bucket>.csv")
def download_csv(bucket):
    def generate():
        yield "timestamp,instant_power_1,instant_power_2,instant_power_3\n"
        for values in database.fetch_series_items(bucket, None, None):
            yield ",".join(str(v) for v in values) + "\n"

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
    global database
    database = Database("dummy.db")
    # TODO add host=0.0.0.0 to open to public
    app.run(port=8000, threaded=False)


if __name__ == '__main__':
    main()
