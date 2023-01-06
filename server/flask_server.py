import mimetypes
from typing import Optional

from flask import Flask, Response

from server.data import Database

app = Flask(__name__, static_url_path="", static_folder="../resources")
global_database: Optional[Database] = None


@app.route("/download/<bucket>.csv")
def download_csv(bucket):
    print(f"Responding to download with bucket size {bucket}")

    try:
        bucket = int(bucket)
        if bucket < 1:
            raise ValueError()

        def generate():
            yield "timestamp,instant_power_1,instant_power_2,instant_power_3\n"
            for values in global_database.fetch_series_items(bucket, None, None):
                yield ",".join(str(v) for v in values) + "\n"

        return app.response_class(generate(), mimetype="text/csv")
    except ValueError:
        return "<p>Invalid or missing bucket size</p>"


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


def flask_main(database: Database):
    global global_database
    global_database = database

    mimetypes.add_type("application/javascript", ".js")
    mimetypes.add_type("text/html", ".html")

    # TODO add host=0.0.0.0 to open to public
    app.run(port=8000, threaded=False)


if __name__ == '__main__':
    flask_main(Database("dummy.db"))
