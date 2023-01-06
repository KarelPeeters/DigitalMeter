import mimetypes
import time

from flask import Flask, Response, current_app

from server.data import Database

app = Flask(__name__, static_url_path="", static_folder="../resources")


@app.route("/download/<bucket>.csv")
def download_csv(bucket):
    print(f"Responding to download with bucket size '{bucket}'")
    database = None

    try:
        bucket = int(bucket)
        if bucket < 1:
            raise ValueError()

        database = Database(current_app.config["database_path"])

        # TODO why is this slow?
        def generate():
            yield "timestamp,instant_power_1,instant_power_2,instant_power_3\n"

            data = database.fetch_series_items(bucket, None, None)

            while True:
                start = time.perf_counter()
                batch = data.fetchmany(1024)
                print(f"fetch took {time.perf_counter() - start}")
                if len(batch) == 0:
                    break

                start = time.perf_counter()
                result = ""
                for values in batch:
                    result += ",".join(str(v) for v in values) + "\n"
                print(f"concat took {time.perf_counter() - start}")

                start = time.perf_counter()
                yield result
                print(f"yield took {time.perf_counter() - start}")

            # TODO if the user cancels the request this code does not run, are we leaking stuff?
            database.close()

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


def flask_main(database_path: str):
    # fix for window registry being broken
    #  (and for python web apps checking the registry for this in the first place, why???)
    mimetypes.add_type("application/javascript", ".js")
    mimetypes.add_type("text/html", ".html")

    app.config["database_path"] = database_path
    app.run(host="0.0.0.0", port=8000, threaded=True)


if __name__ == '__main__':
    flask_main("dummy.db")
