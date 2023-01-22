import mimetypes
from dataclasses import dataclass
from enum import auto, Enum
from io import StringIO
from typing import Optional

import flask
from flask import Flask, Response, current_app, request

from server.data import Database

app = Flask(__name__, static_url_path="", static_folder="../resources")


class DownloadType(Enum):
    CSV = auto()
    JSON = auto()


@dataclass
class DownloadParams:
    bucket_size: int
    oldest: Optional[int]
    newest: Optional[int]
    type: DownloadType


class ParseDownloadError(ValueError):
    def __init__(self, html: str):
        self.html = html


def parse_download_params(args, ty: str) -> DownloadParams:
    args = dict(args)
    curr_arg = None

    try:
        curr_arg = "bucket_size"
        bucket_size = int(args.pop("bucket_size"))
        if bucket_size < 1:
            raise ValueError()

        curr_arg = "oldest"
        oldest = args.pop("oldest", None)
        if oldest is not None:
            oldest = int(oldest)

        curr_arg = "newest"
        newest = args.pop("newest", None)
        if newest is not None:
            newest = int(newest)

        curr_arg = "type"
        if ty == "csv":
            ty = DownloadType.CSV
        elif ty == "json":
            ty = DownloadType.JSON
        else:
            raise ValueError()

    except ValueError:
        curr_arg = f"'{curr_arg}'"
        raise ParseDownloadError(f"<p>Invalid parameter {flask.escape(curr_arg)}</p>")
    except KeyError:
        curr_arg = f"'{curr_arg}'"
        raise ParseDownloadError(f"<p>Missing parameter {flask.escape(curr_arg)}</p>")
    if len(args) > 0:
        raise ParseDownloadError(f"<p>Unused parameters {flask.escape(list(args.keys()))}</p>")

    return DownloadParams(bucket_size, oldest, newest, ty)


@app.route("/download/samples_<name>.<ty>")
def download_csv(name: str, ty: str):
    # name is only used to suggest a file name when downloading
    _ = name

    args = dict(request.args)
    print(f"Responding to download with args '{args}' and type '{ty}'")

    try:
        params = parse_download_params(request.args, ty)
    except ParseDownloadError as e:
        return e.html

    # open new temporary db connection
    database = Database(current_app.config["database_path"])

    def generate():
        yield "timestamp,instant_power_1,instant_power_2,instant_power_3\n"

        # convert data to csv in batches, using StringIO for string concatenation
        data = database.fetch_series_items(params.bucket_size, params.oldest, params.newest)
        while True:
            batch = data.fetchmany(10 * 1024)
            if len(batch) == 0:
                break

            writer = StringIO()
            for x in batch:
                writer.write(",".join(str(d) for d in x) + "\n")
            yield writer.getvalue()

        # TODO if the user cancels the request this code does not run, are we leaking stuff?
        database.close()

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


def flask_main(database_path: str):
    # fix for window registry being broken
    #  (and for python web apps checking the registry for this in the first place, why???)
    mimetypes.add_type("application/javascript", ".js")
    mimetypes.add_type("text/html", ".html")

    app.config["database_path"] = database_path
    app.run(host="0.0.0.0", port=8000, threaded=True)


if __name__ == '__main__':
    flask_main("dummy.db")
