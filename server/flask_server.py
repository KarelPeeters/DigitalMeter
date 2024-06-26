import mimetypes
from dataclasses import dataclass
from enum import auto, Enum
from io import StringIO
from threading import Thread
from typing import Optional

import flask
import simplejson
from flask import Flask, Response, current_app, request

from server.data import Database, Series, Buckets, SeriesKind

app = Flask(__name__, static_url_path="", static_folder="../resources")


class DownloadType(Enum):
    CSV = auto()
    CSV_BE = auto()
    JSON = auto()


@dataclass
class DownloadParams:
    bucket_size: Optional[int]
    oldest: Optional[int]
    newest: Optional[int]
    type: DownloadType
    kind: SeriesKind


class ParseDownloadError(ValueError):
    def __init__(self, html: str):
        self.html = html


def parse_download_params(args, ext: str) -> DownloadParams:
    all_args = args
    args = dict(args)
    curr_arg = None

    try:
        curr_arg = "oldest"
        oldest = args.pop("oldest", None)
        if oldest is not None:
            oldest = int(oldest)

        curr_arg = "newest"
        newest = args.pop("newest", None)
        if newest is not None:
            newest = int(newest)

        curr_arg = "bucket_size"
        bucket_size = args.pop("bucket_size")
        if bucket_size == "null":
            bucket_size = None
        else:
            bucket_size = int(bucket_size)
            if bucket_size < 1:
                raise ValueError()
        if bucket_size == 1:
            bucket_size = None

        curr_arg = "quantity"
        quantity = args.pop("quantity", None)
        if quantity == "power":
            quantity = SeriesKind.POWER
        elif quantity == "gas":
            quantity = SeriesKind.GAS
        elif quantity == "water_height":
            quantity = SeriesKind.WATER_HEIGHT
        elif quantity == "water_volume":
            quantity = SeriesKind.WATER_VOLUME
        else:
            raise ValueError()

        curr_arg = "type"
        if ext == "csv":
            csv_types = {
                "csv": DownloadType.CSV,
                "csv-be": DownloadType.CSV_BE,
            }
            csv_format = args.pop("format", "csv")
            ty = csv_types[csv_format]
        elif ext == "json":
            ty = DownloadType.JSON
        else:
            raise ValueError()

    except ValueError:
        curr_arg_str = f"'{curr_arg}'"
        curr_value = f"'{all_args.get(curr_arg)}'"
        raise ParseDownloadError(f"<p>Invalid parameter {flask.escape(curr_arg_str)}={flask.escape(curr_value)}</p>")
    except KeyError:
        curr_arg_str = f"'{curr_arg}'"
        raise ParseDownloadError(f"<p>Missing parameter {flask.escape(curr_arg_str)}</p>")
    if len(args) > 0:
        raise ParseDownloadError(f"<p>Unused parameters {flask.escape(list(args.keys()))}</p>")

    return DownloadParams(bucket_size, oldest, newest, ty, quantity)


def generate_csv(params: DownloadParams, database, csv_be_mode: bool):
    sep = "\t" if csv_be_mode else ","

    def generate():
        titles = ["timestamp"] + params.kind.value.columns
        yield sep.join(titles) + "\n"

        # convert data to string in batches, using StringIO for string concatenation
        data = database.fetch_series_items(params.kind, params.bucket_size, params.oldest, params.newest)
        while True:
            batch = data.fetchmany(10 * 1024)
            if len(batch) == 0:
                break

            writer = StringIO()
            for x in batch:
                line = sep.join(str(d) for d in x) + "\n"
                if csv_be_mode:
                    line = line.replace(".", ",")
                writer.write(line)
            yield writer.getvalue()

        # TODO if the user cancels the request this code does not run, are we leaking stuff?
        database.close()

    return app.response_class(generate(), mimetype="text/csv")


def generate_json(params: DownloadParams, database):
    series = Series.empty(params.kind, Buckets(None, params.bucket_size))

    # don't allow infinitely large json requests,
    #   since we don't stream the output and could run out of memory
    # TODO properly handle missing bucket size here
    if (params.oldest is None or params.newest is None or
            (params.bucket_size is not None and (params.newest - params.oldest) / params.bucket_size > 1e6)):
        error = "too many items requested"
    else:
        items = database.fetch_series_items(params.kind, params.bucket_size, params.oldest, params.newest)
        series.extend_items(items)
        error = None

    json_dict = series.to_json()
    if error is not None:
        json_dict["error"] = error

    json_str = simplejson.dumps(json_dict)
    return app.response_class(json_str, mimetype="application/json")


@app.route("/download/samples_<name>.<ext>")
def download_samples(name: str, ext: str):
    # name is only used to suggest a file name when downloading
    _ = name

    args = dict(request.args)
    print(f"Responding to download with args '{args}' and type '{ext}'")

    try:
        params = parse_download_params(request.args, ext)
    except ParseDownloadError as e:
        print(f"Error parsing download parameters: {e}")
        return e.html

    # open new temporary db connection
    # TODO reuse these? and are we leaking anything?
    database = Database(current_app.config["database_path"])

    if params.type == DownloadType.CSV:
        return generate_csv(params, database, csv_be_mode=False)
    elif params.type == DownloadType.CSV_BE:
        return generate_csv(params, database, csv_be_mode=True)
    elif params.type == DownloadType.JSON:
        return generate_json(params, database)
    else:
        ty_str = f"'{params.type}'"
        return f"<p>Unknown download type {flask.escape(ty_str)}</p>"


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

    threads = []
    for port in [8000, 80]:
        def target():
            app.run(host="0.0.0.0", port=port, threaded=True)

        thread = Thread(target=target)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


if __name__ == '__main__':
    flask_main("dummy.db")
