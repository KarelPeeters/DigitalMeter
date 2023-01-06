from flask import Flask, Response

app = Flask(__name__, static_url_path="")


@app.route("/download/<group>.csv")
def hello_world(group):
    return f"<p>CSV file with group {group}</p>"


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
