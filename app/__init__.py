from pathlib import Path

from flask import Flask

from .db import init_app as init_db_app
from .views import register_routes


def create_app() -> Flask:
    app = Flask(__name__, instance_relative_config=True)
    app.config.update(
        SECRET_KEY="change-this-in-production",
        DATABASE=Path(app.instance_path) / "accomplishment.db",
        REPORT_OUTPUT_DIR=Path(app.instance_path) / "reports",
        UPLOAD_DIR=Path(app.instance_path) / "uploads",
    )

    Path(app.instance_path).mkdir(parents=True, exist_ok=True)
    Path(app.config["REPORT_OUTPUT_DIR"]).mkdir(parents=True, exist_ok=True)
    Path(app.config["UPLOAD_DIR"]).mkdir(parents=True, exist_ok=True)

    init_db_app(app)
    register_routes(app)
    return app
