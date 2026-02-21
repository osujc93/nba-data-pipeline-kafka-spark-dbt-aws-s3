import logging
import os
import shutil
from datetime import datetime
from typing import Any

from flask import Flask
from flask_appbuilder.const import AUTH_DB
from flask_session import FileSystemSessionInterface

secret_key = os.environ.get("AIRFLOW__WEBSERVER__SECRET_KEY", "hLShosEyV4h6jCpcF0BYFQ==")

WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

AUTH_TYPE = AUTH_DB

SESSION_TYPE = "filesystem"
SESSION_USE_SIGNER = False


class PatchedFileSystemSessionInterface(FileSystemSessionInterface):
    def open_session(self, app: Flask, request: Any) -> Any:
        print("DEBUG: Inside patched open_session()")
        rv = super().open_session(app, request)
        if rv and getattr(rv, "expiry", None):
            if rv.expiry.utcoffset() is not None:
                rv.expiry = rv.expiry.replace(tzinfo=None)
        return rv


def configure_app(app: Flask) -> None:
    logger = logging.getLogger("webserver_config")
    logger.info("Using filesystem-based Flask session with tzinfo-stripping monkey patch.")

    app.session_interface = PatchedFileSystemSessionInterface(app)

    shutil.rmtree("/tmp/flask_session", ignore_errors=True)
    logger.info("Old session data in /tmp/flask_session has been removed.")
