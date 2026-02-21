import hashlib
import typing as t
import warnings
from collections.abc import MutableMapping
from datetime import datetime
from datetime import timezone

from itsdangerous import BadSignature
from itsdangerous import URLSafeTimedSerializer
from werkzeug.datastructures import CallbackDict

from .helpers import is_ip
from .json.tag import TaggedJSONSerializer

if t.TYPE_CHECKING:
    import typing_extensions as te
    from .app import Flask
    from .wrappers import Request, Response


class SessionMixin(MutableMapping):
    """Expands a basic dictionary with session attributes."""

    @property
    def permanent(self) -> bool:
        """This reflects the ``'_permanent'`` key in the dict."""
        return self.get("_permanent", False)

    @permanent.setter
    def permanent(self, value: bool) -> None:
        self["_permanent"] = bool(value)

    new = False

    modified = True

    accessed = True


class SecureCookieSession(CallbackDict, SessionMixin):
    """Base class for sessions based on signed cookies.

    This session backend will set the :attr:`modified` and
    :attr:`accessed` attributes. It cannot reliably track whether a
    session is new (vs. empty), so :attr:`new` remains hard coded to
    ``False``.
    """

    modified = False

    accessed = False

    def __init__(self, initial: t.Any = None) -> None:
        def on_update(self) -> None:
            self.modified = True
            self.accessed = True

        super().__init__(initial, on_update)

    def __getitem__(self, key: str) -> t.Any:
        self.accessed = True
        return super().__getitem__(key)

    def get(self, key: str, default: t.Any = None) -> t.Any:
        self.accessed = True
        return super().get(key, default)

    def setdefault(self, key: str, default: t.Any = None) -> t.Any:
        self.accessed = True
        return super().setdefault(key, default)


class NullSession(SecureCookieSession):
    """Class used to generate nicer error messages if sessions are not
    available.  Will still allow read-only access to the empty session
    but fail on setting.
    """

    def _fail(self, *args: t.Any, **kwargs: t.Any) -> "te.NoReturn":
        raise RuntimeError(
            "The session is unavailable because no secret "
            "key was set.  Set the secret_key on the "
            "application to something unique and secret."
        )

    __setitem__ = __delitem__ = clear = pop = popitem = update = setdefault = _fail
    del _fail


class SessionInterface:
    """The basic interface you have to implement in order to replace the
    default session interface which uses werkzeug's securecookie
    implementation.  The only methods you have to implement are
    :meth:`open_session` and :meth:`save_session`, the others have
    useful defaults which you don't need to change.

    ...
    """

    null_session_class = NullSession
    pickle_based = False

    def make_null_session(self, app: "Flask") -> NullSession:
        return self.null_session_class()

    def is_null_session(self, obj: object) -> bool:
        return isinstance(obj, self.null_session_class)

    def get_cookie_name(self, app: "Flask") -> str:
        return app.config["SESSION_COOKIE_NAME"]

    def get_cookie_domain(self, app: "Flask") -> t.Optional[str]:
        rv = app.config["SESSION_COOKIE_DOMAIN"]
        if rv is not None:
            return rv if rv else None

        rv = app.config["SERVER_NAME"]
        if not rv:
            app.config["SESSION_COOKIE_DOMAIN"] = False
            return None

        rv = rv.rsplit(":", 1)[0].lstrip(".")
        if "." not in rv:
            warnings.warn(
                f"{rv!r} is not a valid cookie domain, it must contain"
                " a '.'. Add an entry to your hosts file, for example"
                f" '{rv}.localdomain', and use that instead."
            )
            app.config["SESSION_COOKIE_DOMAIN"] = False
            return None

        ip = is_ip(rv)
        if ip:
            warnings.warn(
                "The session cookie domain is an IP address. This may not work"
                " as intended in some browsers. Add an entry to your hosts"
                ' file, for example "localhost.localdomain", and use that'
                " instead."
            )

        if self.get_cookie_path(app) == "/" and not ip:
            rv = f".{rv}"
        app.config["SESSION_COOKIE_DOMAIN"] = rv
        return rv

    def get_cookie_path(self, app: "Flask") -> str:
        return app.config["SESSION_COOKIE_PATH"] or app.config["APPLICATION_ROOT"]

    def get_cookie_httponly(self, app: "Flask") -> bool:
        return app.config["SESSION_COOKIE_HTTPONLY"]

    def get_cookie_secure(self, app: "Flask") -> bool:
        return app.config["SESSION_COOKIE_SECURE"]

    def get_cookie_samesite(self, app: "Flask") -> str:
        return app.config["SESSION_COOKIE_SAMESITE"]

    def get_expiration_time(
        self, app: "Flask", session: SessionMixin
    ) -> t.Optional[datetime]:
        if session.permanent:
            return datetime.now(timezone.utc) + app.permanent_session_lifetime
        return None

    def should_set_cookie(self, app: "Flask", session: SessionMixin) -> bool:
        return session.modified or (
            session.permanent and app.config["SESSION_REFRESH_EACH_REQUEST"]
        )

    def open_session(
        self, app: "Flask", request: "Request"
    ) -> t.Optional[SessionMixin]:
        raise NotImplementedError()

    def save_session(
        self, app: "Flask", session: SessionMixin, response: "Response"
    ) -> None:
        raise NotImplementedError()


session_json_serializer = TaggedJSONSerializer()


class SecureCookieSessionInterface(SessionInterface):
    salt = "cookie-session"
    digest_method = staticmethod(hashlib.sha1)
    key_derivation = "hmac"
    serializer = session_json_serializer
    session_class = SecureCookieSession

    def get_signing_serializer(
        self, app: "Flask"
    ) -> t.Optional[URLSafeTimedSerializer]:
        if not app.secret_key:
            return None
        signer_kwargs = dict(
            key_derivation=self.key_derivation, digest_method=self.digest_method
        )
        return URLSafeTimedSerializer(
            app.secret_key,
            salt=self.salt,
            serializer=self.serializer,
            signer_kwargs=signer_kwargs,
        )

    def open_session(
        self, app: "Flask", request: "Request"
    ) -> t.Optional[SecureCookieSession]:
        s = self.get_signing_serializer(app)
        if s is None:
            return None
        val = request.cookies.get(self.get_cookie_name(app))
        if not val:
            return self.session_class()
        max_age = int(app.permanent_session_lifetime.total_seconds())
        try:
            data = s.loads(val, max_age=max_age)
            return self.session_class(data)
        except BadSignature:
            return self.session_class()

    def save_session(
        self, app: "Flask", session: SessionMixin, response: "Response"
    ) -> None:
        name = self.get_cookie_name(app)
        domain = self.get_cookie_domain(app)
        path = self.get_cookie_path(app)
        secure = self.get_cookie_secure(app)
        samesite = self.get_cookie_samesite(app)
        httponly = self.get_cookie_httponly(app)

        if session is not None and hasattr(session, "accessed") and session.accessed:
            response.vary.add("Cookie")

        if not session or session is None:
            if (
                session is not None
                and hasattr(session, "modified")
                and session.modified
            ):
                response.delete_cookie(
                    name,
                    domain=domain,
                    path=path,
                    secure=secure,
                    samesite=samesite,
                    httponly=httponly,
                )
                response.vary.add("Cookie")
            return

        if not self.should_set_cookie(app, session):
            return

        expires = self.get_expiration_time(app, session)
        serializer = self.get_signing_serializer(app)

        if serializer is None:
            return

        val = serializer.dumps(dict(session))
        response.set_cookie(
            name,
            val,
            expires=expires,
            httponly=httponly,
            domain=domain,
            path=path,
            secure=secure,
            samesite=samesite,
        )
        response.vary.add("Cookie")
