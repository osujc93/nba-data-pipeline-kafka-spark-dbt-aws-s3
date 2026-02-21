import contextvars
import sys
import typing as t
from functools import update_wrapper
from types import TracebackType

from werkzeug.exceptions import HTTPException

from . import typing as ft
from .globals import _cv_app
from .globals import _cv_request
from .signals import appcontext_popped
from .signals import appcontext_pushed

if t.TYPE_CHECKING:
    from .app import Flask
    from .sessions import SessionMixin
    from .wrappers import Request


_sentinel = object()


class _AppCtxGlobals:
    """A plain object. Used as a namespace for storing data during an
    application context.

    Creating an app context automatically creates this object, which is
    made available as the :data:`g` proxy.
    """

    def __getattr__(self, name: str) -> t.Any:
        try:
            return self.__dict__[name]
        except KeyError:
            raise AttributeError(name) from None

    def __setattr__(self, name: str, value: t.Any) -> None:
        self.__dict__[name] = value

    def __delattr__(self, name: str) -> None:
        try:
            del self.__dict__[name]
        except KeyError:
            raise AttributeError(name) from None

    def get(self, name: str, default: t.Optional[t.Any] = None) -> t.Any:
        return self.__dict__.get(name, default)

    def pop(self, name: str, default: t.Any = _sentinel) -> t.Any:
        if default is _sentinel:
            return self.__dict__.pop(name)
        else:
            return self.__dict__.pop(name, default)

    def setdefault(self, name: str, default: t.Any = None) -> t.Any:
        return self.__dict__.setdefault(name, default)

    def __contains__(self, item: str) -> bool:
        return item in self.__dict__

    def __iter__(self) -> t.Iterator[str]:
        return iter(self.__dict__)

    def __repr__(self) -> str:
        ctx = _cv_app.get(None)
        if ctx is not None:
            return f"<flask.g of '{ctx.app.name}'>"
        return object.__repr__(self)


def after_this_request(f: ft.AfterRequestCallable) -> ft.AfterRequestCallable:
    ctx = _cv_request.get(None)

    if ctx is None:
        raise RuntimeError(
            "'after_this_request' can only be used when a request"
            " context is active, such as in a view function."
        )

    ctx._after_request_functions.append(f)
    return f


def copy_current_request_context(f: t.Callable) -> t.Callable:
    ctx = _cv_request.get(None)

    if ctx is None:
        raise RuntimeError(
            "'copy_current_request_context' can only be used when a"
            " request context is active, such as in a view function."
        )

    ctx = ctx.copy()

    def wrapper(*args, **kwargs):
        with ctx:
            return ctx.app.ensure_sync(f)(*args, **kwargs)

    return update_wrapper(wrapper, f)


def has_request_context() -> bool:
    return _cv_request.get(None) is not None


def has_app_context() -> bool:
    return _cv_app.get(None) is not None


class AppContext:
    """The app context contains application-specific information."""

    def __init__(self, app: "Flask") -> None:
        self.app = app
        self.url_adapter = app.create_url_adapter(None)
        self.g: _AppCtxGlobals = app.app_ctx_globals_class()
        self._cv_tokens: t.List[contextvars.Token] = []

    def push(self) -> None:
        self._cv_tokens.append(_cv_app.set(self))
        appcontext_pushed.send(self.app)

    def pop(self, exc: t.Optional[BaseException] = _sentinel) -> None:
        try:
            if len(self._cv_tokens) == 1:
                if exc is _sentinel:
                    exc = sys.exc_info()[1]
                self.app.do_teardown_appcontext(exc)
        finally:
            ctx = _cv_app.get()
            _cv_app.reset(self._cv_tokens.pop())

        if ctx is not self:
            raise AssertionError(
                f"Popped wrong app context. ({ctx!r} instead of {self!r})"
            )

        appcontext_popped.send(self.app)

    def __enter__(self) -> "AppContext":
        self.push()
        return self

    def __exit__(
        self,
        exc_type: t.Optional[type],
        exc_value: t.Optional[BaseException],
        tb: t.Optional[TracebackType],
    ) -> None:
        self.pop(exc_value)


class RequestContext:
    """The request context contains per-request information."""

    def __init__(
        self,
        app: "Flask",
        environ: dict,
        request: t.Optional["Request"] = None,
        session: t.Optional["SessionMixin"] = None,
    ) -> None:
        self.app = app
        if request is None:
            request = app.request_class(environ)
            request.json_module = app.json
        self.request: "Request" = request
        self.url_adapter = None
        try:
            self.url_adapter = app.create_url_adapter(self.request)
        except HTTPException as e:
            self.request.routing_exception = e
        self.flashes: t.Optional[t.List[t.Tuple[str, str]]] = None
        self.session: t.Optional["SessionMixin"] = session
        self._after_request_functions: t.List[ft.AfterRequestCallable] = []
        self._cv_tokens: t.List[t.Tuple[contextvars.Token, t.Optional[AppContext]]] = []

    def copy(self) -> "RequestContext":
        return self.__class__(
            self.app,
            environ=self.request.environ,
            request=self.request,
            session=self.session,
        )

    def match_request(self) -> None:
        try:
            result = self.url_adapter.match(return_rule=True)
            self.request.url_rule, self.request.view_args = result
        except HTTPException as e:
            self.request.routing_exception = e

    def push(self) -> None:
        app_ctx = _cv_app.get(None)
        if app_ctx is None or app_ctx.app is not self.app:
            app_ctx = self.app.app_context()
            app_ctx.push()
        else:
            app_ctx = None

        self._cv_tokens.append((_cv_request.set(self), app_ctx))

        if self.session is None:
            session_interface = self.app.session_interface
            self.session = session_interface.open_session(self.app, self.request)

            from datetime import datetime

            if (
                self.session is not None
                and hasattr(self.session, "expiry")
                and self.session.expiry
            ):
                if getattr(self.session.expiry, "tzinfo", None) is not None:
                    self.session.expiry = self.session.expiry.replace(tzinfo=None)

                if isinstance(self.session.expiry, (list, tuple)):
                    if len(self.session.expiry) == 1:
                        self.session.expiry = self.session.expiry[0]

                if isinstance(self.session.expiry, dict):
                    if len(self.session.expiry) == 1:
                        key = next(iter(self.session.expiry))
                        maybe_val = self.session.expiry[key]
                        if isinstance(maybe_val, datetime) or isinstance(maybe_val, str):
                            self.session.expiry = maybe_val

                if not isinstance(self.session.expiry, datetime):
                    try:
                        self.session.expiry = datetime.fromisoformat(str(self.session.expiry))
                    except Exception:
                        self.session.expiry = str(self.session.expiry)

        if self.url_adapter is not None:
            self.match_request()

    def pop(self, exc: t.Optional[BaseException] = _sentinel) -> None:
        clear_request = len(self._cv_tokens) == 1

        try:
            if clear_request:
                if exc is _sentinel:
                    exc = sys.exc_info()[1]
                self.app.do_teardown_request(exc)

                request_close = getattr(self.request, "close", None)
                if request_close is not None:
                    request_close()
        finally:
            ctx = _cv_request.get()
            token, app_ctx = self._cv_tokens.pop()
            _cv_request.reset(token)

            if clear_request:
                ctx.request.environ["werkzeug.request"] = None

            if app_ctx is not None:
                app_ctx.pop(exc)

            if ctx is not self:
                raise AssertionError(
                    f"Popped wrong request context. ({ctx!r} instead of {self!r})"
                )

    def __enter__(self) -> "RequestContext":
        self.push()
        return self

    def __exit__(
        self,
        exc_type: t.Optional[type],
        exc_value: t.Optional[BaseException],
        tb: t.Optional[TracebackType],
    ) -> None:
        self.pop(exc_value)

    def __repr__(self) -> str:
        return (
            f"<{type(self).__name__} {self.request.url!r}"
            f" [{self.request.method}] of {self.app.name}>"
        )
