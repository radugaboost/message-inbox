from typing import Any, Callable, TypeVar


class InboxRouter:
    def __init__(self) -> None:
        self._handlers: dict[str, Callable[..., Any]] = {}

    def on_event(self, event_type: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        func_type = TypeVar("func_type", bound=Callable[..., Any])

        def decorator(func: func_type) -> func_type:
            self._handlers[event_type] = func
            return func

        return decorator

    def get_handler(self, event_type: str) -> Callable[..., Any] | None:
        return self._handlers.get(event_type)
