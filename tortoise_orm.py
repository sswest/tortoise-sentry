from glob import glob
from pathlib import Path
from inspect import getmembers, isclass
from importlib import import_module

from sentry_sdk import Hub
from sentry_sdk.integrations import Integration, DidNotEnable


def find_db_client():
    from tortoise import backends
    from tortoise.backends.base.client import BaseDBAsyncClient

    _imported = set()
    cls = set()

    base = Path(backends.__file__).parent

    for path in glob(f"{base}/**/client.py", recursive=True):
        if path not in _imported:
            if "__init__" in path:
                *_, name, __ = path.split("/")
            _imported.add(path)
            path = path[:-3].split("/")
            for p, s in enumerate(path):
                if s == "tortoise":
                    path = path[p:]
                    break
            try:
                mod = import_module('.'.join(path))
            except ModuleNotFoundError:
                continue
            for _, member in getmembers(mod):
                if isclass(member) and issubclass(member, BaseDBAsyncClient):
                    mro = member.mro()
                    if len(mro) > 2 and mro[1] is BaseDBAsyncClient:
                        cls.add(member)
    return list(cls)


class TortoiseIntegration(Integration):
    identifier = "tortoise"

    @staticmethod
    def setup_once():
        try:
            import tortoise
        except ImportError:
            raise DidNotEnable("Tortoise-orm not installed")

        patch_queryset()

        for client in find_db_client():
            patch_db_client(client, "execute_query", "execute")
            patch_db_client(client, "execute_query_dict", "execute_dict")
            patch_db_client(client, "execute_insert", "insert")
            patch_db_client(client, "execute_script", "script")
            patch_db_client(client, "execute_many", "many_execute")


def patch_queryset():
    from tortoise.queryset import QuerySet

    old_execute = QuerySet._execute

    async def sentry_patch_execute(self):
        hub = Hub.current

        if hub.get_integration(TortoiseIntegration) is None:
            return old_execute(self)

        with hub.start_span(op="tortoise", description="queryset") as span:
            describe = self.model.describe()
            span.set_tag("app", describe.get("app"))
            span.set_tag("db", self._db.connection_name)
            span.set_tag("table", describe.get("table"))
            span.set_tag("model", self.model.__name__)
            span.set_data("sql", self.sql())
            return await old_execute(self)

    QuerySet._execute = sentry_patch_execute


def patch_db_client(cls, method_name, desc):
    old_func = getattr(cls, method_name)

    async def sentry_patch_method(self, *args, **kwargs):
        hub = Hub.current

        if hub.get_integration(TortoiseIntegration) is None:
            return await old_func(self, *args, **kwargs)

        with hub.start_span(op="db", description=desc) as span:
            if query := kwargs.get("query", args[0] if len(args) > 0 else None):
                span.set_data("query", query)
            if values := kwargs.get("values", args[1] if len(args) > 1 else None):
                span.set_data("values", values)

            return await old_func(self, *args, **kwargs)

    setattr(cls, method_name, sentry_patch_method)
