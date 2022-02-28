from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, Generic, Iterable, List, Optional, Type, TypeVar

from psycopg2._psycopg import AsIs, connection
from psycopg2.extras import RealDictCursor

from constants import CHUNK_SIZE

Serialized = Dict[str, Any]
T = TypeVar("T")


@dataclass
class Serialisable(Generic[T]):

    """Abstract base for deserializing content from a request."""

    __initializer = None
    __init__ = None

    def __new__(cls, *args, **kwargs):
        try:
            initializer = cls.__initializer
        except AttributeError:
            cls.__initializer = initializer = cls.__init__
            cls.__init__ = lambda *a, **k: None

        added_args = {}
        for name in list(kwargs.keys()):
            if name not in cls.__annotations__:
                added_args[name] = kwargs.pop(name)

        ret = object.__new__(cls)
        initializer(ret)
        for new_name, new_val in added_args.items():
            setattr(ret, new_name, new_val)

        return ret

    @classmethod
    def from_dict(cls: Type[T], d: Serialized) -> Optional[T]:
        if d is not None:
            return cls(**d)

    def to_dict(self) -> Serialized:
        d = dict()
        for k, v in asdict(self).items():
            if v is None:
                continue
            if isinstance(v, Enum):
                v = v.value
            elif isinstance(v, Serialisable):
                v = v.to_dict()

            d[k] = v

        return d

    def _insert(
        self,
        conn: connection,
        table_name: str,
        *returning
    ) -> None:
        """
        :param conn: 
        :param table_name: 
        :param returning: 
        :return: 
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            d = tuple(self.to_dict().items())
            keys = AsIs(",".join(k for k, v in d))
            values = tuple(v for k, v in d)
            if len(returning) == 0:
                cur.execute(f"INSERT INTO {table_name} (%s) VALUES %s", (keys, values))
            else:
                cur.execute(
                    f"INSERT INTO {table_name} (%s) VALUES %s RETURNING {','.join(returning)}",
                    (keys, values),
                )
                result = cur.fetchone()
                for field in returning:
                    setattr(self, field, result[field])

    def _update(
        self,
        conn: connection,
        table_name: str,
        *key_fields
    ) -> None:
        """
        :param conn
        :param table_name 
        :param key_fields
        :return: 
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            where_clause = []
            assignment_names = []
            assignment_values: List[Any] = []
            for k, v in self.to_dict().items():
                if k not in key_fields:
                    assignment_names.append("%s=%s")
                    assignment_values.extend((AsIs(k), v))

            for field_name in key_fields:
                where_clause.append(f"{field_name}=%s")
                assignment_values.append(getattr(self, field_name))

            statement = f"UPDATE {table_name} SET {','.join(assignment_names)} WHERE {' AND '.join(where_clause)}"
            cur.execute(statement, assignment_values)
            assert cur.rowcount == 1, f"Unexpectedly updated {cur.rowcount} {table_name} records"

    @classmethod
    def _fetch_one(
        cls: Type[Serialisable],
        conn: connection,
        query: str,
        *args: Any
    ) -> Optional[T]:
        """
        :param conn: 
        :param query: 
        :param args: 
        :return: 
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, args)
            result = cur.fetchone()
            return cls.from_dict(result)

    @classmethod
    def _fetch_many(
        cls: Type[Serialisable],
        conn: connection,
        query: str,
        *args: Any
    ) -> Iterable[T]:
        """
        :param conn: 
        :param query: 
        :param args: 
        :return: 
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, args)
            while True:
                results = cur.fetchmany(CHUNK_SIZE)
                if not results:
                    break
                for result in results:
                    obj = cls.from_dict(result)
                    if obj is not None:
                        yield obj

    @staticmethod
    def _delete_one(
        conn: connection,
        query: str,
        *args: Any
    ) -> bool:
        """
        :param conn: 
        :param query: 
        :param args: 
        :return: 
        """
        with conn.cursor() as cur:
            cur.execute(query, args)
            return True

    @staticmethod
    def _exists(
        conn: connection,
        table_name: str,
        table_id_name: str,
        table_id: Any
    ) -> bool:
        """
        :param conn: 
        :param table_name: 
        :param table_id_name: 
        :param table_id: 
        :return: 
        """
        with conn.cursor() as cur:
            # TODO: create an actual prepared statement
            cur.execute(
                f"select exists(select 1 from {table_name} where {table_id_name}=(%s));",
                (table_id,),
            )
            (result,) = cur.fetchone()
            return result
