from __future__ import annotations

from typing import Optional, Iterable

from psycopg2._psycopg import connection

from abc import Serialisable


class Model(Serialisable):

    model_id: int

    def to_db(self, conn: connection) -> None:
        """Single table insert from a table called model"""
        self._insert(conn, "model", "model_id")

    def update_db(self, conn: connection) -> None:
        """Single table insert from a table called model"""
        return self._update(conn, "model", "model_id")

    @classmethod
    def from_db(cls, conn, customer_id: int) -> Optional[Model]:
        """Single record retrieval from a table called model"""
        return cls._fetch_one(
            conn,
            "SELECT * FROM customer WHERE customer_id = (%s)",
            customer_id
        )

    @staticmethod
    def all_models(conn: connection) -> Iterable[Model]:
        """Returns many records"""
        return Model._fetch_many(
            conn,
            "SELECT * FROM model"
        )

    @classmethod
    def model_exists(cls, conn: connection, model_id: int) -> bool:
        """Returns a true/false if the record does/does not exists"""
        return cls._exists(conn, "model", "model_id", model_id)
