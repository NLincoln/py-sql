import unittest
from enum import Enum


class Schema:
    class Column:
        def __init__(self, *, type=None, size=1, name=None):
            self.type = type
            self.size = size
            self.name = name

    class Type(Enum):
        Int = 0
        Varchar = 1

    def __init__(self, *columns):
        self.columns = columns


class DataStore:
    def __init__(self):
        self.rows = []

    def load_row(self, row):
        self.rows.append(row)

    def get_row(self, index):
        return self.rows[index]


class Table:
    def __init__(self, *, schema=None):
        assert schema, 'Tables must be initialized with a schema'
        self.schema = schema
        self.store = DataStore()


class Statements:
    pass


class TestTables(unittest.TestCase):
    def test_loading_and_getting(self):
        pass


class TestDataStorage(unittest.TestCase):
    def test_loading_from_memory(self):
        schema = Schema(
            Schema.Column(name='id', type=Schema.Type.Int, size=2)
        )
        table = Table(schema=schema)
        table.store.load_row((2))

        self.assertEqual(table.store.get_row(table, 0), (2))

if __name__ == '__main__':
    unittest.main()
