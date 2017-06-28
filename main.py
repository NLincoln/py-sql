import unittest


class Schema:
    class Column:
        def __init__(self, *, type=None, size=1, name=None, nullable=False):
            self.type = type
            self.size = size
            self.name = name
            self.nullable = nullable

    class Type:
        class Ignored:
            @staticmethod
            def validate(*value):
                return True

        class Int:
            @staticmethod
            def validate(value):
                return isinstance(value, int)

        class Varchar:
            @staticmethod
            def validate(value):
                return isinstance(value, str)

    def __init__(self, *columns):
        self.columns = columns

    def validate(self, values):
        for column, value in zip(self.columns, values):
            if not column.type.validate(value):
                if column.nullable and value is None:
                    continue
                return False
        return True


class DataStore:
    def __init__(self, *, readonly = False, rows=None):
        if rows is None:
            rows = []
        self.rows = list(rows)
        self.readonly = readonly

    def load_row(self, row):
        if not self.readonly:
            self.rows.append(row)

    def get_length(self):
        return len(self.rows)

    def get_row(self, index):
        return self.rows[index]

    def get_readonly_copy(self):
        return DataStore(readonly=True, rows=self.rows)


class Table:
    def __init__(self, *, schema=None):
        assert schema, 'Tables must be initialized with a schema'
        self.schema = schema
        self.store = DataStore()

    def get_columns(self, *columns):
        schema_columns = []
        for column in self.schema.columns:
            if column.name in columns:
                schema_columns.append(column)
            else:
                schema_columns.append(
                    Schema.Column(type=Schema.Type.Ignored)
                )
        schema = Schema(*schema_columns)
        table = Table(schema=schema)
        table.store = self.store.get_readonly_copy()
        return table

    def insert_row(self, row):
        if self.schema.validate(row):
            self.store.load_row(row)

    def get_row(self, index):
        row = self.store.get_row(index=index)
        return tuple([val for val, column in zip(row, self.schema.columns) if column.type != Schema.Type.Ignored])


class TestDataStorage(unittest.TestCase):
    def test_loading_from_memory(self):
        schema = Schema(
            Schema.Column(name='id', type=Schema.Type.Int, size=2)
        )
        table = Table(schema=schema)
        table.store.load_row(2)

        self.assertEqual(table.store.get_row(0), 2)

    def test_read_only_copy(self):
        store = DataStore()
        store.load_row((1, 2, 3))
        store.load_row((3, 2, 1))
        copied_store = store.get_readonly_copy()
        copied_store.load_row((4, 5, 6))
        self.assertEqual(copied_store.get_length(), 2)
        with self.assertRaises(IndexError):
            copied_store.get_row(2)


class TestTables(unittest.TestCase):
    def test_loading_columns(self):
        schema = Schema(
            Schema.Column(name='id', type=Schema.Type.Int, size=2),
            Schema.Column(name='name', type=Schema.Type.Varchar, size=100),
            Schema.Column(name='lastname', type=Schema.Type.Varchar, size=100),
        )
        table = Table(schema=schema)
        table.store.load_row((1, 'abc', 'whoo'))
        table.store.load_row((2, 'def', 'hooo'))
        smaller_table = table.get_columns('name')
        self.assertEqual(smaller_table.get_row(0), ('abc',))
        self.assertEqual(smaller_table.get_row(1), ('def',))

    def test_schema(self):
        table = Table(schema=Schema(
            Schema.Column(type=Schema.Type.Int),
        ))

        table.insert_row(('a',))
        self.assertEqual(table.store.get_length(), 0)


class TestSchema(unittest.TestCase):
    def test_validating_schemas(self):
        schema = Schema(
            Schema.Column(name='id', type=Schema.Type.Int),
            Schema.Column(name='name', type=Schema.Type.Varchar)
        )
        self.assertTrue(schema.validate((1, 'abc')))
        self.assertTrue(schema.validate((2, 'bef')))
        self.assertFalse(schema.validate(('d', 'def')))
        self.assertFalse(schema.validate(('d',)))

    def test_int_validator(self):
        self.assertTrue(Schema.Type.Int.validate(1))
        self.assertTrue(Schema.Type.Int.validate(3))
        self.assertFalse(Schema.Type.Int.validate('a'))
        self.assertFalse(Schema.Type.Int.validate((1,)))
        self.assertFalse(Schema.Type.Int.validate([1]))
        self.assertFalse(Schema.Type.Int.validate(['a']))

    def test_nullable_types(self):
        schema = Schema(
            Schema.Column(name='id', type=Schema.Type.Int, nullable=True),
            Schema.Column(name='name', type=Schema.Type.Varchar, nullable=False)
        )
        self.assertTrue(schema.validate((None, 'ab')))
        self.assertTrue(schema.validate((1, 'ab')))
        self.assertFalse(schema.validate((1, None)))

if __name__ == '__main__':
    unittest.main()
