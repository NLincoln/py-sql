import unittest
from math import log


class Schema:
    class Column:
        def __init__(self, *,
                     type=None,
                     size=1,
                     name=None,
                     nullable=False,
                     default=None,
                     auto_increment=False
                     ):
            self.type = type
            self.size = size
            self.name = name
            self.nullable = nullable
            self.default = default
            self.auto_increment = auto_increment

    class Type:
        class Ignored:
            @staticmethod
            def validate(value, column):
                return True

        class Int:
            @staticmethod
            def validate(value, column):
                if not isinstance(value, int):
                    return False
                if value == 0:
                    return True
                return int(log(value, 256)) + 1 <= column.size

        class Varchar:
            @staticmethod
            def validate(value, column):
                if not isinstance(value, str):
                    return False
                return len(value) <= column.size

    def __init__(self, *columns):
        self.columns = columns

    def validate(self, values):
        for column, value in zip(self.columns, values):
            if column.nullable and value is None:
                continue
            if not column.type.validate(value, column):
                return False
        return True


class DataStore:
    def __init__(self, *, readonly=False, rows=None):
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

    def insert_complete_row(self, row):
        if self.schema.validate(row):
            self.store.load_row(row)

    def insert_row(self, columns, row):
        values = []
        for i, column in enumerate(self.schema.columns):
            if column.name in columns:
                values.append(row[i])
            elif column.default:
                values.append(column.default)
            elif column.auto_increment:
                if len(self.store.rows) == 0:
                    values.append(0)
                else:
                    values.append(
                        self.store.rows[-1][i] + 1
                    )

        self.insert_complete_row(row=values)

    def get_row(self, index):
        row = self.store.get_row(index=index)
        return tuple([val for val, column in zip(row, self.schema.columns) if column.type != Schema.Type.Ignored])

    def get_copy(self):
        table = Table(schema=self.schema)
        table.store = self.store.get_readonly_copy()
        return table

    def get_data(self):
        return self.store.get_readonly_copy().rows

    def as_hash(self):
        result = []
        for row in self.store.rows:
            hash_ = {}
            for i, column in enumerate(self.schema.columns):
                hash_[column.name] = row[i]
            result.append(hash_)
        return result



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

        table.insert_complete_row(('a',))
        self.assertEqual(table.store.get_length(), 0)

    def test_column_default_values(self):
        schema = Schema(
            Schema.Column(name='id', type=Schema.Type.Int, size=2),
            Schema.Column(name='name', type=Schema.Type.Varchar, size=100),
            Schema.Column(name='lastname', type=Schema.Type.Varchar, size=100, default='lolz'),
        )
        table = Table(schema=schema)
        table.insert_row(('id', 'name'), (1, 'abc'))
        table.insert_row(('id', 'name', 'lastname'), (1, 'abc', 'def'))
        self.assertEqual(table.get_row(0), (1, 'abc', 'lolz'))
        self.assertEqual(table.get_row(1), (1, 'abc', 'def'))

    def test_auto_increment(self):
        table = Table(schema=Schema(
            Schema.Column(name='id', type=Schema.Type.Int, size=2, auto_increment=True),
            Schema.Column(name='lastname', type=Schema.Type.Varchar, size=100, default='lolz'),
        ))

        table.insert_row((), ())
        table.insert_row((), ())
        table.insert_row((), ())

        table.insert_row(('id',), (6,))
        table.insert_row((), ())

        self.assertEqual(table.get_row(1), (1, 'lolz'))
        self.assertEqual(table.get_row(3), (6, 'lolz'))
        self.assertEqual(table.get_row(4), (7, 'lolz'))


class TestSchema(unittest.TestCase):
    def test_validating_schemas(self):
        schema = Schema(
            Schema.Column(name='id', type=Schema.Type.Int),
            Schema.Column(name='name', type=Schema.Type.Varchar, size=10)
        )
        self.assertTrue(schema.validate((1, 'abc')))
        self.assertTrue(schema.validate((2, 'bef')))
        self.assertFalse(schema.validate(('d', 'def')))
        self.assertFalse(schema.validate(('d',)))

    def test_int_validator(self):
        column = Schema.Column(type=Schema.Type.Int)
        self.assertTrue(Schema.Type.Int.validate(1, column))
        self.assertTrue(Schema.Type.Int.validate(3, column))
        self.assertFalse(Schema.Type.Int.validate('a', column))
        self.assertFalse(Schema.Type.Int.validate((1,), column))
        self.assertFalse(Schema.Type.Int.validate([1], column))
        self.assertFalse(Schema.Type.Int.validate(['a'], column))

    def test_nullable_types(self):
        schema = Schema(
            Schema.Column(name='id', type=Schema.Type.Int, nullable=True),
            Schema.Column(name='name', type=Schema.Type.Varchar, nullable=False, size=10)
        )
        self.assertTrue(schema.validate((None, 'ab')))
        self.assertTrue(schema.validate((1, 'ab')))
        self.assertFalse(schema.validate((1, None)))

    def test_validating_size_variables(self):
        schema = Schema(
            Schema.Column(name='id', type=Schema.Type.Int, size=1)
        )
        self.assertTrue(schema.validate((1,)))
        self.assertFalse(schema.validate((256,)))
        self.assertTrue(schema.validate((255,)))


class QueryBuilder:
    def __init__(self, table):
        self.table = table
        self.conditions = []

    def where(self, expression):
        table = Table(schema=self.table.schema)
        for hash_row, row in zip(self.table.as_hash(), self.table.get_data()):
            if expression(hash_row):
                table.insert_complete_row(row)
        self.table = table

    def execute(self):
        return self.table


class Expressions:
    @staticmethod
    def equal_to(lhs, rhs):
        def func(row):
            return lhs(row) == rhs(row)
        return func

    @staticmethod
    def literal(value):
        def func(row):
            return value
        return func

    @staticmethod
    def column(value):
        def func(row):
            return row[value]
        return func


class TestQuery(unittest.TestCase):
    def test_simple_where_condition(self):
        table = Table(schema=Schema(
            Schema.Column(name='id', type=Schema.Type.Int),
            Schema.Column(name='name', type=Schema.Type.Varchar),
        ))

        table.insert_row(('id', 'name'), (1, 'a'))
        table.insert_row(('id', 'name'), (2, 'a'))
        table.insert_row(('id', 'name'), (3, 'a'))
        query = QueryBuilder(table)
        query.where(
            Expressions.equal_to(
                lhs=Expressions.column('id'),
                rhs=Expressions.literal(2)
            )
        )
        results = query.execute()
        self.assertEqual(results.get_row(0), (2, 'a'))

if __name__ == '__main__':
    unittest.main()
