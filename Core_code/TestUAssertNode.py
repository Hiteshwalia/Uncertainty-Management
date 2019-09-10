

import Transaction
import unittest
import sqlite3, datetime

class TestInsertDBAssertNode(unittest.TestCase):

    def test_insertWithAssertDate(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 1, 1),
                    datetime.date(2015, 1, 1),
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

            self.assertEqual(e.exception.args[0], 'knowledgeBeginBegin <= knowledgeBeginEnd < knowledgeEndEnd')

            tx.commit()
            con.close()

    def test_insertWithAssertTypeDateBB(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                tx.addNewNode(
                    'Person',
                    2,
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 1, 1),
                    datetime.date(2015, 1, 1),
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

            self.assertEqual(e.exception.args[0], 'Need knowledgeBeginBegin datetime.date')

            tx.commit()
            con.close()

    def test_insertWithAssertTypeDateBE(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 4),
                    2,
                    datetime.date(2015, 1, 1),
                    datetime.date(2015, 1, 1),
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

            self.assertEqual(e.exception.args[0], 'Need knowledgeBeginEnd datetime.date')

            tx.commit()
            con.close()

    def test_insertWithAssertTypeDateEB(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 1, 4),
                    2,
                    datetime.date(2015, 1, 1),
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

            self.assertEqual(e.exception.args[0], 'Need knowledgeEndBegin datetime.date')

            tx.commit()
            con.close()

    def test_insertWithAssertTypeDateEE(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    2,
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

            self.assertEqual(e.exception.args[0], 'Need knowledgeEndEnd datetime.date')

            tx.commit()
            con.close()

    def test_insertWithAssertNodeType(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                tx.addNewNode(
                    'Pokemon',
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

            self.assertEqual(e.exception.args[0], 'Invalid nodeType')

            tx.commit()
            con.close()

    def test_insertWithAssertNodePropertiesType(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    1,
                    1)

            self.assertEqual(e.exception.args[0], 'Need dict() nodeProperties')

            tx.commit()

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    1,
                    "{a:b}")

            self.assertEqual(e.exception.args[0], 'Need dict() nodeProperties')

            tx.commit()

            con.close()

    def test_insertWithAssertProbability(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    "1",
                    1,
                    {"name":"Alice"})

            tx.commit()

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    1.5,
                    1,
                    {"name":"Alice"})

            self.assertEqual(e.exception.args[0], 'Probability must be in ]0.0, 1.0]')

            tx.commit()

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            tx.addNewNode(
                'Person',
                datetime.date(2015, 1, 4),
                datetime.date(2015, 1, 4),
                datetime.date(2015, 2, 2),
                datetime.date(2015, 2, 2),
                1,
                1,
                {"name":"Alice"})

            tx.commit()

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.0,
                    1,
                    {"name":"Alice"})

            self.assertEqual(e.exception.args[0], 'Probability must be in ]0.0, 1.0]')

            tx.commit()

            con.close()





    def test_insertWithAssertConflictRight(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 3),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 3),
                    datetime.date(2015, 2, 3),
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

            self.assertEqual(e.exception.args[0], "Conflict detected")

            tx.commit()
            con.close()

    def test_insertWithAssertConflictLeft(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 3),
                    datetime.date(2015, 1, 3),
                    datetime.date(2015, 2, 3),
                    datetime.date(2015, 2, 3),
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

            self.assertEqual(e.exception.args[0], "Conflict detected")

            tx.commit()
            con.close()

    def test_insertWithAssertConflictSameDate(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

            self.assertEqual(e.exception.args[0], "Conflict detected")

            tx.commit()
            con.close()

    def test_insertWithAssertConflictInterior(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 2, 5),
                    datetime.date(2015, 2, 5),
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

            self.assertEqual(e.exception.args[0], "Conflict detected")

            tx.commit()
            con.close()

    def test_insertWithAssertConflictExterior(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 2, 5),
                    datetime.date(2015, 2, 5),
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

                tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

            self.assertEqual(e.exception.args[0], "Conflict detected")

            tx.commit()
            con.close()


if __name__ == '__main__':
    unittest.main()
