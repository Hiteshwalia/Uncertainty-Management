import Transaction
import unittest
import sqlite3, datetime

class TestAssertMutexMutex(unittest.TestCase):

    def test_mutexNode2NotExist(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()
            with self.assertRaises(AssertionError) as e:
                node1 = tx.addNewNode(
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

                tx.createMutualExclusion(node1[0], 58)

            self.assertEqual(e.exception.args[0], "Your object 2 must be exist")

            tx.commit()
            con.close()


    def test_mutexNode1NotExist(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()
            with self.assertRaises(AssertionError) as e:
                node1 = tx.addNewNode(
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

                tx.createMutualExclusion(58, node1)

            self.assertEqual(e.exception.args[0], "Your object 1 must be exist")

            tx.commit()
            con.close()


    def test_mutexSameObjectNotExist(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()
            with self.assertRaises(AssertionError) as e:
                node1 = tx.addNewNode(
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

                tx.createMutualExclusion(node1, node1)

            self.assertEqual(e.exception.args[0], "Your 2 objects must be differents")

            tx.commit()
            con.close()

