

import Transaction
import unittest
from neo4j import GraphDatabase
import sqlite3, datetime
from ConceptualModel import *


class TestInsertDB(unittest.TestCase):

    def test_ResetDatabase(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None
            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()
            tx.commit()
            self.assertEqual(tx._c.execute('select * from NodesData').fetchall().__len__(), 0)
            con.close()

    def test_insertWithoutAnyConflictGoodID(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            idNode = tx.addNewNode(
                'Person',
                datetime.date(2015, 1, 2),
                datetime.date(2015, 1, 4),
                datetime.date(2015, 2, 2),
                datetime.date(2015, 2, 4),
                0.5,
                1,
                {
                    'name': 'Alice'
                })

            a=tx._tx.run('match (n) return n').single()[0]

            self.assertEqual(tx._tx.run('match (n) return n').single()[0].id, idNode[0])

            tx.commit()
            con.close()

    def test_insertWithoutAnyConflict(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            tx.addNewNode(
                'Person',
                datetime.date(2015, 1, 2),
                datetime.date(2015, 1, 4),
                datetime.date(2015, 2, 2),
                datetime.date(2015, 2, 4),
                0.5,
                1,
                {
                    'name': 'Alice'
                })

            self.assertEqual(tx._c.execute('select * from NodesData').fetchall().__len__(), 1)
            self.assertEqual(tx._tx.run('match (n) return count(n)').single()[0], 1)

            tx.addNewNode(
                'Person',
                datetime.date(2015, 2, 6),
                datetime.date(2015, 2, 6),
                datetime.date(2015, 2, 7),
                datetime.date(2015, 2, 7),
                0.5,
                2,
                {
                    'name': 'Bob'
                })

            self.assertEqual(tx._tx.run('match (n) return count(n)').single()[0], 2)

            tx.commit()
            con.close()




    def test_insertCreateBlankInterval1(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            tx.addNewNode(
                'Person',
                datetime.date(2015, 1, 2),
                datetime.date(2015, 1, 4),
                datetime.date(2015, 2, 2),
                datetime.date(2015, 2, 4),
                0.5,
                1,
                {
                    'name': 'Alice'
                })

            tx.addNewNode(
                'Person',
                datetime.date(2015, 3, 3),
                datetime.date(2015, 3, 3),
                datetime.date(2015, 4, 4),
                datetime.date(2015, 4, 4),
                0.5,
                1,
                {
                    'name': 'Alice',
                    'number': '1'
                })

            allNodes = tx._c.execute('select * from NodesData').fetchall()




            self.assertEqual(allNodes[0][2], datetime.date(2015, 1, 2))
            self.assertEqual(allNodes[0][3], datetime.date(2015, 1, 4))
            self.assertEqual(allNodes[0][4], datetime.date(2015, 2, 2))
            self.assertEqual(allNodes[0][5], datetime.date(2015, 2, 4))
            self.assertEqual(allNodes[0][6], datetime.date.today())
            self.assertEqual(allNodes[0][7], None)

            self.assertEqual(allNodes[1][2], datetime.date(2015, 3, 3))
            self.assertEqual(allNodes[1][3], datetime.date(2015, 3, 3))
            self.assertEqual(allNodes[1][4], datetime.date(2015, 4, 4))
            self.assertEqual(allNodes[1][5], datetime.date(2015, 4, 4))
            self.assertEqual(allNodes[1][6], datetime.date.today())
            self.assertEqual(allNodes[1][7], None)

            self.assertEqual(allNodes[2][2], datetime.date(2015, 2, 4))
            self.assertEqual(allNodes[2][3], datetime.date(2015, 2, 4))
            self.assertEqual(allNodes[2][4], datetime.date(2015, 3, 3))
            self.assertEqual(allNodes[2][5], datetime.date(2015, 3, 3))
            self.assertEqual(allNodes[2][6], datetime.date.today())
            self.assertEqual(allNodes[2][7], None)

            tx.commit()
            con.close()


if __name__ == '__main__':
    unittest.main()


