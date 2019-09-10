

import Transaction
import unittest
import sqlite3, datetime

class TestInsertDBAssertRelations(unittest.TestCase):

    def test_insertWithAssertDate(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                resultInsert1 = tx.addNewNode(
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

                resultInsert2 = tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    2,
                    {
                        'name': 'Alice'
                    })

                tx.addNewRelation('Friend',
                                  datetime.date(2015, 1, 2),
                                  datetime.date(2015, 1, 4),
                                  datetime.date(2015, 1, 1),
                                  datetime.date(2015, 1, 1),
                                  1.0,
                                  {"degre d'amitie":"5"},
                                  resultInsert1[0],
                                  resultInsert2[0])

            self.assertEqual(e.exception.args[0], 'knowledgeBeginBegin < knowledgeBeginEnd < knowledgeEndEnd')

            tx.commit()
            con.close()

    def test_insertWithAssertTypeDateBB(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                resultInsert1 = tx.addNewNode(
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

                resultInsert2 = tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    2,
                    {
                        'name': 'Alice'
                    })

                tx.addNewRelation('Friend',
                                  "2015-1-4",
                                  datetime.date(2015, 1, 4),
                                  datetime.date(2015, 1, 1),
                                  datetime.date(2015, 1, 1),
                                  1.0,
                                  {"degre d'amitie":"5"},
                                  resultInsert1[0],
                                  resultInsert2[0])

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
                resultInsert1 = tx.addNewNode(
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

                resultInsert2 = tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    2,
                    {
                        'name': 'Alice'
                    })

                tx.addNewRelation('Friend',
                                  datetime.date(2015, 1, 4),
                                  "2015-1-4",
                                  datetime.date(2015, 1, 1),
                                  datetime.date(2015, 1, 1),
                                  1.0,
                                  {"degre d'amitie":"5"},
                                  resultInsert1[0],
                                  resultInsert2[0])

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
                resultInsert1 = tx.addNewNode(
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

                resultInsert2 = tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    2,
                    {
                        'name': 'Alice'
                    })

                tx.addNewRelation('Friend',
                                  datetime.date(2015, 1, 4),
                                  datetime.date(2015, 1, 1),
                                  "2015-1-4",
                                  datetime.date(2015, 1, 1),
                                  1.0,
                                  {"degre d'amitie":"5"},
                                  resultInsert1[0],
                                  resultInsert2[0])

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
                resultInsert1 = tx.addNewNode(
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

                resultInsert2 = tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    2,
                    {
                        'name': 'Alice'
                    })

                tx.addNewRelation('Friend',
                                  datetime.date(2015, 1, 4),
                                  datetime.date(2015, 1, 1),
                                  datetime.date(2015, 1, 1),
                                  "2015-1-4",
                                  1.0,
                                  {"degre d'amitie":"5"},
                                  resultInsert1[0],
                                  resultInsert2[0])

            self.assertEqual(e.exception.args[0], 'Need knowledgeEndEnd datetime.date')

            tx.commit()
            con.close()

    def test_insertWithAssertUntemporal(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                resultInsert1 = tx.addNewNode(
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

                resultInsert2 = tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    2,
                    {
                        'name': 'Alice'
                    })

                tx.addNewRelation('Friend',
                                  None,
                                  None,
                                  datetime.date(2015, 2, 2),
                                  datetime.date(2015, 2, 2),
                                  1.0,
                                  {"degre d'amitie":"5"},
                                  resultInsert1[0],
                                  resultInsert2[0])

            self.assertEqual(e.exception.args[0], 'Need knowledgeEndEnd None (to create timeless relation) or knowledgeBeginBegin not None')

            tx.commit()
            con.close()

    def test_insertWithAssertUntemporal2(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                resultInsert1 = tx.addNewNode(
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

                resultInsert2 = tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    2,
                    {
                        'name': 'Alice'
                    })

                tx.addNewRelation('Friend',
                                  datetime.date(2015, 2, 2),
                                  datetime.date(2015, 2, 2),
                                  None,
                                  None,
                                  1.0,
                                  {"degre d'amitie":"5"},
                                  resultInsert1[0],
                                  resultInsert2[0])

            self.assertEqual(e.exception.args[0], 'Need knowledgeEndEnd datetime.date or knowledgeBeginBegin must be None')

            tx.commit()
            con.close()

    def test_insertWithAssertUntemporal3(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            resultInsert1 = tx.addNewNode(
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

            resultInsert2 = tx.addNewNode(
                'Person',
                datetime.date(2015, 1, 2),
                datetime.date(2015, 1, 4),
                datetime.date(2015, 2, 2),
                datetime.date(2015, 2, 2),
                0.5,
                2,
                {
                    'name': 'Alice'
                })

            tx.addNewRelation('Friend',
                              None,
                              None,
                              None,
                              None,
                              1.0,
                              {"degre d'amitie":"5"},
                              resultInsert1[0],
                              resultInsert2[0])

            tx.commit()
            con.close()


    def test_insertWithAssertProbability2(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                resultInsert1 = tx.addNewNode(
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

                resultInsert2 = tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    2,
                    {
                        'name': 'Alice'
                    })

                tx.addNewRelation('Friend',
                                  datetime.date(2015, 2, 2),
                                  datetime.date(2015, 2, 2),
                                  datetime.date(2015, 2, 3),
                                  datetime.date(2015, 2, 3),
                                  0.0,
                                  {"degre d'amitie":"5"},
                                  resultInsert1[0],
                                  resultInsert2[0])

            self.assertEqual(e.exception.args[0], 'Probability must be in ]0.0, 1.0]')

            tx.commit()
            con.close()

    def test_insertWithAssertProbability3(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                resultInsert1 = tx.addNewNode(
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

                resultInsert2 = tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    2,
                    {
                        'name': 'Alice'
                    })

                tx.addNewRelation('Friend',
                                  datetime.date(2015, 2, 2),
                                  datetime.date(2015, 2, 2),
                                  datetime.date(2015, 2, 3),
                                  datetime.date(2015, 2, 3),
                                  2.0,
                                  {"degre d'amitie":"5"},
                                  resultInsert1[0],
                                  resultInsert2[0])

            self.assertEqual(e.exception.args[0], 'Probability must be in ]0.0, 1.0]')

            tx.commit()
            con.close()



    def test_insertWithAssertNode2NotExist(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                resultInsert1 = tx.addNewNode(
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

                tx.addNewRelation('Friend',
                                  datetime.date(2015, 2, 2),
                                  datetime.date(2015, 2, 2),
                                  datetime.date(2015, 2, 3),
                                  datetime.date(2015, 2, 3),
                                  1,
                                  {"degre d'amitie": "5"},
                                  resultInsert1[0],
                                  58)

            self.assertEqual(e.exception.args[0], 'Node 2 doesn\'t exist')

            tx.commit()
            con.close()

    def test_insertWithAssertNode1NotExist(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                resultInsert1 = tx.addNewNode(
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

                tx.addNewRelation('Friend',
                                  datetime.date(2015, 2, 2),
                                  datetime.date(2015, 2, 2),
                                  datetime.date(2015, 2, 3),
                                  datetime.date(2015, 2, 3),
                                  1,
                                  {"degre d'amitie":"5"},
                                  58,
                                  resultInsert1[0])

            self.assertEqual(e.exception.args[0], 'Node 1 doesn\'t exist')

            tx.commit()
            con.close()

    def test_insertWithAssertDateNode(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                resultInsert1 = tx.addNewNode(
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

                resultInsert2 = tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    2,
                    {
                        'name': 'Alice'
                    })

                tx.addNewRelation('Friend',
                                  datetime.date(2014, 2, 2),
                                  datetime.date(2014, 2, 2),
                                  datetime.date(2015, 2, 3),
                                  datetime.date(2015, 2, 3),
                                  1,
                                  {"degre d'amitie":"5"},
                                  resultInsert1[0],
                                  resultInsert2[0])

            self.assertEqual(e.exception.args[0], "Impossible to add this relation, begin date is before the begin date of node 2")

            tx.commit()
            con.close()

    def test_insertWithAssertDateNode2(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                resultInsert1 = tx.addNewNode(
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

                resultInsert2 = tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    2,
                    {
                        'name': 'Alice'
                    })

                tx.addNewRelation('Friend',
                                  datetime.date(2014, 2, 2),
                                  datetime.date(2014, 2, 2),
                                  datetime.date(2015, 2, 3),
                                  datetime.date(2015, 2, 3),
                                  1,
                                  {"degre d'amitie":"5"},
                                  resultInsert1[0],
                                  resultInsert2[0])

            self.assertEqual(e.exception.args[0], "Impossible to add this relation, begin date is before the begin date of node 1")

            tx.commit()
            con.close()

    def test_insertWithAssertDateNode3(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                resultInsert1 = tx.addNewNode(
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

                resultInsert2 = tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    2,
                    {
                        'name': 'Alice'
                    })

                tx.addNewRelation('Friend',
                                  datetime.date(2015, 2, 2),
                                  datetime.date(2015, 2, 2),
                                  datetime.date(2016, 2, 3),
                                  datetime.date(2016, 2, 3),
                                  1,
                                  {"degre d'amitie":"5"},
                                  resultInsert1[0],
                                  resultInsert2[0])

            self.assertEqual(e.exception.args[0], "Impossible to add this relation, end date is before the begin date of node 2")

            tx.commit()
            con.close()

    def test_insertWithAssertDateNode4(self):
        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            with self.assertRaises(AssertionError) as e:
                resultInsert1 = tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 1, 2),
                    datetime.date(2015, 2, 3),
                    datetime.date(2015, 2, 3),
                    0.5,
                    1,
                    {
                        'name': 'Alice'
                    })

                resultInsert2 = tx.addNewNode(
                    'Person',
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 1, 4),
                    datetime.date(2015, 2, 2),
                    datetime.date(2015, 2, 2),
                    0.5,
                    2,
                    {
                        'name': 'Alice'
                    })

                tx.addNewRelation('Friend',
                                  datetime.date(2015, 2, 2),
                                  datetime.date(2015, 2, 2),
                                  datetime.date(2016, 2, 3),
                                  datetime.date(2016, 2, 3),
                                  1,
                                  {"degre d'amitie":"5"},
                                  resultInsert1[0],
                                  resultInsert2[0])

            self.assertEqual(e.exception.args[0], "Impossible to add this relation, end date is before the begin date of node 1")

            tx.commit()
            con.close()

if __name__ == '__main__':
    unittest.main()