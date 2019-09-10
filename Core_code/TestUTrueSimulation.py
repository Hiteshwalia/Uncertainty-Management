import json

import Transaction
import unittest
import sqlite3, datetime

class TestTrueSimulation(unittest.TestCase):

    def test_trueSimulation(self):

        with Transaction.driver.session() as session:
            con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
            con.isolation_level = None

            tx = Transaction.Transaction(session, con)

            tx.resetDatabase()

            nodeNapoleon = tx.addNewNode(
                'Person',
                datetime.date(1815, 3, 20),
                datetime.date(1815, 3, 20),
                datetime.date(1815, 6, 22),
                datetime.date(1815, 6, 22),
                1.0,
                1,
                {
                    'name': 'Napoleon 1er',
                    'Function': 'French Emperor'
                })

            nodeWellsley = tx.addNewNode(
                'Person',
                datetime.date(1769, 5, 1),
                datetime.date(1769, 5, 1),
                datetime.date(1852, 3, 2),
                datetime.date(1852, 3, 2),
                1.0,
                2,
                {
                    'name': 'Arthur Wellesley',
                    'Function': 'first duc of Wellington'
                })

            nodeBeauharnais = tx.addNewNode(
                'Person',
                datetime.date(1796, 3, 9),
                datetime.date(1796, 3, 9),
                datetime.date(1809, 12, 16),
                datetime.date(1809, 12, 16),
                1.0,
                3,
                {
                    'name': 'Josephine Beauharnais'
                })

            nodeMLAutriche = tx.addNewNode(
                'Person',
                datetime.date(1810, 4, 2),
                datetime.date(1810, 4, 2),
                datetime.date(1847, 12, 17),
                datetime.date(1847, 12, 17),
                1.0,
                4,
                {
                    'name': 'Marie-Louise d’Autriche',
                    'function': 'Archduchesse'
                })

            nodeWaterloo = tx.addNewNode(
                'Battle',
                datetime.date(1815, 6, 18),
                datetime.date(1815, 6, 18),
                datetime.date(1815, 6, 19),
                datetime.date(1815, 6, 19),
                1.0,
                5,
                {"name":"Waterloo",
                 "fantassins français:": "74000",
                 "cavaliers français": "12600",
                 "Canons français": "266",
                 "fantassins des alliers:": "68000",
                 "cavaliers des alliers": "12000",
                 "Canons des alliers": "156"
                 })

            tx.addNewRelation('Defeat',
                              datetime.date(1815, 6, 18),
                              datetime.date(1815, 6, 18),
                              datetime.date(1815, 6, 19),
                              datetime.date(1815, 6, 19),
                              1.0,
                              {"Résultat":"Napoleon Exilé"},
                              nodeNapoleon[0],
                              nodeWaterloo[0])

            tx.addNewRelation('Victory',
                              datetime.date(1815, 6, 18),
                              datetime.date(1815, 6, 18),
                              datetime.date(1815, 6, 19),
                              datetime.date(1815, 6, 19),
                              1.0,
                              {"Resultat":"Retour auprès du roi en Héros"},
                              nodeWellsley[0],
                              nodeWaterloo[0])

            tx.addNewRelation('Maried',
                              datetime.date(1796, 3, 9),
                              datetime.date(1796, 3, 9),
                              datetime.date(1809, 12, 16),
                              datetime.date(1809, 12, 16),
                              1.0,
                              {"Raison du divorce":"Stérilité de Joséphine"},
                              nodeNapoleon[0],
                              nodeBeauharnais[0])

            tx.addNewRelation('Maried',
                              datetime.date(1810, 4, 2),
                              datetime.date(1810, 4, 2),
                              datetime.date(1847, 12, 17),
                              datetime.date(1847, 12, 17),
                              1.0,
                                 {"Raison du divorce": "Mort de Louise-Marie"},
                              nodeNapoleon[0],
                              nodeMLAutriche[0])

            nodeTroisIlets = tx.addNewNode(
                'City',
                datetime.date(1500, 1, 1),
                datetime.date(1500, 1, 1),
                datetime.date.today(),
                datetime.date.today(),
                1.0,
                6,
                {"name": "Trois-Îlets",
                 })

            nodeMartinique = tx.addNewNode(
                'Island',
                datetime.date(1500, 1, 1),
                datetime.date(1500, 1, 1),
                datetime.date.today(),
                datetime.date.today(),
                1.0,
                7,
                {"name": "Martinique",
                 })

            nodeCorse = tx.addNewNode(
                'Island',
                datetime.date(1768, 5, 15),
                datetime.date(1768, 5, 15),
                datetime.date.today(),
                datetime.date.today(),
                1.0,
                8,
                {"name": "Corse",
                 })

            nodeFrance = tx.addNewNode(
                'Country',
                datetime.date(476,1,1),
                datetime.date(476,1,1),
                datetime.date.today(),
                datetime.date.today(),
                1.0,
                9,
                {"name": "France",
                 })

            nodeRomanEmpire = tx.addNewNode(
                'Country',
                datetime.date(476, 1, 1),
                datetime.date(476, 1, 1),
                datetime.date.today(),
                datetime.date.today(),
                1.0,
                10,
                {"name": "Empire romain d'occident",
                 })

            tx.addNewRelation('Belongs',
                              None,
                              None,
                              None,
                              None,
                              1.0,
                              {"_": "_"},
                              nodeTroisIlets[0],
                              nodeMartinique[0])

            tx.addNewRelation('Belongs',
                              None,
                              None,
                              None,
                              None,
                              1.0,
                              {"_": "_"},
                              nodeMartinique[0],
                              nodeFrance[0])

            tx.addNewRelation('Belongs',
                              None,
                              None,
                              None,
                              None,
                              1.0,
                              {"_": "_"},
                              nodeCorse[0],
                              nodeFrance[0])


            tx.addNewRelation('IsBorn',
                              datetime.date(1796, 3, 9),
                              datetime.date(1796, 3, 9),
                              datetime.date(1796, 3, 9),
                              datetime.date(1796, 3, 9),
                              1.0,
                              {"_": "_"},
                              nodeBeauharnais[0],
                              nodeTroisIlets[0])

            tx.createMutualExclusion(nodeFrance[0], nodeRomanEmpire[0])


            # print(json.dumps(tx.extractLogicalModel(), indent=2, ensure_ascii=False))
            reel = {'Labels(Σ)': ['Friend', 'Known', 'Kill', 'HasChild', 'Maried', 'Victory', 'Defeat'], 'Entities(U)': [1, 2, 3, 4, 5], 'Nodes(V)': [45, 34, 27, 26, 29], 'Relations(E)': [-33, -34, -32, -36], 'V->U(μ)': {45: 1, 34: 2, 27: 3, 26: 4, 29: 5}, 'Ω->2^P(π)': {'-36': [('Raison du divorce', 'Mort de Louise-Marie')], '-32': [('Raison du divorce', 'Stérilité de Joséphine')], '-33': [('Résultat', 'Napoleon Exilé')], '-34': [('Resultat', 'Retour auprès du roi en Héros')], '26': [('name', 'Marie-Louise d’Autriche'), ('function', 'Archduchesse')], '27': [('name', 'Josephine Beauharnais')], '29': [('cavaliers français', '12600'), ('Canons des alliers', '156'), ('name', 'Waterloo'), ('cavaliers des alliers', '12000'), ('fantassins français:', '74000'), ('fantassins des alliers:', '68000'), ('Canons français', '266')], '34': [('name', 'Arthur Wellesley'), ('Function', 'first duc of Wellington')], '45': [('name', 'Napoleon 1er'), ('Function', 'French Emperor')]}, 'E->V*Σ*V(ρ)': {'33': [34, 'Victory', 29], '32': [45, 'Defeat', 29], '35': [45, 'Maried', 26], '31': [45, 'Maried', 27]}, 'Ω->TransactionTime(τ)': {'45': '2019-01-25', '34': '2019-01-25', '27': '2019-01-25', '26': '2019-01-25', '29': '2019-01-25', '-33': '2019-01-25', '-34': '2019-01-25', '-32': '2019-01-25', '-36': '2019-01-25'}, 'Ω->ValidTime(υ)': {'45': ['1815-03-20', '1815-03-20', '1815-06-22', '1815-06-22'], '34': ['1769-05-01', '1769-05-01', '1852-03-02', '1852-03-02'], '27': ['1796-03-09', '1796-03-09', '1809-12-16', '1809-12-16'], '26': ['1810-04-02', '1810-04-02', '1847-12-17', '1847-12-17'], '29': ['1815-06-18', '1815-06-18', '1815-06-19', '1815-06-19'], '-33': ['1815-06-18', '1815-06-18', '1815-06-19', '1815-06-19'], '-34': ['1815-06-18', '1815-06-18', '1815-06-19', '1815-06-19'], '-32': ['1796-03-09', '1796-03-09', '1809-12-16', '1809-12-16'], '-36': ['1810-04-02', '1810-04-02', '1847-12-17', '1847-12-17']}, 'Ω->Probability(w)': {'45': 1.0, '34': 1.0, '27': 1.0, '26': 1.0, '29': 1.0, '-33': 1.0, '-34': 1.0, '-32': 1.0, '-36': 1.0}}
            extractModal= tx.extractLogicalModel()
            print(extractModal)
            print(type(extractModal))
            print(extractModal == reel)
            #self.assertEqual(extractModal,reel)

            print(tx._c.execute("Select * from NodesData").fetchall())

            tx.commit()
            con.close()