from functools import reduce

from neo4j import GraphDatabase
import sqlite3, datetime
from ConceptualModel import *

sqlite3.register_adapter(bool, int)
sqlite3.register_converter("BOOLEAN", lambda v: bool(int(v)))

uri = 'bolt://localhost:7687'
driver = GraphDatabase.driver(uri, auth=('neo4j', 'admin'))


class Transaction(object):

    def __init__(self, session, sqliteCon):
        #   Neo4J
        self._session = session
        self._tx = session.begin_transaction()
        #   SQLite3
        self._sqliteCon = sqliteCon
        self._c = self._sqliteCon.cursor()


    def resetDatabase(self):
        """
            Delete and recreate the databases
        """
        self._tx.run('MATCH(N) DETACH DELETE(N)')
        self._c.execute('DROP TABLE IF EXISTS NodesData')
        self._c.execute('DROP TABLE IF EXISTS LinksData')
        self._c.execute('DROP TABLE IF EXISTS mutualExclusion')
        self._c.execute('CREATE TABLE NodesData('
                        'nodeID INT,'
                        'entityID INT,'
                        'knowledgeBeginBegin DATE,'
                        'knowledgeBeginEnd DATE,'
                        'knowledgeEndBegin DATE,'
                        'knowledgeEndEnd DATE,'
                        'transactionBegin DATE,'
                        'transactionEnd DATE,'
                        'probability FLOAT)')
        self._c.execute('CREATE TABLE LinksData('
                        'linkID INT,'
                        'knowledgeBeginBegin DATE,'
                        'knowledgeBeginEnd DATE,'
                        'knowledgeEndBegin DATE,'
                        'knowledgeEndEnd DATE,'
                        'transactionBegin DATE,'
                        'transactionEnd DATE,'
                        'probability FLOAT)')
        self._c.execute('CREATE TABLE mutualExclusion('
                      'id1 INT,'
                      'id2 INT,'
                      'PRIMARY KEY(id1, id2))')


    def extractLogicalModel(self):
        """
            Extract the model to remplace all information by all alphabet
        :return: return the model
        """
        return {
            'Σv: NodesLabels': NODE_TYPES,
            'Σe: EdgesLabels': RELATION_TYPES,
            'U: Entities': list(map(lambda t: t[0], self._c.execute('SELECT DISTINCT entityID FROM NodesData'))),
            'V: Nodes': list(map(lambda t: t[0], self._c.execute('SELECT nodeID FROM NodesData'))),
            'μ: V->U': {v: u for v, u in list(self._c.execute('SELECT nodeID, entityID FROM NodesData'))},
            'λv: V->Σv': {
                str(t[0]): t[1][0]
                for t in self._tx.run(
                    'MATCH (n)'
                    'RETURN ID(n), LABELS(n)'
                ).values()
            },
            'E: Relations': list(map(lambda t: t[0], self._c.execute('SELECT linkID FROM LinksData'))),
            '◁: E->V': {
                str(t[0]): t[1]
                for t in self._tx.run(
                    'MATCH (n1)-[r]->(n2) '
                    'RETURN ID(r), ID(n1)'
                ).values()
            },
            '▷: E->V': {
                str(t[0]): t[1]
                for t in self._tx.run(
                    'MATCH (n1)-[r]->(n2) '
                    'RETURN ID(r), ID(n2)'
                ).values()
            },
            'λe: V->Σe': {
                str(t[0]): t[1]
                for t in self._tx.run(
                    'MATCH (n1)-[r]->(n2)'
                    'RETURN ID(r), TYPE(r)'
                ).values()
            },
            'π: Ω->2^P': reduce(
                lambda obj, t: dict(**obj, **{
                    str(t[0]._id): [(k, v) for k, v in t[0]._properties.items()]
                }),
                self._tx.run(
                    'MATCH (n)'
                    'RETURN n'
                ).values(),
                reduce(
                    lambda obj, t: dict(**obj, **{
                        str(-t[0]._id - 1): [(k, v) for k, v in t[0]._properties.items()]
                    }),
                    self._tx.run(
                        'MATCH ()-[r]->() '
                        'RETURN r'
                    ).values(),
                    {}
                )
            ),
            'τt: Ω->I(T)': {
                **{
                    str(t[0]): str(t[1])
                    for t in
                self._c.execute('SELECT nodeID, COALESCE(transactionEnd,transactionBegin) from NodesData')
                },
                **{
                    str(t[0]): str(t[1])
                    for t in
                self._c.execute('SELECT linkID, COALESCE(transactionEnd,transactionBegin) from LinksData')
                }
            },
            'τv: Ω->I(T)×I(T)': {
                **{
                    str(t[0]): list(map(lambda x: str(x), t[1:]))
                    for t in self._c.execute(
                    'SELECT nodeID, knowledgeBeginBegin, knowledgeBeginEnd, knowledgeEndBegin, knowledgeEndEnd from NodesData')
                },
                **{
                    str(t[0]): list(map(lambda x: str(x), t[1:]))
                    for t in self._c.execute(
                    'SELECT linkID, knowledgeBeginBegin, knowledgeBeginEnd, knowledgeEndBegin, knowledgeEndEnd from LinksData')
                }
            },
            'w: Ω->[0-1]': {
                **{
                    str(t[0]): t[1]
                    for t in self._c.execute(
                    'SELECT nodeID, probability from NodesData')
                },
                **{
                    str(t[0]): t[1]
                    for t in self._c.execute(
                    'SELECT linkID, probability from LinksData')
                }
            },
            '⊕: Ω×Ω': list(self._c.execute('SELECT * from mutualExclusion').fetchall())
        }

    def addNewNode(self, nodeType,
                   knowledgeBeginBegin, knowledgeBeginEnd,
                   knowledgeEndBegin, knowledgeEndEnd,
                   probability,
                   entityId,
                   nodeProperties, conflictAccept=False):
        """
        Add new node in the model
        :param nodeType: String in array in conceptual model
        :param knowledgeBeginBegin: Date begin begin interval
        :param knowledgeBeginEnd: Date end begin interval
        :param knowledgeEndBegin: Date begin end interval
        :param knowledgeEndEnd: Date end end interval
        :param probability: Float in ]0,1]
        :param entityId: The ID of entity (example, Napoleon : 1, Ramses II : 2)
        :param nodeProperties: Properties in this node (python dict)
        :param conflictAccept: Optionnal boolean to accept or not the conflict
        :return: The node, created in neo4j
        """


        assert nodeType in NODE_TYPES, 'Invalid nodeType'
        assert type(nodeProperties) == dict, 'Need dict() nodeProperties'
        assert type(knowledgeBeginBegin) == datetime.date, 'Need knowledgeBeginBegin datetime.date'
        assert type(knowledgeBeginEnd) == datetime.date, 'Need knowledgeBeginEnd datetime.date'
        assert type(knowledgeEndBegin) == datetime.date, 'Need knowledgeEndBegin datetime.date'
        assert type(knowledgeEndEnd) == datetime.date, 'Need knowledgeEndEnd datetime.date'
        assert type(probability) == float or type(probability) == int, 'Probability must be a number'
        assert knowledgeBeginBegin <= knowledgeBeginEnd < knowledgeEndEnd, 'knowledgeBeginBegin <= knowledgeBeginEnd < knowledgeEndEnd'
        assert knowledgeBeginBegin < knowledgeEndBegin <= knowledgeEndEnd, 'knowledgeBeginBegin < knowledgeEndBegin <= knowledgeEndEnd'
        if type(probability) == float:
            assert 0.0 < probability <= 1.0, 'Probability must be in ]0.0, 1.0]'
        if type(probability) == int:
            assert 0 < probability <= 1, 'Probability must be in ]0.0, 1.0]'
            probability = float(probability)


        result = self.separateOtherIfNecessary(entityId, knowledgeBeginBegin, knowledgeBeginEnd, knowledgeEndBegin, knowledgeEndEnd,
                                      probability, nodeProperties, nodeType, conflictAccept)




        return result

    def separateOtherIfNecessary(self,
                                 entityId,
                                 knowledgeBeginBegin, knowledgeBeginEnd,
                                 knowledgeEndBegin, knowledgeEndEnd, probability, nodeProperties, nodeType, conflictAccept=False):
        """
        Create the new node and add mutual exclusion if conflictAccept=True
        :param nodeType: String in array in conceptual model
        :param knowledgeBeginBegin: Date begin begin interval
        :param knowledgeBeginEnd: Date end begin interval
        :param knowledgeEndBegin: Date begin end interval
        :param knowledgeEndEnd: Date end end interval
        :param probability: Float in ]0,1]
        :param entityId: The ID of entity (example, Napoleon : 1, Ramses II : 2)
        :param nodeProperties: Properties in this node (python dict)
        :param conflictAccept: Optionnal boolean to accept or not the conflict
        :return: New node and array of mutual exclsion (if not None)
        """


        # Intérior
        resultInterior = self._c.execute('select * from NodesData where entityID = ? and knowledgeBeginBegin < ? and knowledgeEndEnd > ?'
                                        ' and transactionEnd is Null'
                                 , (entityId,knowledgeBeginBegin,knowledgeEndEnd)).fetchall()
        if conflictAccept == False:
            assert resultInterior.__len__() == 0, "Conflict detected"

        # Exterior
        resultExterior = self._c.execute('select * from NodesData where entityID = ? and knowledgeBeginBegin > ? and knowledgeEndEnd < ?'
                                        ' and transactionEnd is Null'
                                 , (entityId,knowledgeBeginBegin,knowledgeEndEnd)).fetchall()

        if conflictAccept == False:
            assert resultExterior.__len__() == 0, "Conflict detected"

        #Intersect on left
        resultIntersectOnLeft = self._c.execute('select * from NodesData where entityID = ? and knowledgeBeginBegin > ? and knowledgeEndEnd > ?'
                                                ' and transactionEnd is Null'
                                                ' and knowledgeBeginBegin < ? '
                                 , (entityId,knowledgeBeginBegin,knowledgeEndEnd, knowledgeEndEnd)).fetchall()

        if conflictAccept == False:
            assert resultIntersectOnLeft.__len__() == 0, "Conflict detected"

        #Intersect on right
        resultIntersectOnRight = self._c.execute('select * from NodesData where entityID = ? and knowledgeBeginBegin < ? and knowledgeEndEnd < ?'
                                                    ' and transactionEnd is NUll'
                                                    ' and knowledgeEndEnd > ?'
                        , (entityId, knowledgeBeginBegin, knowledgeEndEnd, knowledgeBeginBegin)).fetchall()

        if conflictAccept == False:
            assert resultIntersectOnRight.__len__() == 0, "Conflict detected"

        # Intersect on right
        resultIntersectSameDate = self._c.execute(
            'select * from NodesData where entityID = ? and knowledgeBeginBegin = ? and knowledgeEndEnd = ?'
            ' and transactionEnd is NUll'
            ' and knowledgeBeginEnd = ? and  knowledgeEndBegin = ?'
            , (entityId, knowledgeBeginBegin, knowledgeEndEnd, knowledgeBeginEnd, knowledgeEndBegin)).fetchall()

        if conflictAccept == False:
            assert resultIntersectSameDate.__len__() == 0, "Conflict detected"


        result = self._tx.run(
            'CREATE (entry:{} $properties) RETURN id(entry)'.format(nodeType), properties=nodeProperties
        ).single()

        assert result, 'Error when inserting new node, result is empty'

        allMutualExclusion = []

        self._c.execute('INSERT INTO NodesData VALUES (?,?,?,?,?,?,?,?,?)',
                        (
                            result.value(),
                            entityId,
                            knowledgeBeginBegin, knowledgeBeginEnd,
                            knowledgeEndBegin, knowledgeEndEnd,
                            datetime.date.today(),
                            None,
                            probability
                        ))

        if conflictAccept:
            for resultOld in resultInterior, resultExterior, resultIntersectOnLeft, resultIntersectOnRight:
                if resultOld != []:
                    allMutualExclusion.append(self.createMutualExclusion(result.value(), resultOld[0][0]))

        newNodeInformation = [entityId, knowledgeBeginBegin, knowledgeBeginEnd, knowledgeEndBegin,
                                              knowledgeEndEnd, probability, nodeProperties]



        resultsAdd = self.verificationBlanckInterval(result.value(), entityId, knowledgeBeginBegin, knowledgeEndEnd)

        if not conflictAccept:
            return [*[result.value()], *resultsAdd]
        else:
            return [* [result.value()], *resultsAdd], allMutualExclusion

    def verificationMutexAlreadyExist(self, objectID1, objectID2):
        """
        Show if a specific mutual exclusion already exist
        :param objectID1: Id of object 1
        :param objectID2: Id of object 2
        :return: True if exist and False if not exist
        """
        result = self._c.execute("SELECT * from mutualExclusion where (id1 = ? and id2 = ?) or (id1 = ? and id2 = ?)",
                        (objectID1, objectID2, objectID2, objectID1)).fetchall()

        return result.__len__() > 0

    def researchMutex(self, idNode, idRelation):
        """
        Search all mutux with an array of node and array of relation
        :param idNode: Array of nodes
        :param idRelation: Array of Relation
        :return: All mutual exclusion that contain this Node or relation
        """
        result = []
        
        if idNode.__len__() == 1:
            idNode.append(0)
        if idRelation.__len__() == 1:
            idRelation.append(0)

        for id in idNode:

            resultTemp = (self._c.execute(
                "SELECT * from mutualExclusion where (id1 = ? and id2 in "+
                str(tuple(idNode)) + ") or (id1 = ? and id2 in " + str(tuple(idRelation)) + ")",
                    (id, id))).fetchall()
            if resultTemp.__len__() > 0:
                result = [*result, *resultTemp]

        for id in idRelation:

            resultTemp = (self._c.execute(
                "SELECT * from mutualExclusion where (id1 = ? and id2 in "+
                str(tuple(idNode)) + ") or (id1 = ? and id2 in " + str(tuple(idRelation)) + ")",
                    (id, id))).fetchall()

            if resultTemp.__len__() > 0:
                result = [*result, *resultTemp]

        return result

    def createMutualExclusion(self, objectID1, objectID2):

        """
        Create a mutual exclusion
        :param objectID1: Id of object 1
        :param objectID2: Id of object 2
        :return: The tuple [objectID1, objectID2]
        """

        assert objectID1 != objectID2, "Your 2 objects must be differents"
        assert self.verifIfNodeExist(objectID1), "Your object 1 must be exist"
        assert self.verifIfNodeExist(objectID2), "Your object 2 must be exist"

        alreadyExist = self.verificationMutexAlreadyExist(objectID1, objectID2)

        assert not alreadyExist, "This mutex already exist"

        if(objectID1 > 0):
            self._c.execute("update NodesData set probability = probability/2 where nodeID=?", (objectID1,))
        else:
            self._c.execute("update LinksData set probability = probability/2 where linkID=?", (objectID1,))

        if (objectID2 > 0):
            self._c.execute("update NodesData set probability = probability/2 where nodeID=?", (objectID2,))
        else:
            self._c.execute("update LinksData set probability = probability/2 where linkID=?", (objectID2,))

        self._c.execute("insert into mutualExclusion values (?,?)", (objectID1, objectID2))

        return [objectID1, objectID2]

    def verifIfNodeExist(self, idNode):
        """
        Vification of node's existance
        :param idNode: Id node
        :return: True if not exist and False if node does'nt exist
        """
        result = self._c.execute("select * from NodesData where nodeID = ?", (str(idNode),)).fetchall()
        return result.__len__() > 0

    def getInformationOfNode(self, idNode):
        """
        Get all sql information of specific node
        :param idNode: Id of Node
        :return: All sql information
        """
        result = self._c.execute("select * from NodesData where nodeID = ?", (str(idNode),)).fetchall()
        return result

    def addNewRelation(self, relationType,
                       knowledgeBeginBegin, knowledgeBeginEnd,
                       knowledgeEndBegin, knowledgeEndEnd,
                       probability,
                       relationProperties, node1ID, node2ID):

        """
        Create a new relation
        :param relationType: String in array in conceptual model
        :param knowledgeBeginBegin: Date begin begin interval
        :param knowledgeBeginEnd: Date end begin interval
        :param knowledgeEndBegin: Date begin end interval
        :param knowledgeEndEnd: Date end end interval
        :param probability: Float in ]0,1]
        :param entityId: The ID of entity (example, Napoleon : 1, Ramses II : 2)
        :param nodeProperties: Properties in this node (python dict)
        :param node1ID: ID of start note
        :param node2ID: ID of end node
        :return: New relation ID
        """

        assert relationType in RELATION_TYPES, 'Invalid relationType'
        assert type(relationProperties) == type(dict()), 'Need dict() relationProperties'
        assert type(knowledgeBeginBegin) == datetime.date or knowledgeBeginBegin is None, 'Need knowledgeBeginBegin datetime.date'
        assert type(knowledgeBeginEnd) == datetime.date or knowledgeBeginEnd is None, 'Need knowledgeBeginEnd datetime.date'
        assert type(knowledgeEndBegin) == datetime.date or knowledgeEndBegin is None, 'Need knowledgeEndBegin datetime.date'
        assert type(knowledgeEndEnd) == datetime.date or knowledgeEndEnd is None, 'Need knowledgeEndEnd datetime.date'
        assert type(probability) == float or type(probability) == int, 'Probability must be a float number'
        if type(knowledgeBeginBegin) is not None and knowledgeEndEnd is not None \
                and knowledgeEndBegin is not None and knowledgeBeginEnd is not None:
            assert knowledgeBeginBegin <= knowledgeBeginEnd <= knowledgeEndEnd, 'knowledgeBeginBegin < knowledgeBeginEnd < knowledgeEndEnd'
            assert knowledgeBeginBegin <= knowledgeEndBegin <= knowledgeEndEnd, 'knowledgeBeginBegin < knowledgeEndBegin < knowledgeEndEnd'
        assert 0.0 < probability <= 1.0, 'Probability must be in ]0.0, 1.0]'
        if knowledgeBeginBegin is None:
            assert knowledgeEndEnd is None, "Need knowledgeEndEnd None (to create timeless relation) or knowledgeBeginBegin not None"
        else:
            assert type(knowledgeEndEnd) == datetime.date, 'Need knowledgeEndEnd datetime.date or knowledgeBeginBegin must be None'
        if type(probability) == float:
            assert 0.0 < probability <= 1.0, 'Probability must be in ]0.0, 1.0]'
        if type(probability) == int:
            assert 0 < probability <= 1, 'Probability must be in ]0.0, 1.0]'
            probability = float(probability)

        node1 = self.getInformationOfNode(node1ID)
        node2 = self.getInformationOfNode(node2ID)

        assert node1.__len__() > 0, 'Node 1 doesn\'t exist'
        assert node2.__len__() > 0, 'Node 2 doesn\'t exist'

        node1 = node1[00]
        node2 = node2[00]

        if(node1[2] < node2[2]):
            assert knowledgeBeginBegin is None or node1[2] <= knowledgeBeginBegin , \
                "Impossible to add this relation, begin date is before the begin date of node 1"
        else:
            assert knowledgeBeginBegin is None or node2[2] <= knowledgeBeginBegin, \
                "Impossible to add this relation, begin date is before the begin date of node 2"

        if node1[5] > node2[5]:
            assert knowledgeEndEnd is None or node1[5] >= knowledgeEndEnd,\
                "Impossible to add this relation, end date is before the begin date of node 1"
        else:
            assert knowledgeEndEnd is None or node2[5] >= knowledgeEndEnd,\
                "Impossible to add this relation, end date is before the begin date of node 2"


        result = self._tx.run(
            'MATCH (a),(b) '
            'WHERE ID(a) = $id1 AND ID(b) = $id2 '
            'CREATE (a)-[rel:{} $properties]->(b)'
            'RETURN id(rel)'.format(relationType), id1=node1ID, id2=node2ID, properties=relationProperties
        ).single()

        assert result, 'Error when inserting new relation, result is empty'

        self._c.execute('INSERT INTO LinksData VALUES (?,?,?,?,?,?,?,?)',
                        (
                            -result.value()-1,
                            knowledgeBeginBegin, knowledgeBeginEnd,
                            knowledgeEndBegin, knowledgeEndEnd,
                            datetime.date.today(),
                            None,
                            probability
                        ))

        return -result.value()-1

    def commit(self):
        return self._tx.commit() and self._sqliteCon.commit() and self._c.close()


    def verificationBlanckInterval(self, nodeID, entityId, knowledgeBeginBegin, knowledgeEndEnd):
        """
        Check if we haven't information about an entity ID beetween 2 nodes that we have information
        :param nodeID: New Node id that we have created
        :param entityId: Entity id
        :param knowledgeBeginBegin: Date of begin
        :param knowledgeEndEnd: Date of end
        :return: An array we the new nodes
        """
        results = self._c.execute(
            'select * from NodesData where entityID = 7').fetchall()


        results = self._c.execute('select n2.knowledgeEndEnd, max(n2.knowledgeEndEnd) from NodesData n1, NodesData n2 where '
                        'n1.nodeID = ? and n2.entityID = ? and '
                        'n1.knowledgeBeginBegin > n2.knowledgeEndEnd and '
                        'n2.nodeID != n1.nodeID '
                        'group by n2.knowledgeEndEnd', (nodeID,entityId)).fetchall()

        resultAddNode1 = []
        resultAddNode2 = []

        for result in results:
            if result != []:
                resultAddNode1 = self.addNewNode('Person', result[0], result[0], knowledgeBeginBegin,
                                             knowledgeBeginBegin, 1.0, entityId, {"_":"_"})

        results = self._c.execute('select n2.knowledgeBeginBegin, min(n2.knowledgeBeginBegin) from NodesData n1, NodesData n2 where '
                                  'n1.nodeID = ? and n2.entityID = ? and '
                                  'n1.knowledgeEndEnd < n2.knowledgeBeginBegin and '
                                  'n2.nodeID != n1.nodeID '
                                  'group by n2.knowledgeEndEnd', (nodeID, entityId)).fetchall()

        for result in results:
            if result != []:
                resultAddNode2 = self.addNewNode('Person', knowledgeEndEnd, knowledgeEndEnd, result[0], result[0], 1.0, entityId,
                            {"_": "_"})

        return [*resultAddNode1, *resultAddNode2]

