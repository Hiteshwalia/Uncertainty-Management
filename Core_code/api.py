import datetime
from json import JSONEncoder
from time import strptime

from flask import Flask, request, send_file
from flask_cors import CORS
import os, io, json
from ConceptualModel import *
import Transaction
import sqlite3



app = Flask(__name__)
CORS(app)


class NodesAndRelationshipsEncoder(JSONEncoder):
    def default(self, obj):
        """
        Transform a python array to JSON object
        :param obj: Object that necessary to transform to JSON
        :return: a JSON
        """
        if type(obj).__name__ == 'Node':
            return {
                'id': obj.id,
                'labels': list(obj.labels),
                'properties': dict(obj._properties)
            }
        elif type(obj).__name__ in RELATION_TYPES:
            return {
                'id': -obj.id-1,
                'type': obj.type,
                'startNode': obj.start_node.id,
                'endNode': obj.end_node.id,
                'properties': dict(obj._properties)
            }
        elif isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        else:
            return JSONEncoder.default(self, obj)

def sendJson(obj):
    """
    Send the jon
    :param obj: Json object
    :return: send the JSON
    """
    buffer = io.BytesIO()
    buffer.write(json.dumps(obj, separators=(',', ':'), cls=NodesAndRelationshipsEncoder, ensure_ascii=False).encode())
    buffer.seek(0)
    return send_file(buffer, mimetype='application/octet-stream')

@app.route("/", methods=["GET"])
def hello():
    return "Hello World!"

def mergeInfoNodeNeo4jAndSQL(sqlInformation, neo4jInformation):
    """
    Merge node information neo4j and sql
    :param sqlInformation: Sql information
    :param neo4jInformation: neo4j information
    :return: Object with merge of sql and neo4j information
    """
    for informationS in sqlInformation:
        for informationN in neo4jInformation:
            if informationS[0] == informationN[0].id:
                informationN[0]._properties['knowledgeBeginBegin'] = informationS[2]
                informationN[0]._properties['knowledgeBeginEnd'] = informationS[3]
                informationN[0]._properties['knowledgeEndBegin'] = informationS[4]
                informationN[0]._properties['knowledgeEndEnd'] = informationS[5]
                informationN[0]._properties['probability'] = informationS[8]

def mergeInfoRelationNeo4jAndSQL(sqlInformation, neo4jInformation):
    """
    Merge relation information neo4j and sql
    :param sqlInformation: Sql information
    :param neo4jInformation: neo4j information
    :return: Object with merge of sql and neo4j information
    """
    for informationS in sqlInformation:
        for informationN in neo4jInformation:
            if -informationS[0] - 1 == informationN[0].id:
                # informationN[1].id = informationS[0]
                informationN[0]._properties['knowledgeBeginBegin'] = informationS[1]
                informationN[0]._properties['knowledgeBeginEnd'] = informationS[2]
                informationN[0]._properties['knowledgeEndBegin'] = informationS[3]
                informationN[0]._properties['knowledgeEndEnd'] = informationS[4]
                informationN[0]._properties['probability'] = informationS[7]

def allMutualExclusionNode(nodeId, tx):
    """
    Get all mutual exclusion and information about node and relation for a specific node
    :param nodeId: Id of node or relation
    :param tx: Transaction
    :return: Information about node or relation with a mutual exclusion
    """
    allID = []

    informationsSQLMutualExclusionID = tx._c.execute("select * from mutualExclusion where id1 = ? or id2 = ?",
                                                     (nodeId, nodeId)).fetchall()

    for id in informationsSQLMutualExclusionID:
        if (id[0] != int(nodeId)):
            allID.append(id[0])
        else:
            allID.append((id[1]))

    allNode = []
    if(allID.__len__() == 1):
        allID.append(0)
    if allID.__len__() > 1:
        informationsSQL = tx._c.execute("select * from NodesData where NodeID in " + str(tuple(allID))).fetchall()
        informationNeo4j = tx._tx.run("match (n) where ID(n) in $id return n", id=allID).values()
        mergeInfoNodeNeo4jAndSQL(informationsSQL, informationNeo4j)

        for info in informationNeo4j:
            allNode.append(info[0])


    #Get all relations
    allRelation = []
    if (allID.__len__() == 1):
        allID.append(0)
    if allID.__len__() > 1:
        informationsSQL = tx._c.execute("select * from LinksData where linkID in " + str(tuple(allID))).fetchall()
        informationNeo4j = tx._tx.run("match (n)-[r]-(n2) where ID(n) in $id return r", id=allID).values()
        mergeInfoNodeNeo4jAndSQL(informationsSQL, informationNeo4j)

        for informationS in informationsSQL:
            for informationN in informationNeo4j:
                if -informationS[0] - 1 == informationN[1].id:
                    informationN[1]._properties['knowledgeBeginBegin'] = informationS[1]
                    informationN[1]._properties['knowledgeBeginEnd'] = informationS[2]
                    informationN[1]._properties['knowledgeEndBegin'] = informationS[3]
                    informationN[1]._properties['knowledgeEndEnd'] = informationS[4]
                    informationN[1]._properties['transactionBegin'] = informationS[5]
                    informationN[1]._properties['transactionEnd'] = informationS[6]
                    informationN[1]._properties['probability'] = informationS[7]
                    allRelation.append(informationN[1])

    # for info in informationNeo4j:
    #     allRelation.append(info[0])

    return informationsSQLMutualExclusionID, allNode, allRelation


@app.route("/getNeighbours/<nodeId>", methods=["GET"])
def getNeighbourID(nodeId):
    """
    Get all neighbour of a specific Node
    :param nodeId: Id of this specific node
    :return: All neighbours information
    """
    with Transaction.driver.session() as session:
        con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
        con.isolation_level = None

        tx = Transaction.Transaction(session, con)
        informationNeo4j = tx._tx.run("match (n)-[r]-(n2) where ID(n)=$id return n2,r", id=int(nodeId)).values()

        allID = []
        for information in informationNeo4j:
            allID.append(information[0].id)

        if allID.__len__() == 1:
            allID.append(-1)

        informationsSQL = tx._c.execute("select * from NodesData where NodeID in "+str(tuple(allID))).fetchall()

        mergeInfoNodeNeo4jAndSQL(informationsSQL, informationNeo4j)

        allID = []
        for information in informationNeo4j:
            allID.append(-information[1].id-1)

        if allID.__len__() == 1:
            allID.append(1)

        informationsSQLRelation = tx._c.execute("select * from linksData where linkID in "+str(tuple(allID))).fetchall()

        allRelation = []
        allNode = []

        for info in informationNeo4j:
            allNode.append(info[0])

        # mergeInfoRelationNeo4jAndSQL(informationsSQLRelation,informationNeo4j)

        for informationS in informationsSQLRelation:
            for informationN in informationNeo4j:
                if -informationS[0] - 1 == informationN[1].id:
                    informationN[1]._properties['knowledgeBeginBegin'] = informationS[1]
                    informationN[1]._properties['knowledgeBeginEnd'] = informationS[2]
                    informationN[1]._properties['knowledgeEndBegin'] = informationS[3]
                    informationN[1]._properties['knowledgeEndEnd'] = informationS[4]
                    informationN[1]._properties['transactionBegin'] = informationS[5]
                    informationN[1]._properties['transactionEnd'] = informationS[6]
                    informationN[1]._properties['probability'] = informationS[7]
                    allRelation.append(informationN[1])

        # allNode = []

        # for info in informationNeo4j:
        #     allNode.append(info[0])

        #Get all mutual exclusion

        mutualExclusionsID, allNode2, allRelation2 = allMutualExclusionNode(nodeId, tx)
        # mutualExclusions = []
        con.close()

        allNode = [*allNode, *allNode2]
        allRelation = [*allRelation, *allRelation2]

        graph = {
            'nodes': allNode,
            'relationships': allRelation,
            'mutex': mutualExclusionsID
        }

    return sendJson(graph)

def getAllNodeInArray(idArray):
    """
    Get ll Information about node in array
    :param idArray: All ids nodes
    :return: Information about there nodes
    """
    with Transaction.driver.session() as session:
        con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
        con.isolation_level = None

        tx = Transaction.Transaction(session, con)
        allNode = []
        for nodeId in idArray:

            informationNeo4j = tx._tx.run("match (n)where ID(n)=$id return n", id=int(nodeId)).values()
            informationsSQL = tx._c.execute("select * from NodesData where NodeID = ?", (nodeId,)).fetchall()

            mergeInfoNodeNeo4jAndSQL(informationsSQL, informationNeo4j)

            for info in informationNeo4j:
                allNode.append(info[0])

        con.close()


    return allNode


@app.route("/node/<nodeId>", methods=["GET"])
def getSpecificNode(nodeId):
    """
    Get specific node
    :param nodeId: Id of this node
    :return: Information about this node
    """
    # with truc2.driver.session() as session:
    #     con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
    #     con.isolation_level = None
    #
    #     tx = truc2.Transaction(session, con)
    #
    #     informationNeo4j = tx._tx.run("match (n)where ID(n)=$id return n", id=int(nodeId)).values()
    #     informationsSQL = tx._c.execute("select * from NodesData where NodeID = ?", (nodeId,)).fetchall()
    #
    #     mergeInfoNodeNeo4jAndSQL(informationsSQL, informationNeo4j)
    #
    #
    #
    #     con.close()
    #
    #     allNode = []
    #
    #     for info in informationNeo4j:
    #         allNode.append(info[0])

    allNode = getAllNodeInArray([nodeId])

    graph = {
        'nodes': allNode,
        'relationships': [],
        'mutex': []
    }


    return sendJson(graph)

@app.route("/relation/<relationId>", methods=["GET"])
def getSpecificRelation(relationId):
    """
    Get specific relation
    :param relationId: Id of this specific relation
    :return: Information about this relation
    """
    with Transaction.driver.session() as session:
        con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
        con.isolation_level = None

        tx = Transaction.Transaction(session, con)

        informationNeo4j = tx._tx.run("match (n)-[r]->(n2) where ID(r)=$id return r", id=int(-relationId-1)).values()
        informationsSQL = tx._c.execute("select * from LinksData where linkID = ?", (relationId,)).fetchall()

        mergeInfoRelationNeo4jAndSQL(informationsSQL, informationNeo4j)



        con.close()

        allRelation = []

        for info in informationNeo4j:
            allRelation.append(info[0])

        graph = {
            'nodes': [],
            'relationships': allRelation,
            'mutex':[]
        }


    return sendJson(graph)

@app.route("/createNode", methods=["POST"])
def createNode():
    """
    Create a specific relation
    :return: Information about this new node
    """
    data = request.get_json(force=True)
    assert data, "data is null"
    assert data.get('properties') is not None, "Properties must be set"
    assert data.get('knowledgeBeginBegin') is not None, "knowledgeBeginBegin mustn't be None"
    assert data.get('knowledgeBeginEnd') is not None, "knowledgeBeginBegin mustn't be None"
    assert data.get('knowledgeEndBegin') is not None, "knowledgeBeginBegin mustn't be None"
    assert data.get('knowledgeEndEnd') is not None, "knowledgeBeginBegin mustn't be None"
    assert type(data.get('conflictAccept')) == bool, "You must specify if you accepte the conflict"

    with Transaction.driver.session() as session:
        con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
        con.isolation_level = None

        tx = Transaction.Transaction(session, con)

        mutualExclusion = []

        try:
            if data.get('conflictAccept'):
                arrayId, mutualExclusion = tx.addNewNode(data.get('entityType'),
                                        datetime.datetime.strptime(data.get('knowledgeBeginBegin'), "%Y-%m-%d").date(),
                                        datetime.datetime.strptime(data.get('knowledgeBeginEnd'), "%Y-%m-%d").date(),
                                        datetime.datetime.strptime(data.get('knowledgeEndBegin'), "%Y-%m-%d").date(),
                                        datetime.datetime.strptime(data.get('knowledgeEndEnd'), "%Y-%m-%d").date(),
                                        data.get('probability'), data.get('entityID'), data.get('properties'), conflictAccept=True)
            else:
                arrayId = tx.addNewNode(data.get('entityType'), datetime.datetime.strptime(data.get('knowledgeBeginBegin'),"%Y-%m-%d").date(),
                                   datetime.datetime.strptime(data.get('knowledgeBeginEnd'),"%Y-%m-%d").date(),
                              datetime.datetime.strptime(data.get('knowledgeEndBegin'),"%Y-%m-%d").date(),
                                   datetime.datetime.strptime(data.get('knowledgeEndEnd'),"%Y-%m-%d").date(),
                              data.get('probability'), data.get('entityID'), data.get('properties'))
        except ValueError as e:
            return str(e), 400

        tx.commit()

        con.close()

    allNode =  getAllNodeInArray(arrayId)

    graph = {
        'nodes': allNode,
        'relationships': [],
        'mutex': mutualExclusion
    }

    return sendJson(graph)



@app.route("/createRelation", methods=["POST"])
def createRelation():
    """
    Create a new relation
    :return: Information about this new relation
    """
    data = request.get_json(force=True)
    assert data, "data is null"
    assert data.get('properties') is not None, "Properties must be set"
    assert data.get('knowledgeBeginBegin') is not None, "knowledgeBeginBegin mustn't be None"
    assert data.get('knowledgeBeginEnd') is not None, "knowledgeBeginBegin mustn't be None"
    assert data.get('knowledgeEndBegin') is not None, "knowledgeBeginBegin mustn't be None"
    assert data.get('knowledgeEndEnd') is not None, "knowledgeBeginBegin mustn't be None"
    assert data.get('start') is not None, "start node mustn't be None"
    assert data.get('end') is not None, "end node mustn't be None"

    with Transaction.driver.session() as session:
        con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
        con.isolation_level = None

        tx = Transaction.Transaction(session, con)

        try:
            id = tx.addNewRelation(data.get('relationType'), datetime.datetime.strptime(data.get('knowledgeBeginBegin'),"%Y-%m-%d").date(),
                               datetime.datetime.strptime(data.get('knowledgeBeginEnd'),"%Y-%m-%d").date(),
                          datetime.datetime.strptime(data.get('knowledgeEndBegin'),"%Y-%m-%d").date(),
                               datetime.datetime.strptime(data.get('knowledgeEndEnd'),"%Y-%m-%d").date(),
                          data.get('probability'), data.get('properties'), data.get('start'), data.get('end'))
        except ValueError as e:
            return str(e), 400

        tx.commit()

        con.close()

    return getSpecificRelation(id)

@app.route("/createMutex", methods=["POST"])
def createMutex():
    """
    Create new mutex
    :return: Array with the 2 nodes in mutual exclusion
    """
    data = request.get_json(force=True)
    assert data, "data is null"
    assert data.get('id1') is not None, "id1 mustn't be None"
    assert type(data.get('id1')) == int, "id1 mustn't be int"
    assert data.get('id2') is not None, "id2 mustn't be None"
    assert type(data.get('id2')) == int, "id1 mustn't be int"

    with Transaction.driver.session() as session:
        con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
        con.isolation_level = None
        tx = Transaction.Transaction(session, con)

        result = tx._tx.run("match (n)-[r]-(n2) where ID(n)=$id1 and ID(n2)=$id2 return n", id1=data.get('id1'), id2=data.get('id2')).values()

        assert result.__len__() == 0, "You must put a mutux beetween 2 nodes that they have a relation"


        try:
            tx.createMutualExclusion(data.get('id1'), data.get('id2'))
            tx.commit()
        except ValueError as e:
            con.close()
            return str(e), 400

        con.close()

    graph = {
        'nodes': [],
        'relationships': [],
        'mutex': [[data.get('id1'), data.get('id2')]]
    }

    return sendJson(graph)



@app.route("/getAllType", methods=['GET'])
def getAllType():
    """
    Get all types nodes and relations
    :return: All types nodes and relations
    """
    return sendJson({"NodeTypes":NODE_TYPES, "RelationTypes":RELATION_TYPES})


def getInformationsGraph(informationNeo4j1, informationNeo4j2,  tx):
    """
    Get information neo4j
    :param informationNeo4j1: Information neo4j 1
    :param informationNeo4j2: Information neo4j 2
    :param tx: Transaction
    :return: Information about node, relation and mutex in there information
    """
    allIDNode = []
    allNode = []
    if informationNeo4j1 is not None:


        for information in informationNeo4j1:
            allIDNode.append(information[0].id)

        if allIDNode.__len__() == 1:
            allIDNode.append(-1)

        informationsSQL = tx._c.execute("select * from NodesData where NodeID in " + str(tuple(allIDNode))).fetchall()

        mergeInfoNodeNeo4jAndSQL(informationsSQL, informationNeo4j1)




        for info in informationNeo4j1:
            allNode.append(info[0])

    allRelation = []
    allIDRelation = []
    if informationNeo4j2 is not None:


        for information in informationNeo4j2:
            allIDRelation.append(-information[0].id - 1)

        if allIDRelation == 1:
            allIDRelation.append(1)

        informationsSQLRelation = tx._c.execute(
            "select * from linksData where linkID in " + str(tuple(allIDRelation))).fetchall()

        mergeInfoRelationNeo4jAndSQL(informationsSQLRelation, informationNeo4j2)

        for info in informationNeo4j2:
            allRelation.append(info[0])

    allMutex = tx.researchMutex(allIDNode, allIDRelation)

    return allNode, allRelation, allMutex

def researchInformationAfterSqlRequest(sqlRequestNode, sqlRequestRelation, tx):
    """
    Research information
    :param sqlRequestNode: Result of the first request (of node)
    :param sqlRequestRelation: Result of the second request (of relation)
    :param tx: Transaction
    :return: Information about node, relation and mutex in there information
    """
    allIDNode = []
    allNode = []
    if sqlRequestNode is not None:

        for information in sqlRequestNode:
            allIDNode.append(information[0])

        informationsNeo4j = tx._tx.run("match (n) where ID(n) in " + str(allIDNode) + " return n").values()

        mergeInfoNodeNeo4jAndSQL(sqlRequestNode, informationsNeo4j)

        for info in informationsNeo4j:
            allNode.append(info[0])

    allRelation = []
    allIDRelation = []
    if sqlRequestRelation is not None:

        for information in sqlRequestRelation:
            allIDRelation.append(-information[0] - 1)

        informationsNeo4jRelation = tx._tx.run("match (n)-[r]-(n2) where ID(r) in " + str(allIDRelation) + " return r").values()

        mergeInfoRelationNeo4jAndSQL(sqlRequestRelation, informationsNeo4jRelation)

        for info in informationsNeo4jRelation:
            allRelation.append(info[0])

    allMutex = tx.researchMutex(allIDNode, allIDRelation)

    return allNode, allRelation, allMutex


def researchInformations(typeReasearch, informations):
    """
    Research information
    :param typeReasearch: Type of research
    :param informations: Information for this research
    :return: Information
    """
    with Transaction.driver.session() as session:
        con = sqlite3.connect('sqlite3.db', detect_types=sqlite3.PARSE_DECLTYPES)
        con.isolation_level = None

        tx = Transaction.Transaction(session, con)
        if typeReasearch == "Type":

            informationNeo4j1 = []
            informationNeo4j2 = []

            assert informations.get('relationType') is not None or informations.get('entityType') is not None, \
                "You must specify a type of entity or type of relation"


            if informations.get('entityType') is not None:

                assert informations.get('entityType') in NODE_TYPES, "Your entity type doesn't exist"

                informationNeo4j1 = tx._tx.run("match (n:" + informations.get('entityType') + ") return n LIMIT $limit",
                                               limit=informations.get('limit')).values()

            if informations.get('relationType') is not None:

                assert informations.get('relationType') in RELATION_TYPES, "Your relation type doesn't exist"

                informationNeo4j2 = tx._tx.run("match (n)-[r:" + informations.get('relationType') + "]-(n2) return r LIMIT $limit",
                                              type=informations,
                                              limit=informations.get('limit')).values()

            if informationNeo4j1 != []:
                allIDNode = []
                for info in informationNeo4j1:
                    allIDNode.append(info[0].id)
            informationNeo4j2 = [*informationNeo4j2, *(tx._tx.run("match (n)-[r]-(n2) where ID(n) in " + str(allIDNode)
                                                                  + " and ID(n2) in " + str(allIDNode) + " return r LIMIT $limit",
                                          type=informations,
                                          limit=informations.get('limit')).values())]

            allNode, allRelation, allMutex = getInformationsGraph(informationNeo4j1, informationNeo4j2, tx)

            graph = {
                'nodes': allNode,
                'relationships': allRelation,
                'mutex': allMutex
            }

            return graph

        elif typeReasearch == "Date":

            assert informations.get('begin') is not None, "Begin date must be set"
            assert informations.get('end') is not None, "End date must be set"

            assert informations.get('begin') < informations.get('end'), "Your end date must be before your begin dates"
            if(informations.get('limit')):
                limit = informations.get('limit')
            else :
                limit = 100

            informationSQL1 = tx._c.execute("select * from NodesData where (knowledgeBeginBegin between ? and ?)"
                                        " or (knowledgeEndEnd between ? and ?) "
                                        " or (knowledgeBeginBegin < ? and knowledgeEndEnd > ?)"
                                        " LIMIT ?", (
                datetime.datetime.strptime(informations.get('begin'), "%Y-%m-%d").date(),
                datetime.datetime.strptime(informations.get('end'), "%Y-%m-%d").date(),
                datetime.datetime.strptime(informations.get('begin'), "%Y-%m-%d").date(),
                datetime.datetime.strptime(informations.get('end'), "%Y-%m-%d").date(),
                datetime.datetime.strptime(informations.get('begin'), "%Y-%m-%d").date(),
                datetime.datetime.strptime(informations.get('end'), "%Y-%m-%d").date(),
                limit
            )).fetchall()

            # informationSQL2 = tx._tx.run("match (n)-[r:" + informations.get('relationType') + "]-(n) return r LIMIT $limit",
            #                               type=informations,
            #                               limit=informations.get('limit')).values()

            allID = []
            for info in informationSQL1:
                allID.append(info[0])

            if allID.__len__() == 1:
                allID.append(-1)

            informationNeo4jR = tx._tx.run("match (n)-[r]-(n2) where ID(n) in " + str(allID) + " and ID(n2) in " + str(allID) + " return r" )

            allIDRel = []

            for info in informationNeo4jR:
                allIDRel.append(-info[0].id-1)


            informationSQL2 = tx._c.execute("select * from LinksData where (knowledgeBeginBegin between ? and ?)"
                                        " or (knowledgeEndEnd between ? and ?) "
                                        " or (knowledgeBeginBegin < ? and knowledgeEndEnd > ?)"
                                        " or (linkID in " + str(tuple(allIDRel)) + ")" 
                                        " LIMIT ?", (
                datetime.datetime.strptime(informations.get('begin'), "%Y-%m-%d").date(),
                datetime.datetime.strptime(informations.get('end'), "%Y-%m-%d").date(),
                datetime.datetime.strptime(informations.get('begin'), "%Y-%m-%d").date(),
                datetime.datetime.strptime(informations.get('end'), "%Y-%m-%d").date(),
                datetime.datetime.strptime(informations.get('begin'), "%Y-%m-%d").date(),
                datetime.datetime.strptime(informations.get('end'), "%Y-%m-%d").date(),
                limit
            )).fetchall()


            allNode, allRelation, allMutex = researchInformationAfterSqlRequest(informationSQL1, informationSQL2, tx)

            graph = {
                'nodes': allNode,
                'relationships': allRelation,
                'mutex': allMutex
            }

            return graph



        elif typeReasearch == "Property":

            assert informations.get('key') is not None, "You have to enter a key of property"
            assert informations.get('value') is not None, "You have to enter a value of property"

            informationNeo4j1 = tx._tx.run("match (n) where n." + informations.get('key') + "=~$value return n LIMIT $limit",
                                           value='.*' + informations.get('value') + '.*',
                                           limit=informations.get('limit')).values()
            informationNeo4j2 = tx._tx.run(
                "match (n)-[r]-(n) where r." + informations.get('key') + "=~$value return r LIMIT $limit",
                value='.*' + informations.get('value') + '.*',
                limit=informations.get('limit')).values()

            if informationNeo4j1 != []:
                allIDNode = []
                for info in informationNeo4j1:
                    allIDNode.append(info[0].id)
                informationNeo4j2 = [*informationNeo4j2, *(tx._tx.run("match (n)-[r]-(n2) where ID(n) in " + str(allIDNode)
                                                                  + " and ID(n2) in " + str(allIDNode) + " return r LIMIT $limit",
                                          type=informations,
                                          limit=informations.get('limit')).values())]

            allNode, allRelation, allMutex = getInformationsGraph(informationNeo4j1, informationNeo4j2, tx)

            graph = {
                'nodes': allNode,
                'relationships': allRelation,
                'mutex': allMutex
            }

            return graph

        else:
            assert "This type : " + typeReasearch + " is impossible at research"

        con.close()

@app.route("/researchInformation", methods=['POST'])
def research():
    """
    Get information for a specific research
    :return: Information for this research
    """
    data = request.get_json(force=True)
    assert data, "data is null"
    assert data.get('searchby') is not None, "You must enter a type for this research"
    assert type(data.get("limit")) == int, "Your limit have to be an integer"

    return sendJson(researchInformations(data.get('searchby'), data))


@app.errorhandler(AssertionError)
def errorH(e):
    """
    Return an error
    :param e: Error that we would display
    :return: error
    """
    return str(e), 400