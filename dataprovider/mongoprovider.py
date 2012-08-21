import pymongo
import datetime
import json
from pymongo import Connection
from pymongo import ASCENDING, DESCENDING

class WorkFlowPyProvider(object):

    def __init__(self):
        self.conn = Connection('localhost', 27017)
        self.db = self.conn['workflowdb']

    def save_workflow_type(self, data):
        workflowtypes = self.db['workflow.types']
        workflowtypes.insert(data)

    def get_workflow_type(self, name):
        workflowtypes = self.db['workflow.types']
        return workflowtypes.find_one({"name": name})

    def save_activity_type(self, data):
        activitytypes = self.db['activity.types']
        activitytypes.insert(data)

    def get_activity_type(self, name):
        activitytypes = self.db['activity.types']
        return activitytypes.find_one({"name": name})

    def save_workflow_exec(self, data):
        workflowexecs = self.db['workflow.execs']
        return workflowexecs.insert(data)

    def CountworkflowId(self, workflowId):
        workflowexecs = self.db['workflow.execs']
        return workflowexecs.find({"workflowId": workflowId}).count()

    def CountworkflowType(self, workflowType):
        workflowtypes = self.db['workflow.types']
        return workflowtypes.find({"name": workflowType}).count()

    def executionStartToCloseTimeout(self, workflowType):
        workflowtypes = self.db['workflow.types']
        return workflowtypes.find_one({"name": workflowType},
                        {"defaultExecutionStartToCloseTimeout":1, "_id" : 0})["defaultExecutionStartToCloseTimeout"]

    def default_tasklist(self, workflowType):
        workflowtypes = self.db['workflow.types']
        return workflowtypes.find_one({"name": workflowType},
                        {"defaultTaskList" : { "name":1, "_id" : 0}})["name"]

    def default_tasktimeout(self, workflowType):
        workflowtypes = self.db['workflow.types']
        return workflowtypes.find_one({"name": workflowType},
                        {"defaultTaskStartToCloseTimeout":1, "_id" : 0})["defaultTaskStartToCloseTimeout"]

    def save_events_history(self, data):
        events_history = self.db['events.history']
        events_history.insert(data)

    def get_events_history(self, runId, reverseOrder):
        events_history = self.db['events.history']
        results = []
        #return events_history.find({"RunId" : runId}, {"_id" : 0}).sort( { "eventTimestamp" : -1 if reverseOrder else 0 } )
        sortorder = -1 if reverseOrder == "True" else 1
        for history in events_history.find({"RunId" : runId}, {"RunId": 0, "_id" : 0}).sort("eventId",sortorder):
            results.append(history)

        return results

    def get_event(self, runId, eventId):
        events_history = self.db['events.history']
        return events_history.find_one({"RunId": runId, "eventId": eventId}, {"_id" : 0})
