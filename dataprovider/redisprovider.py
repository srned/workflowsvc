from datetime import datetime, timedelta
import redis
import json
import ast

class RedisProvider(object):

    def __init__(self):
        self.conn = redis.StrictRedis('localhost', port=6379, db=0)

    def save_timeout(self, timeouttype, uid, expiretime):
        """Saves timers

        Args:
        timeouttype (str): Timeout.WFExecution, Timeout.DecisionTask,
        Timeout.Activity.SchedToClose, Timeout.Acitivity.SchedToStart,
        Timeout.Acitivity.heartbeat
        uid (str): runId[:taskToken]
        expiretime (datetime): expiration time
        """
        #data = {"expiretime": expiretime.strftime('%s'),"id": uid}
        print "Adding to:" + str(timeouttype)
        r = self.conn.zadd(timeouttype, int(expiretime.strftime('%s')), uid)
        print "result:" + str(r)

    def get_timeout(self, timeouttype, to_date, from_date = datetime(1970,1,1,0,0,0)):
        """Gets timeouts that have expired until to_date

        Args:
        timeouttype (str): wfexecs, activity
        from_date (datetime): optional
        to_date (datetime): usually now
        """
        # start a redis MULTI/EXEC transaction
        pipeline = self.conn.pipeline()

        expired_ids = []
        start = int(from_date.strftime("%s"))
        end = int(to_date.strftime("%s"))
        #rows = self.conn.zrangebyscore(timeouttype, start, end)
        pipeline.zrangebyscore(timeouttype, start, end)
        pipeline.zremrangebyscore(timeouttype, start, end)

        # commit transaction to redis
        results = pipeline.execute()

        for row in results[-2]:
            #row = ast.literal_eval(row)
            expired_ids.append(row)

        return expired_ids

    def del_timeout(self, timeouttype, uidlist):
        """ Removes timers
        """
        self.conn.zrem(timeouttype, uidlist)


    def save_workflow_type(self, workflow_type):
        """
        save to workflow.type set the given type
        """
        self.conn.sadd("workflow.type", workflow_type)

    def check_workflow_type(self, workflow_type):
        """
        Check if given workflow type is already present
        """
        return self.conn.sismember("workflow.type",workflow_type)

    def del_workflow_type(self, workflow_type):
        self.conn.srem(workflow_type)

    def save_activity_type(self, activity_type):
        self.conn.sadd("activity.type", activity_type)

    def check_activity_type(self, activity_type):
        return self.conn.sismember("activity.type", activity_type)

    def del_activity_type(self, activity_type):
        self.conn.srem(activity_type)

    def save_workflow_current(self, runid, wf_data):
        """
        Stores current state of workflow executions
        workflow.current:[Runid]
        key: RunId
        Values: workflowtype, workflowid, TaskTimeout (Decision task), TaskList
        """
        self.conn.hmset("workflow.current:" + runid, wf_data)

    def check_workflow_current(self, runid):
        """
        Future use
        Check if run id currently running
        """
        return self.conn.exists(runid)

    def get_workflow_current(self, runid):
        """
        Retrieves meta data related to workflow execution
        """
        return self.conn.hgetall("workflow.current:" + runid)

    def del_workflow_current(self, runid):
        """
        Delete workflow current hash
        """
        self.conn.delete("workflow.current:" + runid)

    def save_task_current(self, tasktoken, taskvalues):
        """
        Stores task related data in hash
        key: task.current:[tasktoken]
        values: type[decision, activity], runid, workflowid,
                scheduledeventid, startedeventid, tasklistname
        """
        self.conn.hmset("task.current:" + tasktoken, taskvalues)

    def pop_task_current(self, tasktoken):
        pipeline = self.conn.pipeline()
        pipeline.hgetall("task.current:" + tasktoken)
        pipeline.delete("task.current:" + tasktoken)
        results = pipeline.execute()
        return results[-2]

    def get_task_current(self, tasktoken):
        return self.conn.hgetall("task.current:" + tasktoken)

    def save_task_open(self, runid, tasktoken):
        """
        Stores open tasks for a runid: Set
        set name: task.open:[runid]
        value: tasktoken
        """
        self.conn.sadd("task.open:"+ runid , tasktoken)

    def get_task_open(self, runid):
        return self.conn.smembers("task.open:" + runid)

    def pop_task_open(self, runid):
        """
        needed only during cleanup
        """
        pipeline = self.conn.pipeline()
        pipeline.smembers("task.open:" + runid)
        pipeline.delete("task.open:" + runid)
        results = pipeline.execute()
        return results[-2]

    def del_task_open(self, runid, tasktoken):
        """ called when task completes
        """
        self.conn.srem("task.open:" + runid, tasktoken)

    def remkey_task_open(self, runid):
        self.conn.delete("task.open:" + runid)

    def save_workflow_id(self, wfid, wf_dict):
        """
        Stores workflow.id:hash
        key: workflowid
        values: RunId, State[Running, Completed, Failed, Terminated]
        """
        self.conn.hmset("workflow.id:" + str(wfid), wf_dict)

    def get_workflow_id(self, wfid):
        return self.conn.hgetall("workflow.id:" + wfid)

    def check_workflow_id(self, wfid):
        """
        Check if given workflow id is already present
        """
        #return self.conn.sismember("workflow.id",workflow_id)
        return self.conn.exists("workflow.id:" + wfid)

    def incr_runid(self):
        """
        Atomic Increment of runid
        """
        if not self.conn.exists("count:Runid"):
            self.conn.set("count:Runid", 1)
            return 1
        else:
            return self.conn.incr("count:Runid")

    def incr_eventid(self, key):
        """
        Increment eventid if present, if not, create one to start value as 1
        Remember to clean up when workflow is closed
        """
        if not self.conn.exists("workflow.eventid:" + key):
            self.conn.set("workflow.eventid:" + key, 1)
            return 1
        else:
            return self.conn.incr("workflow.eventid:" + key)

    def del_eventid(self, key):
        """
        Delete eventid when workflow is complete!!!!!!!
        """
        self.conn.delete("workflow.eventid:" + key)

    def incr_tasktoken(self):
        """
        Atomic Increment of taskToken
        """
        if not self.conn.exists("count:TaskToken"):
            self.conn.set("count:TaskToken", 1)
            return 1
        else:
            return self.conn.incr("count:TaskToken")

    def add_tasklist(self, tasklist, tasktoken):
        """
        Push the taskToken to the Decision/Activity Tasklist
        """
        self.conn.rpush(tasklist, tasktoken)

    def pop_tasklist(self, tasklist):
        """
        Pops the first task from the Tasklist, if not, returns NONE
        """
        return self.conn.lpop(tasklist)

    def del_tasklist(self, tasklist, tasktoken):
        """
        Deletes the given Id from the given list
        TODO: Delete even the list if empty
        """
        self.conn.lrem(tasklist, 0, tasktoken)
