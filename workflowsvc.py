#!/usr/bin/python
import tornado.auth
import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from datetime import datetime, timedelta
from validator import validator
import config

from tornado.options import define, options

define("port", default=8888, help="run on the given port", type=int)
def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    #vLoop = tornado.ioloop.IOLoop.instance()
    #vLoop.PeriodicCallback(CronServer, 1000)
    #vLoop.start()
    timerinterrupt = tornado.ioloop.PeriodicCallback(CronServer, 1000)
    timerinterrupt.start()
    tornado.ioloop.IOLoop.instance().start()

"""
Here is where we do all our async operations
"""
def CronServer():
    """ Close the Workflow Executions and dont allow other than visibility calls """
    RunIds = config.redis.get_timeout("Timeout.WFExecution", datetime.now())
    #print RunIds
    for runId in RunIds:
        runId = str(runId)
        print "Timeout.WFExecution:" + runId
        config.events.add(runId, "WorkflowExecutionTimedOut",
                {'childPolicy': 'TERMINATE', 'timeoutType': 'START_TO_CLOSE'})
        terminateworkflowexecution(runId, "TIMEDOUT")
        #config.redis.del_timeout("Timeout.WFExecution", RunIds) #Bulk delete
        """ TODO dont allow other than visibility calls """

    #print "Timeout.DecisionTask:"
    DecisionTasks =  config.redis.get_timeout("Timeout.DecisionTask", datetime.now())
    task_timedout(DecisionTasks, "DECISION_TASK")

    #print "Timeout.Activity.SchedToClose:"
    ActivityTasks =  config.redis.get_timeout("Timeout.Activity.SchedToClose", datetime.now())
    task_timedout(ActivityTasks, "SCHED_TO_CLOSE")

    #print "Timeout.Acitivity.SchedToStart:"
    ActivityTasks = config.redis.get_timeout("Timeout.Activity.SchedToStart", datetime.now())
    task_timedout(ActivityTasks, "SCHED_TO_START")

    ActivityTasks = config.redis.get_timeout("Timeout.Activity.StartToClose", datetime.now())
    task_timedout(ActivityTasks, "START_TO_CLOSE")

    #print "Timeout.Acitivity.heartbeat:"
    ActivityTasks = config.redis.get_timeout("Timeout.Acitivity.heartbeat", datetime.now())
    task_timedout(ActivityTasks, "HEARTBEAT")

"""
Common Methods
"""

def terminateworkflowexecution(runId, state):
    wf_data = config.redis.get_workflow_current(runId)
    print wf_data
    tasks = config.redis.pop_task_open(runId)
    for taskToken in tasks:
        taskToken = str(taskToken)
        task = config.redis.pop_task_current(taskToken)
        if 'startedEventId' in task:
            if task['type'] == "Activity":
                config.redis.del_timeout("Timeout.Activity.SchedToClose", runId + ":" + taskToken)
                config.redis.del_timeout("Timeout.Activity.StartToClose", runId + ":" + taskToken)
                config.redis.del_timeout("Timeout.Activity.heartbeat", runId + ":" + taskToken)
            else:
                config.redis.del_timeout("Timeout.DecisionTask",runId + ":" + taskToken)
        elif task['type'] == "Activity":
            config.redis.del_timeout("Timeout.Activity.SchedToClose", runId + ":" + taskToken)
            config.redis.del_timeout("Timeout.Activity.SchedToStart", runId + ":" + taskToken)
        config.redis.del_tasklist(task['taskList'], taskToken)
    config.redis.del_workflow_current(runId)
    config.redis.del_eventid(runId)
    config.redis.save_workflow_id(wf_data['workflowId'], {"runId": runId, "State": state})
    config.redis.del_timeout("Timeout.WFExecution", runId)
    


def task_timedout(Timeouttasks, timeouttype):
    #print Timeouttasks
    for t_task in Timeouttasks:
        runId = str(t_task.split(':')[0])
        taskToken = str(t_task.split(':')[1])
        print timeouttype + ":" + runId + ":" + taskToken
        wf_data = config.redis.get_workflow_current(str(runId))
        task = config.redis.pop_task_current(taskToken)
        config.redis.del_task_open(runId, taskToken)
        if task['type'] == "Decision":
            config.events.add(runId, "DecisionTaskTimedOut",
                    {'scheduledEventId': task['scheduledEventId'],
                        'startedEventId': task['startedEventId'],
                        'timeoutType': 'START_TO_CLOSE'})
        else:
            tasktimeout = {'details': 'last heartbeat details', 
                'scheduledEventId': task['scheduledEventId']}
            tasktimeout['timeoutType'] = timeouttype
            if 'startedEventId' in task:
                tasktimeout['startedEventId'] = task['startedEventId']
                del task['startedEventId']
                config.redis.del_timeout("Timeout.Activity.StartToClose", runId + ":" + taskToken)
                config.redis.del_timeout("Timeout.Activity.SchedToClose", runId + ":" + taskToken)
                config.redis.del_timeout("Timeout.Activity.heartbeat", runId + ":" + taskToken)
            else:
                config.redis.del_timeout("Timeout.Activity.SchedToStart", runId + ":" + taskToken)
                config.redis.del_timeout("Timeout.Activity.SchedToClose", runId + ":" + taskToken)
                config.redis.del_tasklist(task['taskList'], taskToken)
            config.events.add(task['runId'], "ActivityTaskTimedOut",tasktimeout)
        [scheduledEventId, taskToken] = add_decisionTask(task['runId'],wf_data)
        task['scheduledEventId'] = scheduledEventId
        task['taskList'] = wf_data['taskList']
        config.redis.save_task_current(taskToken, task)
        print "Done removing:adding new" + str(taskToken)
        return

#TODO Write CloseActivity Method

def add_decisionTask(RunId, wf_data):
    scheduledEventId = config.events.add(RunId,"DecisiontaskScheduled",
        {'startToCloseTimeout': wf_data['taskTimeout'],
            'taskList': {'name': wf_data['taskList']}})
    taskToken = str(config.redis.incr_tasktoken())
    config.redis.add_tasklist(wf_data['taskList'],
        taskToken) 
    return [scheduledEventId, str(taskToken)]

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/RegisterWorkFlowType", RegisterWorkFlowTypeHandler),
            (r"/RegisterActivityType", RegisterActivityTypeHandler),
            (r"/StartWorkflowExecution", StartWorkflowExecutionHandler),
            (r"/PollForDecisionTask", PollForDecisionTaskHandler),
            (r"/RespondDecisionTaskCompleted", RespondDecisionTaskCompletedHandler),
            (r"/PollForActivityTask", PollForActivityTaskHandler),
            (r"/RespondActivityTaskCompleted", RespondActivityTaskCompletedHandler),
            (r"/RespondActivityTaskCanceled", RespondActivityTaskCanceledHandler),
            (r"/RespondActivityTaskFailed", RespondActivityTaskFailedHandler),
            (r"/RecordActivityTaskHeartbeat", RecordActivityTaskHeartbeatHandler),
            (r"/TerminateWorkflowExecution", TerminateWorkflowExecutionHandler),
            (r"/RequestCancelWorkflowExecution", RequestCancelWorkflowExecutionHandler),
        ]
        settings = dict(
            autoescape=None,
        )
        tornado.web.Application.__init__(self, handlers, **settings)

class BaseHandler(tornado.web.RequestHandler):
    validator = validator()

    def get_input(self, body):
        input_json = self.request.body
        if not input_json: return None
        input_json = tornado.escape.json_decode(input_json)
        return input_json

class RegisterWorkFlowTypeHandler(BaseHandler):
    def post(self):
        input_json = self.get_input(self.request.body)
        config.workflow_provider.save_workflow_type(input_json)
        """ Save to Redis """
        config.redis.save_workflow_type(input_json["name"])

    def get(self):
        config.workflow_provider.get_workflow_type()

class RegisterActivityTypeHandler(BaseHandler):
    def post(self):
        input_json = self.get_input(self.request.body)
        config.workflow_provider.save_activity_type(input_json)
        config.redis.save_activity_type(input_json["name"])

    def get(self):
        config.workflow_provider.get_activity_type()

class StartWorkflowExecutionHandler(BaseHandler):
    def post(self):
        input_json = self.get_input(self.request.body)

        workflow_type_name = input_json["workflowType"]["name"]
        workflow_type = config.workflow_provider.get_workflow_type(input_json["workflowType"]["name"])
        if workflow_type is None:
            self.write("Workflowtype doesnt exist")
            return

        workflowId = input_json["workflowId"]

        if config.redis.check_workflow_id(workflowId):
            self.write("WorkflowIdAlreadyExists")
            #self.set_status(400)
            return

        if "taskList" in input_json and "name" in input_json["taskList"]:
            taskList = input_json["taskList"]["name"]
        else:
            taskList = workflow_type["taskList"]["name"]

        if "executionStartToCloseTimeout" in input_json:
            exec_timeout = input_json["executionStartToCloseTimeout"]
        else:
            exec_timeout = workflow_type["executionStartToCloseTimeout"]

        if "taskStartToCloseTimeout" in input_json:
            taskTimeout = input_json["taskStartToCloseTimeout"]
        else:
            taskTimeout = workflow_type["taskStartToCloseTimeout"]

        if "defaultChildPolicy" in input_json:
            childPolicy = input_json["defaultChildPolicy"]
        else:
            childPolicy = workflow_type["defaultChildPolicy"]

        """ Store this state """
        runId = str(config.redis.incr_runid())
        
        """ StartWFExec to event history """
        config.events.add(runId,"workflowExecutionStarted", input_json)

        wf_data = {}
        wf_data['workflowType'] = workflow_type_name 
        wf_data['workflowId'] = workflowId
        wf_data['taskTimeout'] = taskTimeout
        wf_data['taskList'] = taskList
        config.redis.save_workflow_current(runId, wf_data)
        
        [scheduledEventId, taskToken] = add_decisionTask(runId, wf_data)
        
        config.redis.save_workflow_id(workflowId, {"runId": runId, "State": "RUNNING"})
        
        task = {}
        task['type'] = "Decision" 
        task['runId'] = runId 
        task['workflowId'] = workflowId
        task['scheduledEventId'] = scheduledEventId
        task['taskList'] = taskList
        config.redis.save_task_current(taskToken, task)

        config.redis.save_task_open(runId, taskToken)
        """ Add it to timers for Workflow Execution """
        config.redis.save_timeout("Timeout.WFExecution", runId, datetime.now() +
                timedelta(seconds=int(exec_timeout)))

        self.write('{"runId":"'+ runId + '"}')

class PollForDecisionTaskHandler(BaseHandler):
    def post(self):
        input_json = self.get_input(self.request.body)

        if not "taskList" in input_json or not "name" in input_json["taskList"]:
            self.write("taskList not present")
            return 
        
        taskList = input_json["taskList"]["name"]
        taskToken = config.redis.pop_tasklist(taskList)
        #print tasklist_data
        if taskToken is not None:
            results = {}
            task = config.redis.get_task_current(taskToken)
            runId = task['runId']
            workflowId = task['workflowId']
            scheduledeventId = int(task['scheduledEventId'])
            
            """ Add Decision Started to Event History """
            startedEventId = config.events.add(runId,"DecisionTaskStarted", 
                    {'identity':input_json["identity"],'scheduledEventId':scheduledeventId})
            
            wf_data = config.redis.get_workflow_current(runId)

            events = config.workflow_provider.get_events_history(runId, 
                    input_json["reverseOrder"])
            results['events'] = events
            results['workflowExecution'] = { 'runId' : runId, 'workflowId' : workflowId }
            results['workflowType'] = wf_data['workflowType']
            results['startedEventId'] = startedEventId
            results['taskToken'] = taskToken

            """ Redis Populate task hash """
            task['startedEventId'] = startedEventId
            task['taskList'] = taskList
            config.redis.save_task_current(taskToken, task)

            """ Add it to timers for Decision TaskList """
            config.redis.save_timeout("Timeout.DecisionTask", 
                    runId + ":" + taskToken, datetime.now() +
                    timedelta(seconds=int(wf_data['taskTimeout'])))

            self.write(results)
        else:
            self.write("No Workflow Execution Ready")

class RespondDecisionTaskCompletedHandler(BaseHandler):
    def post(self):
        input_json = self.get_input(self.request.body)
        taskToken = input_json["taskToken"]
        task = config.redis.pop_task_current(str(taskToken))
        #print "RespondDecision:" + str(taskToken)
        #print task
        
        if not task:
            self.write("NoResourceFound")
            return

        """ Record decision task complete event """
        completedEventId = config.events.add(task['runId'], "DecisionTaskCompleted",
                {'executionContext':input_json["executionContext"],
                    'scheduledEventId':task['scheduledEventId'], 
                    'startedEventId':task['startedEventId']})

        """ Remove Decision Task Timer """
        config.redis.del_timeout("Timeout.DecisionTask",
                                    task['runId'] + ":" + str(taskToken))
        config.redis.del_task_open(task['runId'], taskToken)

        decisions = input_json["decisions"]
        for decision in decisions:
            if decision["decisionType"] == "ScheduleActivityTask":
                decisionAttr = decision["scheduleActivityTaskDecisionAttributes"]
                if not self.validator.activityType(decisionAttr["activityType"]["name"]):
                    self.write("UnknownResource")
                    return
                if decisionAttr["activityId"] is None:
                    self.write("activityId is required")
                    return
                if (decisionAttr["scheduleToCloseTimeout"] is None or
                    (decisionAttr["taskList"] is None) or
                    decisionAttr["taskList"]["name"] is None or
                    decisionAttr["scheduleToStartTimeout"] is None or
                    decisionAttr["startToCloseTimeout"] is None or
                    decisionAttr["heartbeatTimeout"] is None):
                        """ Read from ActivityType """
                        activitytype = config.workflow_provider.get_activity_type(decisionAttr["activityType"]["name"])
                        if decisionAttr["scheduleToCloseTimeout"] is None:
                            decisionAttr["scheduleToCloseTimeout"] = activitytype["defaultTaskScheduleToCloseTimeout"]
                        if decisionAttr["taskList"] is None:
                            decisionAttr["taskList"] = activitytype["defaultTaskList"]
                        if decisionAttr["scheduleToStartTimeout"] is None:
                            decisionAttr["scheduleToStartTimeout"] = activitytype["defaultTaskScheduleToStartTimeout"]
                        if decisionAttr["startToCloseTimeout"] is None:
                            decisionAttr["startToCloseTimeout"] = activitytype["defaultTaskStartToCloseTimeout"]
                        if decisionAttr["heartbeatTimeout"] is None:
                            decisionAttr["heartbeatTimeout"] = activitytype["defaultTaskHeartbeatTimeout"]

                taskscheduledEventId = config.events.add(task['runId'], "ActivityTaskScheduled",
                        decisionAttr)
                ActivitytaskToken = str(config.redis.incr_tasktoken())
                config.redis.add_tasklist(decisionAttr["taskList"]["name"], ActivitytaskToken)
                atask = {}
                atask['type'] = "Activity"
                atask['runId'] = task['runId']
                atask['workflowId'] = task['workflowId']
                atask['scheduledEventId'] = taskscheduledEventId
                atask['taskList'] = decisionAttr["taskList"]["name"]
                config.redis.save_task_current(ActivitytaskToken, atask)
                config.redis.save_task_open(task['runId'], ActivitytaskToken)

                """ Add it to timers for Activity Scheduled """
                config.redis.save_timeout("Timeout.Activity.SchedToClose", task['runId'] + 
                        ":" + ActivitytaskToken, 
                        datetime.now() + timedelta(
                            seconds=int(decisionAttr["scheduleToCloseTimeout"])))
                config.redis.save_timeout("Timeout.Activity.SchedToStart", task['runId'] + 
                        ":" + ActivitytaskToken,
                        datetime.now() + timedelta(
                            seconds=int(decisionAttr["scheduleToStartTimeout"])))
                        
            if decision["decisionType"] == "CompleteWorkflowExecution":
                decisionAttr = decision['CompleteWorkflowExecutionDecisionAttributes'] 
                """ Add to event """
                WFCompletionEventId = config.events.add(task['runId'], 
                        "WorkflowExecutionCompleted",
                        {'result':decisionAttr['result'],
                            'scheduledEventId':task['scheduledEventId'],
                            'decisionTaskCompletedEventId':completedEventId})
                """ Remove Timers TODO: What if some orphaned worker task still running"""
                config.redis.del_timeout("Timeout.WFExecution", task['runId'])
                """ Clean up Redis Data """
                config.redis.del_workflow_current(task["runId"])
                config.redis.remkey_task_open(task["runId"]) #Canbe more aggressive to remove orphaned
                #Assuming no outstanding task, nothing to be done for task_current
                config.redis.save_workflow_id(task['workflowId'], {"runId": task["runId"], "State": "COMPLETED"})
                config.redis.del_eventid(task['runId'])

            if decision["decisionType"] == "RequestCancelActivityTask":
                """TODO """

            if decision["decisionType"] == "FailWorkflowExecution":
                """TODO"""

            if decision["decisionType"] == "CancelWorkflowExecution":
                """TODO"""

            if decision["decisionType"] == "ContinueAsNewWorkflowExecution":
                """TODO"""

            if decision["decisionType"] == "StartTimer":
                """TODO"""

            if decision["decisionType"] == "CancelTimer":
                """TODO"""

            if decision["decisionType"] == "SignalExternalWorkflowExecution":
                """TODO"""

            if decision["decisionType"] == "RequestCancelExternalWorkflowExecution":
                """TODO"""

            if decision["decisionType"] == "StartChildWorkflowExecution":
                """TODO"""

class PollForActivityTaskHandler(BaseHandler):
    def post(self):
        input_json = self.get_input(self.request.body)

        if not "taskList" in input_json or not "name" in input_json["taskList"]:
            self.write("taskList not present")
            return
        
        tasklist = input_json["taskList"]["name"]
        taskToken = config.redis.pop_tasklist(tasklist)

        if taskToken is not None:
            task = config.redis.get_task_current(taskToken)
            if not task:
                self.write("NoResourceFound")
                return
            runId = task['runId']
            workflowId = task['workflowId']
            scheduledeventId = int(task['scheduledEventId'])

            taskScheduledevent = config.workflow_provider.get_event(runId, scheduledeventId)
            if taskScheduledevent is not None:
                results = {}
                taskScheduledAttr = taskScheduledevent['ActivityTaskScheduledEventAttributes']
                startedEventId = config.events.add(runId, "ActivityTaskStarted",
                        {'identity':input_json["identity"],'scheduledEventId':scheduledeventId})

                results['activityId'] = taskScheduledAttr['activityId']
                results['activityType'] = taskScheduledAttr['activityType']
                results['input'] = taskScheduledAttr['input']
                results['workflowExecution'] = { 'runId' : runId, 'workflowId' : workflowId }
                results['taskToken'] = taskToken
                results['startedEventId'] = startedEventId

                """ Redis Populate task hash(when taken by Worker or Decider) """
                task['Type'] = "Activity"
                task['activityType'] = taskScheduledAttr['activityType']
                task['startedEventId'] = startedEventId
                task['taskList'] = tasklist
                config.redis.save_task_current(taskToken, task)

                """ Add it to timers for Activity Started """
                print "Adding to timers"
                config.redis.save_timeout("Timeout.Activity.StartToClose", runId + ":" + str(taskToken),
                    datetime.now() + timedelta(seconds=int(taskScheduledAttr["startToCloseTimeout"])))

                config.redis.save_timeout("Timeout.Activity.heartbeat", runId + ":" + str(taskToken),
                        datetime.now() + timedelta(seconds=int(taskScheduledAttr["heartbeatTimeout"])))

                """ Remove SchedToStart Timer """
                config.redis.del_timeout("Timeout.Activity.SchedToStart", runId + ":" + str(taskToken))
                self.write(results)
            else:
                self.write("No Task Scheduled")
                return
        else:
            self.write("No Task Ready")
            return

#TODO
class RecordActivityTaskHeartbeatHandler(BaseHandler):
    def post(self):
        return







class RespondActivityTaskCompletedHandler(BaseHandler):
    def post(self):
        input_json = self.get_input(self.request.body)
        taskToken = input_json["taskToken"]
        task = config.redis.pop_task_current(str(taskToken))
        if not task:
            self.write("NoResourceFound")
            return

        taskComplete = {}
        if 'result' in input_json:
            taskComplete['result'] = input_json['result']
        taskComplete['scheduledEventId'] = task['scheduledEventId']
        taskComplete['startedEventId'] = task['startedEventId']

        """ Record Activity task complete event """
        completedEventId = config.events.add(task['runId'], 
                "ActivityTaskCompleted", taskComplete)

        """ Remove timers related to Activity """
        config.redis.del_timeout("Timeout.Activity.SchedToClose", task['runId'] +
                ":" + str(taskToken))
        config.redis.del_timeout("Timeout.Activity.StartToClose", task['runId'] + 
                ":" + str(taskToken))
        config.redis.del_timeout("Timeout.Activity.heartbeat", task['runId'] + 
                ":" + str(taskToken))

        config.redis.del_task_open(task['runId'], taskToken)

        """ Add to Decision TaskList """ 
        wf_data = config.redis.get_workflow_current(task['runId'])
        [scheduledEventId, taskToken] = add_decisionTask(task['runId'], wf_data)
        dtask = {}
        dtask['type'] = "Decision"
        dtask['runId'] = task['runId']
        dtask['workflowId'] = task['workflowId']
        dtask['scheduledEventId'] = scheduledEventId
        dtask['taskList'] = wf_data['taskList']
        config.redis.save_task_current(taskToken, dtask)
        config.redis.save_task_open(task['runId'], taskToken)

class RespondActivityTaskCanceledHandler(BaseHandler):
    def post(self):
        self.write("canceled")

class RespondActivityTaskFailedHandler(BaseHandler):
    def post(self):
        return




class TerminateWorkflowExecutionHandler(BaseHandler):
    def post(self):
        input_json = self.get_input(self.request.body)
        runId = input_json['runId']
        workflowId = input_json['workflowId']
        workflowid_data = config.redis.get_workflow_id(workflowId)

        if (not workflowid_data or workflowid_data['State'] != "RUNNING"):
            self.write("UnKnownResource")
            return
        config.events.add(runId, "WorkflowExecutionTerminated",
                {'cause': 'OPERATOR_INITIATED', 'ChildPolicy': 'TERMINATE',
                    'details': input_json['details'], 'reason': input_json['reason']})
        terminateworkflowexecution(runId,"TERMINATE")

class RequestCancelWorkflowExecutionHandler(BaseHandler):
    def post(self):
        input_json = self.get_input(self.request.body)
        runId = input_json['runId']
        workflowId = input_json['workflowId']
        workflowid_data = config.redis.check_workflow_id(workflowId)

        if (workflowid_data is None or workflowid_data['State'] != "RUNNING"):
            self.write("UnKnownResource")
        config.events.add(runId, "WorkflowExecutionCancelRequested",
                {'cause': 'CHILD_POLICY_APPLIED', 'externalInitiatedEventId': '123456',
                    'externalWorkflowExecution': {'runId': runId, 'workflowId': workflowId}})


if __name__ == "__main__":
    main()
