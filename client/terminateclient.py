#!/usr/bin/python
import tornado.httpclient
import tornado.ioloop
import tornado.auth
import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import json
import random
import sys

http_client = tornado.httpclient.AsyncHTTPClient()
TestCaseNo = 0
TestCase = ""
TestData = {'executionStartToCloseTimeout': '80', 'taskStartToCloseTimeout': '50',
        'scheduleToCloseTimeout': '20', 'startToCloseTimeout': '10',
        'heartbeatTimeout': '5', 'scheduleToStartTimeout': 10},{}
runId = ""
workflowId = ""

def main():
    API_Data = {}
    API_Data = [['RegisterWorkFlowType', 'POST', '{"domain": "23534",\
                        "name": "customerOrderWorkflow",\
                        "description": "Handle customer orders",\
                        "defaultTaskStartToCloseTimeout": "600",\
                        "defaultExecutionStartToCloseTimeout": "3600",\
                        "defaultTaskList":{"name": "mainTaskList"},\
                        "defaultChildPolicy": "TERMINATE"}'],
                 ['RegisterActivityType', 'POST', '{"domain": "867530901",\
                        "name": "activityVerify",\
                        "description": "Verify the customer credit card",\
                        "defaultTaskStartToCloseTimeout": "600",\
                        "defaultTaskHeartbeatTimeout": "120",\
                        "defaultTaskList":{"name": "mainTaskList"},\
                        "defaultTaskScheduleToStartTimeout": "300",\
                        "defaultTaskScheduleToCloseTimeout": "900"}']]

    h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
    h.add("Accept", "application/json")
    regwf_request = tornado.httpclient.HTTPRequest(url='http://localhost:8888/'+
            API_Data[0][0], method=API_Data[0][1],body=API_Data[0][2],headers=h)
    regactivity_request = tornado.httpclient.HTTPRequest(url='http://localhost:8888/' +
            API_Data[1][0], method=API_Data[1][1],body=API_Data[1][2],headers=h)

    http_client.fetch(regactivity_request, RegisterActivity)
    http_client.fetch(regwf_request, generic_handler)

    decider = tornado.ioloop.PeriodicCallback(PollForDecisionTask, 1000)
    decider.start()

    worker = tornado.ioloop.PeriodicCallback(PollForActivityTask, 1000)
    worker.start()

    tornado.ioloop.IOLoop.instance().start()

def make_request(Action, Method, Body, Callback):
    h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
    h.add("Accept", "application/json")
    request = tornado.httpclient.HTTPRequest(url='http://localhost:8888/' +
            Action, method=Method, body=Body, headers=h)
    http_client.fetch(request, Callback)

def RegisterActivity(response):
    if response.error:
        print response.request.url
        print "Error:", response.error
        assert False
    else:
        try:
            print response.request.url
            output_json = json.loads(response.body)
            print json.dumps(output_json, sort_keys=True, indent=4)
        except ValueError:
            print response.body
        StartWorkflowExecution()

def generic_handler(response):
    if response.error:
        print response.request.url
        print "Error:", response.error
    else:
        try:
            print response.request.url
            output_json = json.loads(response.body)
            print json.dumps(output_json, sort_keys=True, indent=4)
        except ValueError:
            print response.body
            if TestCase == "TERMINATE":
                print "All Test Success"
                sys.exit()

def StartWorkflowExecution():
    global workflowId
    svcinput = {"domain": "867530901",
            "workflowType": {"name": "customerOrderWorkflow"},
            "taskList":{"name": "specialTaskList"},
            "input": "TestCase"}
    svcinput['workflowId'] = "TestCase:" + str(TestCaseNo) + ":" +str(random.randint(1,10000))
    svcinput['executionStartToCloseTimeout'] = TestData[0]['executionStartToCloseTimeout']
    svcinput['taskStartToCloseTimeout'] = TestData[0]['taskStartToCloseTimeout']
    workflowId = svcinput['workflowId']
    make_request('StartWorkflowExecution', 'POST', json.dumps(svcinput), StartWFCallback)

def StartWFCallback(response):
    if response.error:
        print response.request.url
        print "Error:", response.error
        assert False
    else:
        global runId
        print response.request.url
        runId = json.loads(response.body)['runId']
        assert runId is not None
        print "WorkflowExecution Started with:" + str(runId)
        

def add_decisions(decisionType):
    decision = {}
    if decisionType == "ScheduleActivityTask":
        decision = {"decisionType": "ScheduleActivityTask",
                "scheduleActivityTaskDecisionAttributes":{
                    "activityType":{"name": "activityVerify",
                        "version": "1.0"},"activityId": "verification-27",
                    "control": "digital music","input": "5634-0056-4367-0923,12/12,437",
                    "taskList":{"name": "mainTaskList"}}}
        decision["scheduleActivityTaskDecisionAttributes"]["scheduleToCloseTimeout"] = TestData[0]["scheduleToCloseTimeout"]
        decision["scheduleActivityTaskDecisionAttributes"]["scheduleToStartTimeout"] = TestData[0]["scheduleToStartTimeout"]
        decision["scheduleActivityTaskDecisionAttributes"]["startToCloseTimeout"] = TestData[0]["startToCloseTimeout"]
        decision["scheduleActivityTaskDecisionAttributes"]["heartbeatTimeout"] = TestData[0]["heartbeatTimeout"]
    elif decisionType == "CompleteWorkflowExecution":
        decision = {"decisionType": "CompleteWorkflowExecution",
                "CompleteWorkflowExecutionDecisionAttributes":{
                    "result":"CompleteOrder"}}
    elif decisionType == "WorkflowExecutionTerminated":
        return decision
    return decision


def PollForDecisionTask():
    svcinput = {"domain": "867530901","taskList": 
            {"name": "specialTaskList"},
            "identity": "Decider01","maximumPageSize": 50,
            "reverseOrder": "True"}
    make_request('PollForDecisionTask','POST',json.dumps(svcinput), decisiontaskhandler)

def decisiontaskhandler(response):
    if response.error:
        print response.request.url
        print "Error:", response.error
    else:
        try:
            global TestCaseNo
            print response.request.url
            output_json = json.loads(response.body)
            svcinput = {}
            #print json.dumps(output_json, sort_keys=True, indent=4)
            Events = output_json['events']
            for event in Events:
                if event['eventType'] in ["DecisionTaskStarted", "DecisiontaskScheduled",
                        "DecisionTaskCompleted", "ActivityTaskScheduled","ActivityTaskStarted"]:
                    continue

                if event['eventType'] == "workflowExecutionStarted":
                    print "workflowExecutionStarted"
                    svcinput['decisions'] = [add_decisions("ScheduleActivityTask")]
                    svcinput['executionContext'] = "TestCase:" + str(TestCaseNo)
                    svcinput['taskToken'] = int(output_json['taskToken'])
                    make_request('RespondDecisionTaskCompleted', 'POST', json.dumps(svcinput), 
                            generic_handler)
                    TerminateworkflowExecution()
                    break
                elif event['eventType'] == "ActivityTaskCompleted":
                    print "ActivityTaskCompleted"
                    #print json.dumps(output_json, sort_keys=True, indent=4)
                    svcinput['decisions'] = [add_decisions("CompleteWorkflowExecution")]
                    svcinput['executionContext'] = "TestCaseComplete:" + str(TestCaseNo)
                    svcinput['taskToken'] = int(output_json['taskToken'])
                    TestCaseNo = TestCaseNo + 1
                    make_request('RespondDecisionTaskCompleted', 'POST', json.dumps(svcinput),
                            generic_handler)
                    break
        except ValueError:
            print response.body


def PollForActivityTask():
    svcinput = {"domain": "867530901", "taskList":{"name": "mainTaskList"},
            "identity": "VerifyCreditCardWorker01"}
    make_request('PollForActivityTask', 'POST', json.dumps(svcinput), Activitytaskhandler)

def Activitytaskhandler(response):
    if response.error:
        print response.request.url
        print "Error:", response.error
    else:
        try:
            svcinput = {}
            print response.request.url
            output_json = json.loads(response.body)
            #print json.dumps(output_json, sort_keys=True, indent=4)
            if 'taskToken' in output_json:
                if output_json['activityType']['name'] == "activityVerify":
                    svcinput['result'] = "Customer Card Verified"
                    svcinput['taskToken'] = int(output_json['taskToken'])
                    make_request('RespondActivityTaskCompleted','POST', json.dumps(svcinput),generic_handler)
        except ValueError:
            print response.body

def TerminateworkflowExecution():
    global TestCase
    svcinput = {"domain": "867530901", "reason": "Testing Termination", 
            "details": "Hope it gets terminated", "childPolicy": "TERMINATE"}
    svcinput['workflowId'] = workflowId
    svcinput['runId'] = runId
    TestCase = "TERMINATE"
    make_request('TerminateWorkflowExecution', 'POST', json.dumps(svcinput), generic_handler)


if __name__ == "__main__":
    main()
