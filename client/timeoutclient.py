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
import ast
import random
import time
import sys
from datetime import datetime, timedelta

http_client = tornado.httpclient.AsyncHTTPClient()
TestCaseNo = 0
TestData = {'executionStartToCloseTimeout': '5', 'taskStartToCloseTimeout': '50',
            'scheduleToCloseTimeout': '20', 'startToCloseTimeout': '10',
            'heartbeatTimeout': '5', 'scheduleToStartTimeout': '10'},{'executionStartToCloseTimeout': '80', 'taskStartToCloseTimeout': '5',
            'scheduleToCloseTimeout': '10', 'startToCloseTimeout': '5',
            'heartbeatTimeout': '1000', 'scheduleToStartTimeout': '20'}
ActivityTimeoutTest = ""
runId = ""
vLoop = None

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

    #decider = tornado.ioloop.PeriodicCallback(PollForDecisionTask, 1000)
    #decider.start()

    #worker = tornado.ioloop.PeriodicCallback(PollForActivityTask, 1000)
    #worker.start()
    global vLoop
    vLoop = tornado.ioloop.IOLoop.instance()
    vLoop.start()

def make_request(Action, Method, Body, Callback):
    h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
    h.add("Accept", "application/json")
    request = tornado.httpclient.HTTPRequest(url='http://localhost:8888/' +
            Action, method=Method, body=Body, headers=h)
    http_client.fetch(request, Callback)

def RegisterActivity(response):
    if response.error:
        print response.request.url.split('/')[-1]
        print "Error:", response.error
        assert False
    else:
        try:
            print response.request.url.split('/')[-1]
            #output_json = json.loads(response.body)
            #print json.dumps(output_json, sort_keys=True, indent=4)
        except ValueError:
            print response.body
        StartWorkflowExecution()

def generic_handler(response):
    if response.error:
        print response.request.url.split('/')[-1]
        print "Error:", response.error
    else:
        try:
            print response.request.url.split('/')[-1]
            assert (response.body == "")
            if TestCaseNo == 2:
                print "All test complete"
                sys.exit()
            #output_json = json.loads(response.body)
            #print json.dumps(output_json, sort_keys=True, indent=4)
        except ValueError:
            print response.body

def noresource_handler(response):
    if response.error:
        print response.request.url.split('/')[-1]
        print "Error:", response.error
    else:
        print response.request.url.split('/')[-1]
        assert (response.body == "NoResourceFound")
        PollForDecisionTask()
        #print response.body

def StartWorkflowExecution():
    svcinput = {"domain": "867530901",
            "workflowType": {"name": "customerOrderWorkflow"},
            "taskList":{"name": "specialTaskList"},
            "input": "TestCase"}
    svcinput['workflowId'] = "TestCase:" + str(TestCaseNo) + ":" +str(random.randint(1,10000))
    svcinput['executionStartToCloseTimeout'] = TestData[TestCaseNo]['executionStartToCloseTimeout']
    svcinput['taskStartToCloseTimeout'] = TestData[TestCaseNo]['taskStartToCloseTimeout']
    make_request('StartWorkflowExecution', 'POST', json.dumps(svcinput), StartWFCallback)
    if TestCaseNo == 0:
# Simulate WF Execution timeout
        print "Testing executionStartToCloseTimeout"
        vLoop.add_timeout(timedelta(seconds=int(TestData[TestCaseNo]['executionStartToCloseTimeout']) + 2),
            PollForDecisionTask)
    elif TestCaseNo == 1:
        vLoop.add_timeout(timedelta(seconds=5),PollForDecisionTask)


def StartWFCallback(response):
    if response.error:
        print response.request.url.split('/')[-1]
        print "Error:", response.error
        assert False
    else:
        print response.request.url.split('/')[-1]
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
        decision["scheduleActivityTaskDecisionAttributes"]["scheduleToCloseTimeout"] = TestData[TestCaseNo]["scheduleToCloseTimeout"]
        decision["scheduleActivityTaskDecisionAttributes"]["scheduleToStartTimeout"] = TestData[TestCaseNo]["scheduleToStartTimeout"]
        decision["scheduleActivityTaskDecisionAttributes"]["startToCloseTimeout"] = TestData[TestCaseNo]["startToCloseTimeout"]
        decision["scheduleActivityTaskDecisionAttributes"]["heartbeatTimeout"] = TestData[TestCaseNo]["heartbeatTimeout"]
    elif decisionType == "CompleteWorkflowExecution":
        decision = {"decisionType": "CompleteWorkflowExecution",
                "CompleteWorkflowExecutionDecisionAttributes":{
                    "result":"CompleteOrder"}}
    return decision


def PollForDecisionTask():
    svcinput = {"domain": "867530901","taskList": 
            {"name": "specialTaskList"},
            "identity": "Decider01","maximumPageSize": 50,
            "reverseOrder": "True"}
    make_request('PollForDecisionTask','POST',json.dumps(svcinput), decisiontaskhandler)

def decisiontaskhandler(response):
    if response.error:
        print response.request.url.split('/')[-1]
        print "Error:", response.error
    else:
        try:
            global TestCaseNo
            global ActivityTimeoutTest
            #print response.request.url.split('/')[-1]
            if TestCaseNo == 0:
                assert response.body == "No Workflow Execution Ready"
                print "Passed Test 0"
                TestCaseNo = TestCaseNo + 1
                #print "TESTCASE:" + str(TestCaseNo)
                print "Starting Test 1"
                StartWorkflowExecution() #Start next Test Case
                return 

            output_json = json.loads(response.body)
            svcinput = {}
            #print json.dumps(output_json, sort_keys=True, indent=4)
            Events = output_json['events']
            for event in Events:
                if event['eventType'] in ["DecisionTaskStarted", "DecisiontaskScheduled",
                        "DecisionTaskCompleted", "ActivityTaskScheduled","ActivityTaskStarted"]:
                    continue

                if event['eventType'] == "workflowExecutionStarted":
                    print "DecisionHandler:workflowExecutionStarted"
                    svcinput['decisions'] = [add_decisions("ScheduleActivityTask")]
                    svcinput['executionContext'] = "TestCase:" + str(TestCaseNo)
                    svcinput['taskToken'] = int(output_json['taskToken'])
                    if TestCaseNo == 1:
                        time.sleep(int(TestData[TestCaseNo]['taskStartToCloseTimeout']) + 1)
                        print "Testing Decision Task Timeout"
                        make_request('RespondDecisionTaskCompleted', 'POST', json.dumps(svcinput), 
                            noresource_handler)
                    break
                if event['eventType'] == "DecisionTaskTimedOut":
                    print "DecisionHandler:DecisionTaskTimedOut"
                    svcinput['decisions'] = [add_decisions("ScheduleActivityTask")]
                    svcinput['executionContext'] = "TestCase:" + str(TestCaseNo)
                    svcinput['taskToken'] = int(output_json['taskToken'])
                    if TestCaseNo == 1:
                        make_request('RespondDecisionTaskCompleted', 'POST', json.dumps(svcinput),
                                generic_handler)
                        print "Testing Activity Related Timers"
                        ActivityTimeoutTest = "START_TO_CLOSE"
                        vLoop.add_timeout(timedelta(seconds=3),PollForActivityTask)
                    break
                if event['eventType'] == "ActivityTaskTimedOut":
                    print "DecisionHandler:ActivityTaskTimedOut"
                    svcinput['decisions'] = [add_decisions("ScheduleActivityTask")]
                    svcinput['executionContext'] = "TestCase:" + str(TestCaseNo)
                    svcinput['taskToken'] = int(output_json['taskToken'])
                    if TestCaseNo == 1:
                        make_request('RespondDecisionTaskCompleted', 'POST', json.dumps(svcinput),
                                generic_handler)
                        if ActivityTimeoutTest == "START_TO_CLOSE":
                            print "Testing SCHED_TO_CLOSE"
                            ActivityTimeoutTest = "SCHED_TO_CLOSE"
                            vLoop.add_timeout(timedelta(seconds=int(TestData[TestCaseNo]['scheduleToCloseTimeout']) + 1),
                                PollForActivityTask)
                        elif ActivityTimeoutTest == "SCHED_TO_CLOSE":
                            ActivityTimeoutTest = "COMPLETE"
                            vLoop.add_timeout(timedelta(seconds=2),PollForActivityTask)
                    break

                elif event['eventType'] == "ActivityTaskCompleted":
                    print "DecisionHandler:ActivityTaskCompleted"
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
        print response.request.url.split('/')[-1]
        print "Error:", response.error
    else:
        try:
            svcinput = {}
            print response.request.url.split('/')[-1]
            if TestCaseNo == 1 and ActivityTimeoutTest == "SCHED_TO_CLOSE":
                assert (response.body == "No Task Ready")
                print "Success SCHED_TO_CLOSE"
                PollForDecisionTask()
                return

            output_json = json.loads(response.body)
            #print json.dumps(output_json, sort_keys=True, indent=4)
            if 'taskToken' in output_json:
                if output_json['activityType']['name'] == "activityVerify":
                    svcinput['result'] = "Customer Card Verified"
                    svcinput['taskToken'] = int(output_json['taskToken'])
                    if TestCaseNo == 0:
                        make_request('RespondActivityTaskCompleted','POST', json.dumps(svcinput),
                                generic_handler)
                    elif TestCaseNo == 1 and ActivityTimeoutTest == "START_TO_CLOSE":
                        time.sleep(int(TestData[TestCaseNo]['startToCloseTimeout']) + 2)
                        print "Testing Activity START_TO_CLOSE"
                        make_request('RespondActivityTaskCompleted','POST', json.dumps(svcinput),
                                noresource_handler)
                        print "Success START_TO_CLOSE"
                    elif TestCaseNo == 1 and ActivityTimeoutTest == "COMPLETE":
                        print "Testing COMPLETE"
                        make_request('RespondActivityTaskCompleted','POST', json.dumps(svcinput),
                                generic_handler)
                        #time.sleep(2)
                        vLoop.add_timeout(timedelta(seconds=2),PollForDecisionTask)

        except ValueError:
            print response.body

if __name__ == "__main__":
    main()
