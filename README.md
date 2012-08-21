# workflowsvc

The Python Clone of Amazon Simple Workflow Service (http://aws.amazon.com/swf/).

## Introduction
workflowsvc implements minimal set of the APIs supported by Simple Workflow Service. 
It is partly intended as a learning experience. 

## Dependencies
* tornado pip install tornado
* redis.py pip install redis
* mongodb pip install pymongo

## API Reference
Amazon Simple Workflow Service helps coordinate work across distributed components. 
A task is a logical unit of work that is accomplished by a worker. A decider component 
arrives at decisions that determines the state of the execution and creates activity tasks 
that will be queued on tasklist which will be then be assigned to workers. 
API support for creation and deletion of activity and workflow types gives great 
flexibility. Inspired by the simplicity of the approach, In this project, 
I have tried to clone subset of features provided by SWF. 
Developer Guide and API Reference is great place to know more (http://aws.amazon.com/documentation/swf/)

### Supported Actions
* RegisterWorkFlowType
* RegisterActivityType
* StartWorkflowExecution
* TerminateWorkflowExecution
* RequestCacnelWorkflowExecution
* PollForDecisionTask
* RespondDecisionTaskCompleted
* PollForActivityTask
* RespondActivityTaskCompleted

### Timeouts
Timeouts for Workflow Execution, Decision Task Start to Close, 
Activity Task SchedToClose, SchedToStart, StartToClose are completely "honored"

## Starting workflowsvc
Before starting the service, a redis server and mongodb has to be started.
Start the workflow service, workflowsvc.py,

### Sample clients
client/simpleclient.py:
This client will cover complete the life cycle of a workflow execution by registering workflow, 
activity types and starting a workflow execution for which a decider will create a task that will be 
completed by a worker. Finally, the decider will mark the execution as completed

client/timeoutclient.py:
This client will run workflow executions that tests different timeouts honored by the workflowsvc

client/terminateclient.py:
This client covers terminating workflow execution abruptly

## TODO
* Visibility Actions
* Child Workflow
* Timers
* Domains
* Task Cancellation
* Markers, Tags
* Signals
* Versioning
