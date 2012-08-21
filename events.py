import os.path
import sys
import json
from datetime import datetime, timedelta
import config

class Events(object):

	def __init__(self):
		return

	def event_header(self, runId, eventId, eventType, eventAttributes):
		data = {}
		data['RunId'] = runId
		data['eventId'] = eventId
		data['eventTimestamp'] = int(datetime.now().strftime('%s'))
		data['eventType'] = eventType
		data[eventType + 'EventAttributes'] = eventAttributes
		return data

	def workflowExecutionStarted(self, runId, eventAttributes):
		eventId = config.redis.incr_eventid(runId)
		config.workflow_provider.save_events_history(self.event_header(runId, eventId,"workflowExecutionStarted", eventAttributes))
		return eventId

	def workflowDecisiontaskScheduled(self, runId, eventAttributes):
		eventId = config.redis.incr_eventid(runId)
		config.workflow_provider.save_events_history(self.event_header(runId, eventId,"DecisionTaskScheduled", eventAttributes))
		return eventId

	def add(self, runId, eventType, eventAttributes):
		eventId = config.redis.incr_eventid(runId)
		config.workflow_provider.save_events_history(self.event_header(runId, eventId,eventType, eventAttributes))
		return eventId





