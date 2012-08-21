import os.path
import sys
import config
#from dataprovider.dataprovider import WorkFlowPyDataProvider

class validator(object):
	#workflow_provider = WorkFlowPyDataProvider.get_provider()

	def __init__(self):
		return

	def workflowId(self, workflowId):
		if config.redis.check_workflow_key(workflowId) == 0:
			return False
		else:
			return True

	def workflowType(self, workflowType):
		if config.redis.check_workflow_type(workflowType) == 0:
			return False
		else:
			return True

	def activityType(self, activityType):
		if config.redis.check_activity_type(activityType) == 0:
			return False
		else:
			return True



