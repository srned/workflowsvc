import mongoprovider

class WorkFlowPyDataProvider(object):

	@staticmethod
	def get_provider():
		return mongoprovider.WorkFlowPyProvider()
