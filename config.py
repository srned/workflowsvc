from dataprovider.dataprovider import WorkFlowPyDataProvider
from dataprovider.redisprovider import RedisProvider
import events

#Global Handlers
redis = RedisProvider()
workflow_provider = WorkFlowPyDataProvider.get_provider()
events = events.Events()

