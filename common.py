from redis import Redis
from service_pools import ServicePools
from service import Service

redis = Redis()
service = Service(redis)
service_pools = ServicePools(service)
