from data_node import DataNode
import falcon
from falcon import Request, Response

node = DataNode()

class mkfs:
    def on_post(self, req: Request, resp: Response):
        pass

api = falcon.API()
api.add_route('/', Resourceeee(data))
