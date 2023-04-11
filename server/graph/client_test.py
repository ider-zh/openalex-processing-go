import grpc
from grpcgraph import graph_pb2_grpc
from grpcgraph import graph_pb2

def run():
    with grpc.insecure_channel('192.168.50.3:50052') as channel:
        stub = graph_pb2_grpc.DirectGraphDistanceServiceStub(channel)
        edges = []
        for row in [[1,2],[3,4],[4,2],[3,2],[1,6],[4,6]]:
            edges.append(graph_pb2.Edge(sourceID=row[0],targetID=row[1]))
        response = stub.DirectGraphDistance(graph_pb2.Graph(edges=edges))
        print("Greeter client received: " , response)

run()


# import graph_tool.all as gt
# import numpy as np

# graph = gt.Graph(directed=True)

# graph.add_edge_list([[1,2],[3,4],[4,2],[3,2],[1,6],[4,6]],hashed=False)

# dist_type="int8_t"
# sp = graph.new_vertex_property("vector<%s>" % dist_type)

# gt.shortest_distance(graph,directed=True,dist_map=sp)

# import pdb
# pdb.set_trace()
# for i in sp:
#     print(i.a)

# f[(f != 255) & (f != 0)]