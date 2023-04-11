import logging
from grpcgraph import graph_pb2
from grpcgraph import graph_pb2_grpc
import grpc
from concurrent import futures
import time
import collections
import graph_tool.all as gt
import numpy as np
import logging
import psutil
import os

def setup_logging():
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger('my_module')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger

logger = setup_logging()

GRPC_URI= '0.0.0.0:50052'

# class Hello(rpc_pb2_grpc.HelloServiceServicer):
#     # 实现 proto 文件中定义的 rpc 调用
#     def Hello(self, request, context):
#         return rpc_pb2.String(value = 'hello {msg}'.format(msg = request.value))


def get_memory() -> int:
    pid = os.getpid()
    p = psutil.Process(pid)
    info = p.memory_full_info()
    return info.uss / 1024. / 1024. / 1024.

ExitFlag = 0 
CurrentCount = 0

class DirectGraphDistance(graph_pb2_grpc.DirectGraphDistanceServiceServicer):
    # 实现 proto 文件中定义的 rpc 调用
    def DirectGraphDistance(self, request, context):
        memoryStat = get_memory()
        graph = gt.Graph(directed=True)
        global CurrentCount
        CurrentCount += 1

        edges  = []
        for each in request.edges:
            edges.append([each.sourceID,each.targetID])

        # logger.info("接到新任务，开始添加边")
        graph.add_edge_list(edges,hashed=True,hash_type="long")
        # graph.add_edge_list(edges,hashed=False)

        # 去除重复边
        gt.remove_parallel_edges(graph)
        # 获得最大联通子图
        # component_graph = gt.extract_largest_component(graph)
        component_graph = graph

        result = graph_pb2.GraphStats()

        result.edgeCount = component_graph.num_edges()
        result.nodeCount = component_graph.num_vertices()
        print(result.edgeCount, result.nodeCount)

        # '''获取图谱的平均最短距离'''123
        if result.nodeCount <= 500000:
            logger.info("开始计算 short distance")
            
            dist_type="int8_t"
            all_sp = graph.new_vertex_property("vector<%s>" % dist_type)
            gt.shortest_distance(component_graph,directed=True,dist_map=all_sp)

            unique_counts = collections.defaultdict(int)
            # total_distance = 0
            # total_path = 0

            ndArray = all_sp.get_2d_array(np.arange(0,graph.num_vertices()))

            ndArray = ndArray[(ndArray != 255) & (ndArray != 0)]
            total_distance = np.sum(ndArray)
            total_path = ndArray.size

            unique, counts = np.unique(ndArray, return_counts=True)

            for i in range(len(unique)):
                result.frequency.append(graph_pb2.FrequencyItem(values=[unique[i],counts[i]]))

            # for item in all_sp:
            #     ndArray = item.a
            #     ndArray = ndArray[(ndArray != 255) & (ndArray != 0)]

            #     total_distance+= np.sum(ndArray)
            #     total_path += ndArray.size

            #     unique, counts = np.unique(ndArray, return_counts=True)
            #     for i in range(unique.size):
            #         unique_counts[int(unique[i])] += int(counts[i])

            # for k,v in unique_counts.items():
            #     result.frequency.append(graph_pb2.FrequencyItem(values=[k,v]))

            result.distanceCount = int(total_distance)
            result.pathCount = int(total_path)
            if int(total_path) != 0:
                result.ASD = float(total_distance/total_path)
                result.EnableAsd = True
            else:
                result.ASD = -1
                result.EnableAsd = False
        else:
            result.ASD = -1
            result.EnableAsd = False 

        # '''获取图谱的平均聚集系数'''    
        logger.info("开始计算 local_clustering")
        try:
            cluster = gt.local_clustering(component_graph)
            vertex_avgs =gt.vertex_average(component_graph, cluster)
            logger.info(vertex_avgs)
            result.CC = float(vertex_avgs[0])
        except Exception as e:
            result.CC=-1
            logger.info(e)

        return result

        logger.info("数据完成发送")

        # return graph_pb2.Matrix(M=RetList)
        # 一次计算内存使用超过 15 GB 的网络重启服务
        if get_memory() - memoryStat > 300:
            logger.info("超过限制，即将重启")
            global ExitFlag
            ExitFlag = 1
        CurrentCount -= 1

MAX_MESSAGE_LENGTH = 1024 * 1024 * 1024
# 定义开启4个线程处理接收到的请求
server = grpc.server(futures.ThreadPoolExecutor(max_workers=1),options=[
               ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
               ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH)])
# 将编译出来的rpc_pb2_grpc的add_HelloServiceServicer_to_server函数添加到server中
graph_pb2_grpc.add_DirectGraphDistanceServiceServicer_to_server(DirectGraphDistance(), server)
# rpc_pb2_grpc.add_HelloServiceServicer_to_server(Hello(), server)
 
# 定义服务端端口1234
server.add_insecure_port(GRPC_URI)
server.start()

logger.info("start")
# 长期监听
try:
    while True:
        time.sleep(50)
        # 暂时取消退出判断
        # if ExitFlag == 1 and CurrentCount == 0:
        #     server.stop(0)
        #     break
except KeyboardInterrupt:
    server.stop(0)
