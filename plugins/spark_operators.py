from airflow.models.baseoperator import BaseOperator
import requests
import logging

class SparkHealthCheckOperator(BaseOperator):
    """
    Custom Operator to check Spark cluster health via YARN ResourceManager API.
    """
    def __init__(self, rm_host, rm_port=8088, **kwargs):
        super().__init__(**kwargs)
        self.rm_host = rm_host
        self.rm_port = rm_port

    def execute(self, context):
        base_url = f"http://{self.rm_host}:{self.rm_port}/ws/v1/cluster"
        
        try:
            # 1. Check Cluster Metrics
            metrics_url = f"{base_url}/metrics"
            response = requests.get(metrics_url, timeout=10)
            response.raise_for_status()
            metrics = response.json().get('clusterMetrics', {})
            
            active_nodes = metrics.get('activeNodes', 0)
            total_nodes = metrics.get('totalNodes', 0)
            
            logging.info(f"YARN Cluster Metrics: Active Nodes: {active_nodes}/{total_nodes}")
            
            if active_nodes == 0:
                raise Exception("No active nodes in YARN cluster!")

            # 2. Check Node Details (Resources)
            nodes_url = f"{base_url}/nodes"
            nodes_response = requests.get(nodes_url, timeout=10)
            nodes_response.raise_for_status()
            nodes = nodes_response.json().get('nodes', {}).get('node', [])
            
            for node in nodes:
                node_id = node.get('id')
                state = node.get('state')
                vcores = node.get('vCores', 0)
                memory = node.get('memory', 0)
                logging.info(f"Node {node_id} ({state}): vCores: {vcores}, Mem: {memory}MB")

            return metrics
        except Exception as e:
            logging.error(f"Failed to connect to YARN ResourceManager at {self.rm_host}:{self.rm_port}: {e}")
            raise e
