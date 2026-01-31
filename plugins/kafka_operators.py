from airflow.models.baseoperator import BaseOperator
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, TopicPartition, KafkaException, ConsumerGroupTopicPartitions
import logging

class KafkaHealthCheckOperator(BaseOperator):
    """
    Custom Operator to check Kafka cluster health.
    """
    def __init__(self, bootstrap_servers, **kwargs):
        super().__init__(**kwargs)
        self.bootstrap_servers = bootstrap_servers

    def execute(self, context):
        conf = {'bootstrap.servers': self.bootstrap_servers, 'socket.timeout.ms': 5000}
        admin_client = AdminClient(conf)
        
        try:
            # metadata() will raise an exception if it can't connect
            metadata = admin_client.list_topics(timeout=10)
            brokers = metadata.brokers
            logging.info(f"Connected to Kafka. Found {len(brokers)} brokers.")
            for b_id, b in brokers.items():
                logging.info(f"Broker {b_id}: {b.host}:{b.port}")
            return True
        except Exception as e:
            logging.error(f"Failed to connect to Kafka brokers at {self.bootstrap_servers}: {e}")
            raise e


class KafkaTopicCheckOperator(BaseOperator):
    """
    Custom Operator to verify topic availability and partition count.
    """
    def __init__(self, bootstrap_servers, topic_name, expected_partitions=None, **kwargs):
        super().__init__(**kwargs)
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.expected_partitions = expected_partitions

    def execute(self, context):
        conf = {'bootstrap.servers': self.bootstrap_servers, 'socket.timeout.ms': 5000}
        admin_client = AdminClient(conf)
        
        try:
            metadata = admin_client.list_topics(timeout=10)
            topics = metadata.topics
            
            if self.topic_name not in topics:
                raise Exception(f"Topic '{self.topic_name}' does not exist!")
            
            topic_metadata = topics[self.topic_name]
            partition_count = len(topic_metadata.partitions)
            
            logging.info(f"Topic '{self.topic_name}' found with {partition_count} partitions")
            
            # Log partition details
            for partition_id, partition in topic_metadata.partitions.items():
                logging.info(f"  Partition {partition_id}: Leader={partition.leader}, "
                           f"Replicas={partition.replicas}, ISR={partition.isrs}")
            
            # Verify expected partition count if specified
            if self.expected_partitions and partition_count != self.expected_partitions:
                logging.warning(f"Expected {self.expected_partitions} partitions, "
                              f"but found {partition_count}")
            
            return {
                'topic': self.topic_name,
                'partition_count': partition_count,
                'partitions': list(topic_metadata.partitions.keys())
            }
            
        except Exception as e:
            logging.error(f"Failed to verify topic '{self.topic_name}': {e}")
            raise e


class KafkaConsumerGroupOperator(BaseOperator):
    """
    Custom Operator to monitor consumer group status and lag.
    """
    def __init__(self, bootstrap_servers, group_id, **kwargs):
        super().__init__(**kwargs)
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id

    def execute(self, context):
        
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': f"{self.group_id}_monitor",  # Use different group for monitoring
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        
        consumer = Consumer(conf)
        
        try:
            # Get cluster metadata
            metadata = consumer.list_topics(timeout=10)
            
            # Get consumer group information
            # Note: This requires librdkafka with admin API support
            admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
            
            # List all consumer groups
            groups = admin_client.list_groups(timeout=10)
            
            group_found = False
            for group in groups:
                if group.id == self.group_id:
                    group_found = True
                    logging.info(f"Consumer Group '{self.group_id}' found")
                    logging.info(f"  Protocol: {group.protocol}")
                    logging.info(f"  Protocol Type: {group.protocol_type}")
                    logging.info(f"  State: {group.state}")
                    logging.info(f"  Members: {len(group.members)}")
                    
                    for member in group.members:
                        logging.info(f"    Member ID: {member.id}")
                        logging.info(f"    Client ID: {member.client_id}")
                        logging.info(f"    Host: {member.host}")
                    
                    break
            
            if not group_found:
                logging.warning(f"Consumer Group '{self.group_id}' not found. "
                              f"This may be normal if no consumers are currently active.")
            
            consumer.close()
            
            return {
                'group_id': self.group_id,
                'found': group_found
            }
            
        except KafkaException as e:
            logging.error(f"Kafka error while checking consumer group '{self.group_id}': {e}")
            consumer.close()
            raise e
        except Exception as e:
            logging.error(f"Failed to check consumer group '{self.group_id}': {e}")
            consumer.close()
            raise e

class KafkaDataFlowOperator(BaseOperator):
    """
    Operator to monitor data flow metrics: Consumer Lag and Throughput.
    
    Checks:
    1. Consumer Lag: Difference between High Watermark and Committed Offset.
        - Requires 'list_consumer_group_offsets' support.
    2. Throughput: Change in High Watermark since last run (calculated via XCom).
    """
    ui_color = '#e4f0e8'

    def __init__(self, bootstrap_servers, topic_name, group_id, max_lag_threshold=10000, min_throughput_threshold=1, **kwargs):
        super().__init__(**kwargs)
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.group_id = group_id
        self.max_lag_threshold = max_lag_threshold
        self.min_throughput_threshold = min_throughput_threshold

    def execute(self, context):
        # 1. Setup Consumer to get High Watermarks (End Offsets)
        # We use a monitor group id to avoid interfering with production groups
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': f"{self.group_id}_monitor_flow_check",
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False
        }
        
        consumer = Consumer(conf)
        admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})

        try:
            # Get metadata to find partitions
            metadata = consumer.list_topics(topic=self.topic_name, timeout=10)
            if self.topic_name not in metadata.topics:
                raise Exception(f"Topic '{self.topic_name}' not found")
                
            topic_metadata = metadata.topics[self.topic_name]
            # Use int for partition id
            partitions = [TopicPartition(self.topic_name, p) for p in topic_metadata.partitions.keys()]
            
            # --- 2. Calculate End Offsets & Throughput ---
            total_end_offset = 0
            end_offsets = {}
            for tp in partitions:
                # get_watermark_offsets returns (low, high)
                low, high = consumer.get_watermark_offsets(tp, timeout=10)
                end_offsets[tp.partition] = high
                total_end_offset += high
            
            logging.info(f"Topic '{self.topic_name}' Total End Offset: {total_end_offset}")
            
            # Check Throughput using XCom
            ti = context['task_instance']
            previous_end_offset = ti.xcom_pull(task_ids=self.task_id, key='total_end_offset')
            
            throughput = 0
            throughput_check_passed = True
            
            if previous_end_offset is not None:
                # Handle potential type mismatch if xcom returns string
                throughput = total_end_offset - int(previous_end_offset)
                logging.info(f"Throughput since last run: {throughput} messages")
                
                # Check for Stalled Processing
                if throughput < self.min_throughput_threshold:
                    logging.error(f"Throughput ({throughput}) is below threshold ({self.min_throughput_threshold}). Potential stall detected!")
                    throughput_check_passed = False
            else:
                logging.info("No previous offset found. Cannot calculate throughput for this run.")

            # --- 3. Calculate Consumer Lag ---
            total_lag = 0
            lag_check_passed = True
            try:
                # Construct list of partitions to query
                # Note: list_consumer_group_offsets requires specific library version.
                # If this fails, we catch it and skip lag check but proceed with throughput
                
                # Check for list_consumer_group_offsets availability (basic check)
                if hasattr(admin_client, 'list_consumer_group_offsets'):
                     # We need ConsumerGroupTopicPartitions, but let's try to construct it carefully
                     # or use the dict format if supported.
                     # Actually, standard confluent_kafka uses list_consumer_group_offsets([ConsumerGroupTopicPartitions...])
                     # We imported ConsumerGroupTopicPartitions above.
                     
                     cgtps = [ConsumerGroupTopicPartitions(self.group_id, partitions)]
                     future = admin_client.list_consumer_group_offsets(cgtps)
                     
                     # Result is a dict of {GroupPartitions: Future} or similar? 
                     # Docs: returns dict of {ConsumerGroupTopicPartitions: Future}
                     
                     # Result is a dict of {group_id: Future}
                     result_cgtp = admin_client.list_consumer_group_offsets(cgtps)
                     
                     # Wait for result
                     for group_str, f in result_cgtp.items():
                         committed_parts = f.result(timeout=10) 
                         
                         for cp in committed_parts.topic_partitions:
                             if cp.offset >= 0:
                                 # Calculate lag
                                 current_high = end_offsets.get(cp.partition, 0)
                                 lag = current_high - cp.offset
                                 total_lag += lag
                                 logging.info(f"Partition {cp.partition}: End={current_high}, Committed={cp.offset}, Lag={lag}")
                             else:
                                 logging.warning(f"Partition {cp.partition}: No committed offset (Offset={cp.offset})")
                else:
                    logging.warning("AdminClient.list_consumer_group_offsets not available. Skipping Lag check.")
                    total_lag = -1 # Indicate unknown

            except Exception as e:
                logging.warning(f"Failed to calculate lag: {e}")
                total_lag = -1

            if total_lag >= 0:
                logging.info(f"Total Consumer Lag: {total_lag}")
                if total_lag > self.max_lag_threshold:
                    logging.error(f"Consumer Lag ({total_lag}) exceeds threshold ({self.max_lag_threshold})!")
                    lag_check_passed = False
            
            # 4. Push new state to XCom
            ti.xcom_push(key='total_end_offset', value=total_end_offset)
            
            # 5. Final Alert Decision
            errors = []
            if not throughput_check_passed:
                errors.append(f"Stalled Processing: Throughput {throughput} < {self.min_throughput_threshold}")
            if not lag_check_passed:
                errors.append(f"High Lag: {total_lag} > {self.max_lag_threshold}")
                
            if errors:
                raise Exception("Data Flow Thresholds Violated: " + "; ".join(errors))
            
            return {
                'total_end_offset': total_end_offset,
                'throughput': throughput,
                'total_lag': total_lag
            }
            
        except Exception as e:
            logging.error(f"Data Flow check failed: {e}")
            raise e
        finally:
            consumer.close()
