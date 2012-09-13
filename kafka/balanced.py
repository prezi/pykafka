from random import SystemRandom
from kazoo.exceptions import NoNodeError
from kafka.base import ConnectionFailure
from kazoo.client import KazooClient
from kafka.blocking import Kafka

rand = SystemRandom()


class BalancedKafka(Kafka):
    """
    Zookeeper based load balanced kafka producer (for now)
    """

    def __init__(self, hostports, max_size=None, include_corrupt=False):
        self.zk_client = KazooClient(hosts=hostports, timeout=60)
        self.zk_client.start()
        self.brokers = self.zk_client.get_children('/brokers/ids')
        if not self.brokers:
            raise ConnectionFailure("No brokers found in zookeeper ensemble {0}".format(hostports))
        (host, port) = self._get_new_broker_host_port()
        super(BalancedKafka, self).__init__(host=host, port=port, max_size=max_size, include_corrupt=include_corrupt)

    def _get_new_broker_host_port(self):
        self.broker = rand.choice(self.brokers)
        (hostport, _) = self.zk_client.get('/brokers/ids/{0}'.format(self.broker))
        (_, host, port) = hostport.split(':')
        return host, int(port)

    def _default_partition_num(self, topic):
        try:
            (partitions, _) = self.zk_client.get('/brokers/topics/{0}/{1}'.format(topic, self.broker))
        except NoNodeError:
            return 0  # topic does not exist yet
        return rand.randrange(0, int(partitions))

    def _reconnect(self):
        old_broker = self.broker
        try:
            self.brokers.remove(old_broker)
            if self.brokers:
                (self.host, self.port) = self._get_new_broker_host_port()
        finally:
            # put back old broker to retry next time
            self.brokers.append(old_broker)
        super(BalancedKafka, self)._reconnect()
