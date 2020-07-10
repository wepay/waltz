from threading import Thread
from time import sleep


class ConnectionInterruption(Thread):
    """
    A scheduler to drop incoming traffic to a port periodically.
    """

    def __init__(self, service, interruption_length, no_of_interruptions, service_node_index):
        """
            Construct a new 'ConnectionInterruption' object.

            :param service: The service having incoming traffic to a port blocked
            :param interruption_length: The interval(milliseconds) between two interruptions and the length of each interruption
            :param no_of_interruptions: Total number of network connection interruptions
            :param service_node_index: A service node used for this test
        """
        Thread.__init__(self)
        self.service = service
        self.interruption_length = interruption_length
        self.interruptions = no_of_interruptions
        self.service_node_index = service_node_index

    def run(self):
        node = self.service.nodes[self.service_node_index]
        for interruption in range(self.interruptions):
            # let waltz server receive transactions as usual
            sleep(self.interruption_length/1000)

            # disable connection on port
            self.service.logger.info("Closing a port")
            node.account.ssh_capture("sudo iptables -I INPUT -p tcp --destination-port {} -j DROP".format(self.service.port))
            sleep(self.interruption_length / 1000)

            # enable connection on port
            self.service.logger.info("Opening a port")
            node.account.ssh_capture("sudo iptables -D INPUT -p tcp --destination-port {} -j DROP".format(self.service.port))