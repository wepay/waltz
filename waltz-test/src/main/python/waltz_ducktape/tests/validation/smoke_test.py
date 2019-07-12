from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from waltz_ducktape.tests.produce_consume_validate import ProduceConsumeValidateTest
from waltz_ducktape.tests.validation.node_bounce_scheduler import NodeBounceScheduler


class SmokeTest(ProduceConsumeValidateTest):
    """
    A class of torture tests that turns on a bunch of ZK, storage, server,
    and client nodes. Fire transactions while turning things off and on,
    to ensure Waltz can recover from expected failure.
    """
    def __init__(self, test_context):
        super(SmokeTest, self).__init__(test_context=test_context)

    @cluster(nodes_spec={'storage':3, 'server':2, 'client':1})
    @parametrize(partition=1, txn_per_client=500, num_clients=10, interval=120, timeout=240)
    def test_produce_consume_no_torture(self, partition, txn_per_client, num_clients, interval, timeout):
        validation_cmd = self.client_cli.validate_txn_cmd(partition, txn_per_client, num_clients, interval)
        self.run_produce_consume_validate(lambda: self.simple_validation_func(validation_cmd, timeout))

    @cluster(nodes_spec={'storage':3, 'server':2, 'client':1})
    @parametrize(partition=0, txn_per_client=500, num_clients=10, interval=120, timeout=480)
    def test_produce_consume_while_bouncing_storage_nodes(self, partition, txn_per_client, num_clients, interval, timeout):
        validation_cmd = self.client_cli.validate_txn_cmd(partition, txn_per_client, num_clients, interval)
        validation_result = self.run_produce_consume_validate(lambda: self.simple_validation_func(validation_cmd, timeout),
                                                              lambda: self._bounce_storage_nodes(3))
        assert "exception" not in validation_result.lower(), "Test failed with exception:\n{}".format(validation_result)

    @cluster(nodes_spec={'storage':3, 'server':2, 'client':1})
    @parametrize(partition=0, txn_per_client=500, num_clients=2, interval=120, timeout=240)
    def test_produce_consume_while_killing_a_server_node(self, partition, txn_per_client, num_clients, interval, timeout):
        validation_cmd = self.client_cli.validate_txn_cmd(partition, txn_per_client, num_clients, interval)
        self.run_produce_consume_validate(lambda: self.simple_validation_func(validation_cmd, timeout),
                                          lambda: self._kill_a_server_node(partition))

    def _bounce_storage_nodes(self, interval):
        storage_node_bounce_scheduler = NodeBounceScheduler(service=self.waltz_storage, interval=interval,
                                                            stop_condition=lambda: self.verifiable_client.task_complete())
        storage_node_bounce_scheduler.start()

    def _kill_a_server_node(self, partition):
        node_idx = self.get_server_node_idx(partition)
        cmd_list = [{"action": NodeBounceScheduler.IDLE},
                              {"action": NodeBounceScheduler.STOP_A_NODE, "node": node_idx}]
        server_node_bounce_scheduler = NodeBounceScheduler(service=self.waltz_server, interval=3,
                                                           stop_condition=lambda: self.verifiable_client.task_complete(),
                                                           iterable_cmd_list=iter(cmd_list))
        server_node_bounce_scheduler.start()
