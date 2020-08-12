from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from ducktape.cluster.cluster_spec import ClusterSpec
from waltz_ducktape.tests.produce_consume_validate import ProduceConsumeValidateTest
from ducktape.utils.util import wait_until
import re


class ProducerConsumerClusterTest(ProduceConsumeValidateTest):
    """
    A class of waltz tests where each client (producer/consumer) in waltz cluster is run as a single process.
    """
    MIN_CLUSTER_SPEC = ClusterSpec.from_list([
        {'cpu':1, 'mem':'1GB', 'disk':'25GB', 'additional_disks':{'/dev/sdb':'100GB'}, 'num_nodes':3},
        {'cpu':1, 'mem':'3GB', 'disk':'15GB', 'num_nodes':2},
        {'cpu':1, 'mem':'1GB', 'disk':'25GB', 'num_nodes':1}])

    def __init__(self, test_context):
        super(ProducerConsumerClusterTest, self).__init__(test_context=test_context)

    @cluster(cluster_spec=MIN_CLUSTER_SPEC)
    @parametrize(num_active_partitions=1, txn_per_client=75, num_producers=3, num_consumers=2, interval=500, timeout=360)
    @parametrize(num_active_partitions=4, txn_per_client=100, num_producers=2, num_consumers=2, interval=250, timeout=360)
    def test_produce_consume_no_torture(self, num_active_partitions, txn_per_client, num_producers, num_consumers, interval, timeout):
        validation_cmd = self.get_produce_consume_parallel(num_active_partitions, txn_per_client, num_producers,
                                                           num_consumers, interval)
        self.run_produce_consume_validate(lambda: self.produce_consume_no_torture(validation_cmd, timeout))


    def get_num_failed_processes_cmd(self):
        return "fail=0; for job in `jobs -p`; do wait $job || let \"fail+=1\"; done ; echo \"number of failed processes: $fail\""

    def get_produce_consume_parallel(self, num_active_partitions, txn_per_client, num_producers, num_consumers, interval):
        cmd_parallel = ""

        for i in range(num_producers):
            cmd_parallel += self.client_cli.create_producer_cmd(txn_per_client, interval, num_active_partitions)
            cmd_parallel += " & "
        for i in range(num_consumers):
            cmd_parallel += self.client_cli.create_consumer_cmd(txn_per_client * num_producers, num_active_partitions)
            cmd_parallel += " & "

        cmd_parallel += self.get_num_failed_processes_cmd()
        return cmd_parallel

    def produce_consume_no_torture(self, validation_cmd, timeout):
        """
        A validate function to test producers and consumers running in parallel.

        :param validation_cmd: Command that is passed to client node
        :param timeout: Test timeout
        """

        # Step 1: Start waltz cluster and execute validation command
        self.verifiable_client.start(validation_cmd)

        # Step 2: Wait until verifiable client ends its task
        wait_until(lambda: self.verifiable_client.task_complete() == True, timeout_sec=timeout,
                   err_msg=lambda: "verifiable_client failed to complete task in {} seconds. Number of producers still running: {}, "
                           "number of consumers still running: {}".format(timeout, self.number_of_running_producers(), self.number_of_running_consumers()))

        # Step 3: Verify child processes of a main process finished successfully (exit code 0 received)
        num_failed_processes = int(re.search('failed processes: (\d+)', self.verifiable_client.get_validation_result()).group(1))
        assert num_failed_processes == 0, "number of failed processes: {}".format(num_failed_processes)

    def number_of_running_producers(self):
        return int(self.verifiable_client.nodes[0].account.ssh_output("ps -eo command | grep -c \"^java .* create-producer\" | cat").strip())


    def number_of_running_consumers(self):
        return int(self.verifiable_client.nodes[0].account.ssh_output("ps -eo command | grep -c \"^java .* create-consumer\" | cat").strip())
