from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from ducktape.cluster.cluster_spec import ClusterSpec
from waltz_ducktape.tests.produce_consume_validate import ProduceConsumeValidateTest
from ducktape.utils.util import wait_until
from random import randrange
from time import sleep
import re


class ClientValidationTest(ProduceConsumeValidateTest):
    """
    A class of waltz tests where each client (producer/consumer) in waltz cluster is run as a single process.
    """
    MIN_CLUSTER_SPEC = ClusterSpec.from_list([
        {'cpu':1, 'mem':'1GB', 'disk':'25GB', 'additional_disks':{'/dev/sdb':'100GB'}, 'num_nodes':3},
        {'cpu':1, 'mem':'3GB', 'disk':'15GB', 'num_nodes':2},
        {'cpu':1, 'mem':'1GB', 'disk':'25GB', 'num_nodes':1}])

    def __init__(self, test_context):
        super(ClientValidationTest, self).__init__(test_context=test_context)

    @cluster(cluster_spec=MIN_CLUSTER_SPEC)
    @parametrize(num_active_partitions=1, txn_per_client=75, num_producers=3, num_consumers=2, interval=500, timeout=360)
    @parametrize(num_active_partitions=4, txn_per_client=100, num_producers=2, num_consumers=2, interval=250, timeout=360)
    def test_produce_consume_no_torture(self, num_active_partitions, txn_per_client, num_producers, num_consumers, interval, timeout):
        validation_cmd = self.get_produce_consume_parallel(num_active_partitions, txn_per_client, num_producers,
                                                           num_consumers, interval)
        self.run_produce_consume_validate(lambda: self.produce_consume_no_torture(validation_cmd, timeout))

    @cluster(cluster_spec=MIN_CLUSTER_SPEC)
    @parametrize(num_active_partitions=1, txn_per_client=200, num_producers=3, num_consumers=2, interval=500, num_consumers_to_stop=1, delay_before_torture=40, timeout=360)
    @parametrize(num_active_partitions=4, txn_per_client=100, num_producers=2, num_consumers=4, interval=500, num_consumers_to_stop=3, delay_before_torture=35,timeout=360)
    def test_produce_consume_abrupt_stop_of_consumers(self, num_active_partitions, txn_per_client, num_producers, num_consumers,
                                                      interval, num_consumers_to_stop, delay_before_torture, timeout):
        validation_cmd = self.get_produce_consume_parallel(num_active_partitions, txn_per_client, num_producers,
                                                           num_consumers, interval)
        self.run_produce_consume_validate(lambda: self.produce_consume_consumer_torture(validation_cmd, timeout, txn_per_client * num_producers,
                                                    num_active_partitions, num_consumers_to_stop, delay_before_torture, num_consumers))

    def get_num_failed_processes_cmd(self):
        return "fail=0; for job in `jobs -p`; do wait $job || let \"fail+=1\"; done ; echo \"number of failed processes: $fail\""

    def get_produce_consume_parallel(self, num_active_partitions, txn_per_client, num_producers, num_consumers, interval):
        cmd_parallel = ""

        for i in range(num_producers):
            cmd_parallel += self.client_cli.create_producer_cmd(self.log_file_path, txn_per_client, interval, num_active_partitions)
            cmd_parallel += " & "
        for i in range(num_consumers):
            cmd_parallel += self.client_cli.create_consumer_cmd(self.log_file_path, txn_per_client * num_producers, num_active_partitions)
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

    def produce_consume_consumer_torture(self, validation_cmd, timeout, expected_number_of_transactions,
                                         num_active_partitions, num_consumers_to_stop, delay_before_torture, num_consumers):
        """
        A validate function to test producers and consumers running in parallel with a torture test that kills consumer client nodes.

        :param validation_cmd: Command that is passed to client node
        :param timeout: Test timeout
        :param expected_number_of_transactions: The expected number of created transaction during this test
        :param num_active_partitions: Number of active partitions
        :param num_consumers_to_stop: Number of consumers to stop with kill SIGTERM command
        :param delay_before_torture: The delay in seconds between first transaction being processed and beginning of the torture test
        :param num_consumers: Total number of consumer clients
        """

        admin_port = self.waltz_storage.admin_port
        port = self.waltz_storage.port
        node = self.waltz_storage.nodes[randrange(len(self.waltz_storage.nodes))]
        random_active_partition = randrange(num_active_partitions)

        # Step 1: Start waltz cluster and execute validation command
        self.verifiable_client.start(validation_cmd)

        # Step 2: Wait till transactions get processed
        wait_until(lambda: self.is_max_transaction_id_updated(self.get_host(node.account.ssh_hostname, admin_port),
                                                             port, random_active_partition, -1), timeout_sec=timeout)

        # Step 3: Wait before staring torture test
        sleep(delay_before_torture)

        # Step 4: Start torture
        for i in range(num_consumers_to_stop):
            self.abrupt_stop_of_consumer_node(num_consumers - i)

        # Step 5: Wait until verifiable client ends its task
        wait_until(lambda: self.verifiable_client.task_complete() == True, timeout_sec=timeout,
                   err_msg="verifiable_client failed to complete task in {} seconds.".format(timeout))

        # Step 6: Verify child processes of a main process finished as expected (num_consumers_to_stop exited with other code than 0)
        num_failed_processes = int(re.search('failed processes: (\d+)', self.verifiable_client.get_validation_result()).group(1))
        assert num_failed_processes == num_consumers_to_stop, "number of failed processes: {}, expected: {}".format(num_failed_processes, num_consumers_to_stop)

        # Step 7: Assert all transactions are persistently stored.
        assert expected_number_of_transactions == self.num_of_transactions_on_storage_node(node, admin_port, port,
                                                                                           num_active_partitions), \
            "number of transactions stored in storage partition does not match with all the transactions sent by producers. " \
            "Client {}, Storage = {}" \
                .format(expected_number_of_transactions,
                        self.num_of_transactions_on_storage_node(node, admin_port, port, num_active_partitions))

    def abrupt_stop_of_consumer_node(self, number_of_consumers):
        # parent process is on the first line and indexing of rows starts with 1, thus number_of_consumers + 2
        get_pid = "ps -ef | grep \"create-consumer\" | awk \'{{print $2}}\' | head -n -1 | sort -n | sed -n {0}p".format(randrange(number_of_consumers) + 2)
        cmd = "kill -SIGTERM `{}`".format(get_pid)
        self.verifiable_client.nodes[0].account.ssh_capture(cmd)

    def num_of_transactions_on_storage_node(self, node, admin_port, port, num_active_partitions):
        """
        :returns Total number of all transactions stored under active partitions in a storage node
        (i.e. (partition1 high watermark + 1) + (partition2 high watermark + 1) ...), where transactions starts at index -1
        """

        total_num_of_transaction = 0
        for partition in range(num_active_partitions):
            total_num_of_transaction += self.get_storage_max_transaction_id(self.get_host(node.account.ssh_hostname, admin_port),
                                                                            port, partition) + 1
        return total_num_of_transaction
