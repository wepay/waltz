from threading import Thread
from random import choice
from time import sleep


class NodeBounceScheduler(Thread):
    """
    A scheduler to bounce service node periodically.
    """
    START_A_NODE = 0
    RESUME_PROCESS = 1
    STOP_A_NODE = 2
    KILL_PROCESS = 3
    PAUSE_PROCESS = 4
    IDLE = 5

    STARTED = "started"
    STOPPED = "stopped"
    PAUSED = "paused"

    def __init__(self, service, interval, stop_condition, iterable_cmd_list=None):
        """
        Construct a new 'NodeBounceScheduler' object.

        :param service: The service to be bounced
        :param interval: The interval(seconds) between two bounce
        :param stop_condition: The condition to stop scheduler
        :param iterable_cmd_list: An iterable list of commands in format of
               [{"action": START_A_NODE, "node": 0}, {"action": IDLE}].
               If missing, scheduler will use _decide_action() to decide
               next action, and take the action on a node based on
               _random_node_idx()
        """
        Thread.__init__(self)
        self.service = service
        self.interval = interval
        self.stop_condition = stop_condition
        self.iterable_cmd_list = iterable_cmd_list
        self.state_nodes_dict = {self.STARTED: set(), self.STOPPED: set(), self.PAUSED: set()}
        self.daemon = True

    def run(self):
        for idx in range(len(self.service.nodes)):
            self.state_nodes_dict[self.STARTED].add(idx)

        self.service.logger.info("Bouncing {} service ...".format(self.service.service_name()))
        while not self.stop_condition():
            self.service.logger.debug("{} nodes state: {}".format(self.service.service_name(), self.state_nodes_dict))
            if self.iterable_cmd_list:
                # take action on specific node according to cmd_list
                # keep IDLE after cmd_list been iterated through
                cmd = next(self.iterable_cmd_list, {})
                action = cmd.get("action", self.IDLE)
                node_idx = cmd.get("node", None)
                self.process(action, node_idx)
            else:
                self.process(self.decide_action())

            sleep(self.interval)

    def process(self, action, node_idx=None):
        """
        Scheduler will take an action on given service node. If node_idx
        is not specified, it will select a random node based on action.
        :param action: operation on specific node
        :param node_idx: operating node index
        """
        if action == self.START_A_NODE:
            node_idx = node_idx if node_idx is not None else self.random_node_idx(self.STOPPED)
            node = self.service.nodes[node_idx]
            self.service.logger.debug("Starting {} on node {} ...".format(self.service.service_name(), node_idx))
            self.take_action(from_state=self.STOPPED, to_state=self.STARTED, node_idx=node_idx,
                             action_func=lambda: self.service.start_node(node))
        elif action == self.RESUME_PROCESS:
            node_idx = node_idx if node_idx is not None else self.random_node_idx(self.PAUSED)
            node = self.service.nodes[node_idx]
            self.service.logger.debug("Resuming {} process on node {} ...".format(self.service.service_name(), node_idx))
            self.take_action(from_state=self.PAUSED, to_state=self.STARTED, node_idx=node_idx,
                             action_func=lambda: node.account.ssh_capture("pgrep -f waltz | xargs sudo kill -CONT"))
        elif action == self.STOP_A_NODE:
            node_idx = node_idx if node_idx is not None else self.random_node_idx(self.STARTED)
            node = self.service.nodes[node_idx]
            self.service.logger.debug("Stopping {} on node {} ...".format(self.service.service_name(), node_idx))
            self.take_action(from_state=self.STARTED, to_state=self.STOPPED, node_idx=node_idx,
                             action_func=lambda: self.service.stop_node(node))
        elif action == self.KILL_PROCESS:
            node_idx = node_idx if node_idx is not None else self.random_node_idx(self.STARTED)
            node = self.service.nodes[node_idx]
            self.service.logger.debug("Killing {} process on node {} ...".format(self.service.service_name(), node_idx))
            self.take_action(from_state=self.STARTED, to_state=self.STARTED, node_idx=node_idx,
                             action_func=lambda: node.account.ssh_capture("pgrep -f waltz | xargs sudo kill -9"))
        elif action == self.PAUSE_PROCESS:
            node_idx = node_idx if node_idx is not None else self.random_node_idx(self.STARTED)
            node = self.service.nodes[node_idx]
            self.service.logger.debug("Pausing {} process on node {} ...".format(self.service.service_name(), node_idx))
            self.take_action(from_state=self.STARTED, to_state=self.PAUSED, node_idx=node_idx,
                             action_func=lambda: node.account.ssh_capture("pgrep -f waltz | xargs sudo kill -STOP"))
        elif action == self.IDLE:
            sleep(self.interval)

    def random_node_idx(self, from_state):
        nodes = self.state_nodes_dict[from_state]
        if not nodes:
            raise ValueError("nodes should not be None or empty")
        node_index = choice(tuple(nodes))
        return node_index

    def decide_action(self):
        """
        Decide scheduler action with dynamic probability based on service nodes state.
        The probability of each actions will be based on a lottery system.

        The idea behind this probability model is to make the scheduler:
          1) tends to bring up a node(START_A_NODE/RESUME_PROCESS) when more nodes are down
          2) tends to bring down a node(STOP_A_NODE/KILL_PROCESS/PAUSE_PROCESS) when more nodes are up
          3) never stop a node or kill/pause a process when over half of the nodes are down
          4) never start a node when no node is stopped
          5) never resume a process when no process is paused

        The draw has two rounds. First round winners will make it to second round. And second round
        winner will be the final decision maker.

        Round one: Decide whether to bring up or bring down a service node:
          1) lottery(bring up) = num_down_nodes
          2) lottery(bring down) = num_up_nodes - num_all_nodes / 2
          Winner will be drawn from two types of lotteries.

        Round two: Decide exactly action to take based on decision made in round one:
          1) If decide to bring up a node:
            (1) lottery(START_A_NODE) = num_stopped_nodes
            (2) lottery(RESUME_PROCESS) = num_paused_nodes
            Winner will be drawn from two types of lotteries.

          2) If decide to bring down a node:
            (1) lottery(STOP_A_NODE) = 1
            (2) lottery(KILL_PROCESS) = 1
            (3) lottery(PAUSE_PROCESS) = 1
            Winner will be drawn from three types of lotteries.

        Probability functions:
          1) Round one:
            (1) lottery(total) = lottery(bring up) + lottery(bring down)
            (1) P(bring up) = lottery(bring up) / lottery(total)
            (2) P(bring down) = lottery(bring down) / lottery(total)

          2) Round two:
            (1) P(START_A_NODE) = (num_stopped_nodes / num_down_nodes) * P(bring up)
            (2) P(RESUME_PROCESS) = (num_paused_nodes / num_down_nodes) * P(bring up)
            (3) P(STOP_A_NODE) = (1/3) * P(bring down)
            (4) P(KILL_PROCESS) = (1/3) * P(bring down)
            (5) P(PAUSE_PROCESS) = (1/3) * P(bring down)

        Decision probability example (for a cluster with 5 nodes):
          1) cluster state: {started:2 stopped:2 paused:1}
             P(START_A_NODE) = 2/3
             P(RESUME_PROCESS) = 1/3
             P(STOP_A_NODE) = 0
             P(KILL_PROCESS) = 0
             P(PAUSE_PROCESS) = 0

          2) cluster state: {started:3 stopped:1 paused:1}
             P(START_A_NODE) = 1/3
             P(RESUME_PROCESS) = 1/3
             P(STOP_A_NODE) = 1/9
             P(KILL_PROCESS) = 1/9
             P(PAUSE_PROCESS) = 1/9

          3) cluster state: {started:4 stopped:1}
             P(START_A_NODE) = 1/3
             P(RESUME_PROCESS) = 0
             P(STOP_A_NODE) = 2/9
             P(KILL_PROCESS) = 2/9
             P(PAUSE_PROCESS) = 2/9

          4) cluster state: {started:5 down:0}
             P(START_A_NODE) = 0
             P(RESUME_PROCESS) = 0
             P(STOP_A_NODE) = 1/3
             P(KILL_PROCESS) = 1/3
             P(PAUSE_PROCESS) = 1/3

        :return: action to take
        """
        num_all_nodes = len(self.service.nodes)
        num_up_nodes = len(self.state_nodes_dict["started"])
        num_stopped_nodes = len(self.state_nodes_dict["stopped"])
        num_paused_nodes = len(self.state_nodes_dict["paused"])
        num_down_nodes = num_stopped_nodes + num_paused_nodes

        if num_up_nodes + num_down_nodes != num_all_nodes:
            raise ValueError("num_up_nodes + num_down_nodes should equal to num_all_nodes")

        # Round one
        num_lotteries_bring_up = num_down_nodes
        num_lotteries_bring_down = num_up_nodes - num_all_nodes / 2

        lottery_box = ['up'] * num_lotteries_bring_up + ['down'] * num_lotteries_bring_down
        lottery_winner = choice(lottery_box)

        # Round two
        if lottery_winner == "up":
            num_lotteries_start_a_node = num_stopped_nodes
            num_lotteries_resume_process = num_paused_nodes
            lottery_box = [self.START_A_NODE] * num_lotteries_start_a_node + [self.RESUME_PROCESS] * num_lotteries_resume_process
            return choice(lottery_box)
        else:
            lottery_box = [self.STOP_A_NODE] * 1 + [self.KILL_PROCESS] * 1 + [self.PAUSE_PROCESS] * 1
            return choice(lottery_box)

    def take_action(self, from_state, to_state, node_idx, action_func):
        action_func()
        self.state_nodes_dict[from_state].remove(node_idx)
        self.state_nodes_dict[to_state].add(node_idx)
