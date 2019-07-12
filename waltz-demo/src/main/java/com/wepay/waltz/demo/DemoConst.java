package com.wepay.waltz.demo;

final class DemoConst {

    private DemoConst() {
    }

    static final int NUM_PARTITIONS = 1;

    static final String BANK_DB = "jdbc:mariadb://localhost/DEMO_BANK";

    static final int WALTZ_REPLICA1_PORT = 6000;
    static final int WALTZ_REPLICA2_PORT = 6002;
    static final int WALTZ_REPLICA3_PORT = 6004;

    static final int WALTZ_REPLICA1_ADMIN_PORT = 6001;
    static final int WALTZ_REPLICA2_ADMIN_PORT = 6003;
    static final int WALTZ_REPLICA3_ADMIN_PORT = 6005;

}
