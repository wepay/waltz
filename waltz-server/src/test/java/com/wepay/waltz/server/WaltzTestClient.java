package com.wepay.waltz.server;

import com.wepay.waltz.client.TransactionContext;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.common.message.AppendRequest;
import com.wepay.waltz.test.mock.MockClusterManager;
import com.wepay.waltz.test.util.WaltzTestClientDriver;
import io.netty.handler.ssl.SslContext;

import java.util.concurrent.Future;

class WaltzTestClient extends WaltzClient {

    WaltzTestClient(WaltzClientCallbacks callbacks, MockClusterManager clusterManager) throws Exception {
        this(null, callbacks, clusterManager);
    }

    WaltzTestClient(SslContext sslCtx, WaltzClientCallbacks callbacks, MockClusterManager clusterManager) throws Exception {
        super(new WaltzTestClientDriver(true, sslCtx, callbacks, clusterManager), 1, 5000);
    }

    // Test only public method
    public AppendRequest buildForTest(TransactionContext context) {
        return super.build(context);
    }

    // Test only public method
    public Future<Boolean> appendForTest(AppendRequest request) {
        return streamClient.append(request);
    }

}
