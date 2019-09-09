package com.wepay.waltz.client.internal;

import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.waltz.test.mock.MockContext;
import com.wepay.zktools.util.Uninterruptibly;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InternalRpcClientTest extends InternalClientTestBase {

    @Test
    public void test() throws Exception {
        final int numTransactions = 10;
        List<String> expected = new ArrayList<>();

        InternalRpcClient internalRpcClient = getInternalRpcClient(WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS);
        InternalStreamClient internalStreamClient = getInternalStreamClient(true, WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS, internalRpcClient);

        assertEquals(allPartitions, internalRpcClient.getActivePartitions());

        for (int i = 0; i < numTransactions; i++) {
            String data = "transaction" + i;
            expected.add(data);

            MockContext context = MockContext.builder().header(0).data(data).build();
            TransactionBuilderImpl transactionBuilder = internalStreamClient.getTransactionBuilder(context);
            context.execute(transactionBuilder);

            TransactionFuture future = internalStreamClient.append(transactionBuilder.buildRequest());

            assertTrue(future.get());
        }

        ArrayList<Integer> ids = new ArrayList<>();
        for (int i = 0; i < numTransactions; i++) {
            ids.add(i);
        }

        for (Integer i : ids) {
            byte[] data = Uninterruptibly.call(() -> internalRpcClient.getTransactionData(0, i).get());
            assertEquals(expected.get(i), new String(data, UTF_8));
        }

        Collections.shuffle(ids);

        for (Integer i : ids) {
            byte[] data = Uninterruptibly.call(() -> internalRpcClient.getTransactionData(0, i).get());
            assertEquals(expected.get(i), new String(data, UTF_8));
        }
    }

}
