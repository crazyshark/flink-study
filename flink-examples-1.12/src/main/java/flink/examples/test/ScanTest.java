package flink.examples.test;


import com.jd.jim.cli.ScanCallback;
import com.jd.jim.cli.ScanFailurePolicy;
import com.jd.jim.cli.protocol.KeyScanResult;
import com.jd.jim.cli.protocol.ScanOptions;
import com.jd.jim.cli.protocol.ScanOptions.*;
import com.jd.jim.cli.protocol.ScanResult;
import com.jd.jim.cli.protocol.ZSetTuple;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class ScanTest extends BaseCommandTest {
    private static final Logger logger = LoggerFactory.getLogger(ScanTest.class);

    @Test
    public void testScan() {
        ScanOptionsBuilder scanOptions = ScanOptions.scanOptions();
        scanOptions.count(100);
//        scanOptions.match("");
        KeyScanResult<String> scan = client.scan("", scanOptions.build());

        while (!scan.isFinished()) {
            try {
                scan = client.scan(scan.getCursor(), scanOptions.build());

                String cursor = scan.getCursor();
                List<String> result = scan.getResult();

                System.out.println(cursor);
                System.out.println(result);
            } catch (Exception e) {
                logger.error("{}", scan.getCursor(), e);
            }
        }
    }

    @Test
    public void testByteScan() {
        ScanOptionsBuilder scanOptions = ScanOptions.scanOptions();
        scanOptions.count(100);
        KeyScanResult<byte[]> scan = client.scan("16383-0".getBytes(), scanOptions.build());

        while (!scan.isFinished()) {
            try {
                scan = client.scan(scan.getCursor().getBytes(), scanOptions.build());

                String cursor = scan.getCursor();
                List<byte[]> result = scan.getResult();

                System.out.println(cursor);
                System.out.println(result);
            } catch (Exception e) {
                logger.error("{}", scan.getCursor(), e);
            }
        }

    }

    @Test
    public void testhScan() {
        client.del(TestConstanst.STRING_KEY_1);

        for (int i = 1; i <= 1000; i++) {
            client.hSet(TestConstanst.STRING_KEY_1, "zset_test_value_" + i, "zset_test_value_" + i);
        }

        Assert.assertTrue(1000 == client.hLen(TestConstanst.STRING_KEY_1));

        int totalCount = 0;
        long cursor = 0;
        while (true) {
            ScanOptions.ScanOptionsBuilder scanOptions = ScanOptions.scanOptions();
            scanOptions.count(100);
            ScanResult<Map.Entry<String, String>> hScan = client.hScan(TestConstanst.STRING_KEY_1, cursor,
                    scanOptions.build());

            cursor = hScan.getCursor();
            List<Map.Entry<String, String>> result = hScan.getResult();
            totalCount += result.size();
            if (cursor == 0) {
                break;
            }
        }

        Assert.assertEquals(1000, totalCount);
    }

    @Test
    public void testsScan() {
        client.del(TestConstanst.STRING_KEY_1);

        for (int i = 1; i <= 1000; i++) {
            client.sAdd(TestConstanst.STRING_KEY_1, "zset_test_value_" + i);
        }

        Assert.assertTrue(1000 == client.sCard(TestConstanst.STRING_KEY_1));

        int totalCount = 0;
        long cursor = 0;
        while (true) {
            ScanOptionsBuilder scanOptions = ScanOptions.scanOptions();
            scanOptions.count(100);
            ScanResult<String> sScan = client.sScan(TestConstanst.STRING_KEY_1, cursor, scanOptions.build());

            cursor = sScan.getCursor();
            List<String> result = sScan.getResult();
            totalCount += result.size();
            if (cursor == 0) {
                break;
            }
        }

        Assert.assertEquals(1000, totalCount);
    }

    @Test
    public void testzScan() {
        client.del(TestConstanst.STRING_KEY_1);

        for (int i = 1; i <= 1000; i++) {
            client.zAdd(TestConstanst.STRING_KEY_1, i, "zset_test_value_" + i);
        }

        Assert.assertTrue(1000 == client.zCard(TestConstanst.STRING_KEY_1));

        int totalCount = 0;
        long cursor = 0;
        while (true) {
            ScanOptionsBuilder scanOptions = ScanOptions.scanOptions();
            scanOptions.count(100);
            ScanResult<ZSetTuple<String>> zScan = client.zScan(TestConstanst.STRING_KEY_1, cursor, scanOptions.build());

            cursor = zScan.getCursor();
            List<ZSetTuple<String>> result = zScan.getResult();
            totalCount += result.size();
            if (cursor == 0) {
                break;
            }
        }

        Assert.assertEquals(1000, totalCount);
    }

    // concurrent scan for every shard
    @Test
    public void testMultiThread() {

        final AtomicInteger ai = new AtomicInteger(0);

        ScanCallback<byte[]> callback = new ScanCallback<byte[]>() {
            @Override
            public void process(List<byte[]> keys) {
                if (keys != null && !keys.isEmpty()) {
                    ai.addAndGet(keys.size());
                    logger.info("====>{}", keys.size());
                }
            }
        };

        ScanOptions scanOptions = ScanOptions.scanOptions().count(1000).concurrent(10).failurePolicy(ScanFailurePolicy.over).retry(0).build();
        Future<Boolean> future = client.scan(scanOptions, callback);


        try {
            future.get();
            logger.info("total count {}", ai.get());
        } catch (Exception e) {
            logger.error("====>,", e);
        }
    }
}
