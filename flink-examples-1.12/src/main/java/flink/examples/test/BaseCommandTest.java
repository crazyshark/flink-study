package flink.examples.test;

import com.google.common.collect.Lists;
import com.jd.jim.cli.*;
import com.jd.jim.cli.serializer.DefaultObjectSerializer;
import com.jd.jim.cli.util.JimFutureUtils;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.jd.jim.cli.config.ConfigLongPollingClientFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BaseCommandTest {

    private static final String jimUrl = "jim://2914175049609821729/110000257";

    protected static Cluster client = null;
    protected static  ReloadableJimClientFactory factory = null;

    @BeforeClass
    public static void setUp() throws Exception {

        ConfigLongPollingClientFactory configClientFactory = new ConfigLongPollingClientFactory(
                "http://cfs.jim.jd.local");
        factory = new ReloadableJimClientFactory();
        // default 0
        factory.setConfigId("0");
        // netty IO线程池数量，一般情况下设置为2效果最佳，针对吞吐要求高的情况，可以根据不同的客户端CPU配置和集群规模建议测试后进行调整
        factory.setIoThreadPoolSize(2);
        //默认16k
        factory.setObjectSerializer(new DefaultObjectSerializer(16*1024));
        factory.setJimUrl(jimUrl);
        //流量控制，该队列由请求和响应两部组成，当队列瞬时达到50000，此时会提示超出队列长度，可以根据业务的流量进行调整，特别针对异步和pipeline调用
        factory.setRequestQueueSize(50000);

        factory.setConfigClient(configClientFactory.create());

        client = factory.getClient();
    }

    //销毁方式，不要用client.destroy();
    public void testDestroy(){
        if (factory!=null){
            factory.clear();
        }
    }

    @Test
    public void testSimple() throws InterruptedException, ExecutionException, TimeoutException {
        //String
        client.del(TestConstanst.STRING_KEY_1);
        client.set(TestConstanst.STRING_KEY_1, "testSetAndGetAndDelAndExists_String_value");
        Boolean rsBool = client.exists(TestConstanst.STRING_KEY_1);
        Assert.assertTrue(rsBool);

        //如果pipeline中积攒的命令数量超过100条(不可调整),会自动提交。
        PipelineClient pipelineClient = client.pipelineClient();
        JimFuture<String> set = pipelineClient.set(TestConstanst.STRING_KEY_1, "xx");
        JimFuture<String> get = pipelineClient.get(TestConstanst.STRING_KEY_1);
        //手动flush可以将pipeline中没有自动提交的命令提交掉
        pipelineClient.flush();
        List<JimFuture> futures = new ArrayList<JimFuture>();
        futures.add(set);
        futures.add(get);
        //可以等待future的返回结果，来判断命令是否成功。
        for (JimFuture future : futures) {
            //如果命令执行失败，此处会抛出异常，业务自行判断是否需要重试
            System.out.println(future.get());
        }

        if (JimFutureUtils.awaitAll(2, TimeUnit.SECONDS, set, get)) {
            System.out.println(String.format("key[%s] value is %s", TestConstanst.STRING_KEY_1, get.get()));
        }

        //async
        AsyncClient asyncClient = client.asyncClient();
        JimFuture<String> asyncSet = asyncClient.set(TestConstanst.STRING_KEY_1, "xx");
        //等待成功。
        asyncSet.get();
        //如果新建连接超时或失败，asyncClient.get(TestConstanst.STRING_KEY_1)会直接抛出异常，而不是封装在future里。
        JimFuture<String> asyncGet = asyncClient.get(TestConstanst.STRING_KEY_1);
        //如果命令处理超时或服务端响应服务会asyncGet.get会抛出异常。
        Assert.assertEquals("xx", asyncGet.get());

        asyncClient = client.asyncClient();
        JimFuture<Long> del = asyncClient.del(TestConstanst.STRING_KEY_1);
        System.out.println(del.get());
    }

    // 判断超时异常
    public static void testTimeOut(){
        try {
            client.del(TestConstanst.STRING_KEY_HASH_TAG_1);
        } catch (Exception ex){
            Throwable rootCause = getRootCause(ex);
            if (rootCause instanceof java.util.concurrent.TimeoutException){
                //超时异常处理逻辑
            }
        }


    }
    public static Throwable getRootCause(Throwable throwable) {
        Throwable cause;
        while ((cause = throwable.getCause()) != null) {
            throwable = cause;
        }
        return throwable;
    }

}
