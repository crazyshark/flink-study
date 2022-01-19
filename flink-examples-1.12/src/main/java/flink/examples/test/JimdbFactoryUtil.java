package flink.examples.test;

import com.jd.jim.cli.ReloadableJimClientFactory;
import com.jd.jim.cli.serializer.DefaultObjectSerializer;

import java.util.HashMap;
import java.util.Map;

public class JimdbFactoryUtil {

    static Map<String, ReloadableJimClientFactory> containers = new HashMap<>();

    public static ReloadableJimClientFactory  getFactory(String jimUrl,String configId){

        String uniqId = jimUrl + ":" + configId;

        ReloadableJimClientFactory  factory;

        if ((factory = containers.get(uniqId ))==null){

            synchronized(JimdbFactoryUtil.class){

                if ((factory=containers.get(uniqId ))==null){

                    //factory = ......; //生成相应的工厂对象
                    factory = new ReloadableJimClientFactory();
                    // default 0
                    factory.setConfigId(configId);
                    // netty IO线程池数量，一般情况下设置为2效果最佳，针对吞吐要求高的情况，可以根据不同的客户端CPU配置和集群规模建议测试后进行调整
                    factory.setIoThreadPoolSize(2);
                    //默认16k
                    factory.setObjectSerializer(new DefaultObjectSerializer(16*1024));
                    factory.setJimUrl(jimUrl);
                    containers.put(uniqId,factory);

                }

            }

        }

        return factory;

    }

    public synchronized static void destoryAllFactorys(){
        if (containers.size()==0){
            return;
        }
        for (ReloadableJimClientFactory factory : containers.values()) {

            try{

                factory.clear();

            } catch(Exception ex){

            }
        }
        containers.clear();
    }
}
