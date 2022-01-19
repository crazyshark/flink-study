package flink.examples.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 *
 *  优化策略 : 在table级别，对多于平局值部分的region，采用最少region节点分配策略，替代 admin.balancer();
 *  最终效果 : 在table级别，每个regionServer的region个数都在平均值上下
 *  balance region on table level
 */
public class HbaseBalancer {

    public static final String tableStr = "data1";
    public static final String ZK_QUORUM = "hadoop01:2181,hadoop02:2181,hadoop03:2181";

    public static final Integer BUCKETS_PER_NODE = 50;
    public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, ZK_QUORUM);
        Connection conn = ConnectionFactory.createConnection(config);

        Admin admin = conn.getAdmin();
        ClusterStatus clusterStatus = admin.getClusterStatus();
        Collection<ServerName> hServers = clusterStatus.getServers();

        System.out.println("region servers :");
        Map<String,RegionServer> allRegionServers = new HashMap<String,RegionServer>(15);
        // 根据region server创建 hostname 和regionServer的映射，对allRegionServers进行初始化
        for(ServerName server : hServers){

            RegionServer rs = new RegionServer();
            rs.setServerName(server);
            allRegionServers.put( server.getHostname(), rs ) ;

            String getHostAndPort = server.getHostAndPort();
            String getHostname = server.getHostname();

            Long startCode = server.getStartcode();
            System.out.println(startCode +" "+getHostname +" " +getHostAndPort);
            //List<HRegionInfo> regionInfos = admin.getOnlineRegions(server);
            allRegionServers .put(server.getHostname(), rs);
        }

        Table table = conn.getTable(TableName.valueOf(tableStr ));
        // 获取region的位置信息
        RegionLocator locator = conn.getRegionLocator(table.getName());
        List<HRegionLocation> hRegionLocations= locator.getAllRegionLocations();

        int avgCnt = (( int)hRegionLocations.size())/ hServers.size();
        System.out.println("avgCnt :" + avgCnt);
        System.out.println("hRegionLocations.size() :"+hRegionLocations.size());
        System.out.println("hServers.size() :" + hServers.size());
        List<HRegionLocation> toAssign = new ArrayList<HRegionLocation>(); // 当一个region server 的region的数量大于平均值的时候，保存需要进行重新分配的region

        System.out.println("=============== get Region Location end =============== ");
        // 根据已有的regionLocation信息进行最大程度的分配到各自节点上
        for (HRegionLocation hRegionLocation: hRegionLocations) {
            String hostname =hRegionLocation.getHostname();
            System.out.println("hostname :" + hostname);
//            RegionServer rs = allRegionServers.getOrDefault(hostname , new RegionServer() );
            // 上面预先创建的allRegionServers,已经进行初始化，保证这里不会取空值
            RegionServer rs = allRegionServers.get(hostname);
            System.out.println("rs.getRegions().size() :"+rs.getRegions().size());
            if (rs.getRegions().size() == 0) {
                rs.setServerName(hRegionLocation.getServerName());
                System.out.println("hRegionLocation.getServerName()  :"+hRegionLocation.getServerName());
            }
            if (rs.getRegions().size() < avgCnt) {
                rs.addRegion(hRegionLocation.getRegionInfo().getRegionNameAsString());
            } else {
                toAssign.add(hRegionLocation);
            }
            //noinspection Since15
            allRegionServers.putIfAbsent(hostname,rs); // move to rs.add
            System.out.println(" one of the" + hRegionLocation.toString());
        }
        System.out.println("=============== get Region Location end =============== ");

        // get all table regions which need to move
        // move to erery serve
        System.out.println(" region reassign");
        Iterator<HRegionLocation> assign = toAssign.iterator();
        for (HRegionLocation assignRegion: toAssign) {
            System.out.println("all need to reassign region " + assignRegion.toString());
        }
        System.out.println("=============== region reassign began ===============");

        while (assign.hasNext()){
            HRegionLocation region = assign.next();
            ServerName sn = region.getServerName();

            HRegionInfo regionInfo = region.getRegionInfo();
            String getEncodedName = regionInfo.getEncodedName();
            String sourceHostname = region.getHostname();
            String sourceName = sn.getServerName();

            Random rand = new Random();
            //String destServerKey = allRegionServers.keySet().toArray()[rand .nextInt(toAssign.size())].toString();
            String destServerKey = getMinRegionServer(allRegionServers);
            RegionServer rs = allRegionServers.get(destServerKey);
            if (rs.getRegions().size() > avgCnt ){
                // 当所有的regionServer中的region个数大于 平均个数的是停止分配，保证每个节点的region的个数尽可能的平均分配到各个节点上，
                // 不会导致最后每个regionServer的region 个数已经达到平均值，但是某些regionServer的region个数仍然> (avgCnt+ 1)
                break;
            }
            System.out.println(" get region toAssign" + region);
            String destServerName = rs.getServerName().getServerName();
            admin.move(regionInfo.getEncodedNameAsBytes(),Bytes.toBytes(destServerName));
            System.out.println(" reassign to " + destServerName);
            rs.addRegion(regionInfo.getRegionNameAsString());
        }

        System.out.println("=============== region reassign end ===============");
    }

    /**
     * 从regionserver中遍历得到最小的 region server 的hostname
     * @param allRegionServers
     * @return region server host name
     */
    public static String getMinRegionServer(Map<String,RegionServer> allRegionServers ){
        String key = "";
        Integer cnt = Integer.MAX_VALUE ;
        for (String hostname : allRegionServers.keySet() ) {
            if ( allRegionServers.get(hostname).getRegions().size() < cnt ){
                cnt = allRegionServers.get(hostname).getRegions().size();
                key = hostname;
            }
        }
        return  key;
    }
}
