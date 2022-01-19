package flink.examples.test;

import org.apache.hadoop.hbase.ServerName;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by angel；
 */
public class RegionServer {

    private ServerName serverName;

    private List<String> regions ;

    public RegionServer(){
        super();
        this.regions = new ArrayList<String>();
    }

    public ServerName getServerName() {
        return serverName;
    }

    public void setServerName(ServerName serverName) {
        this.serverName = serverName;
    }


    public List<String> getRegions() {
        return regions;
    }

    public void setRegions(List<String> regions) {
        this.regions = regions;
    }
    public void addRegion ( String region ) {
        this.regions.add(region);
    }

}