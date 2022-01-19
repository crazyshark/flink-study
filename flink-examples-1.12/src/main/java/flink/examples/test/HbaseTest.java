package flink.examples.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
//import org.apache.hadoop.hbase.client.TableDescriptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HbaseTest {

    public static void main(String[] args) throws IOException {
        /*Map<String,String> conf = new HashMap<>();
        conf.put("table-name","flink_sql_test:flink_sql_hbase_dim1");
        conf.put("bdp.hbase.instance.name","SL1000000004371");
        conf.put("bdp.hbase.accesskey","MZYH5UIKEY3BVGTTXC4BTZRHDI");
        conf.put("","");
        HbaseDB db = HbaseDB.create(conf);*/
        Configuration conf = new Configuration();
        //conf.set("table-name","flink_sql_test:flink_sql_hbase_dim1");
        conf.set("bdp.hbase.instance.name","SL1000000004371");
        conf.set("bdp.hbase.accesskey","MZYH5UIKEY3BVGTTXC4BTZRHDI");
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        /*for(TableDescriptor desc : admin.listTableDescriptors()){
            System.out.println(desc.getTableName());
        }*/
    }
}
