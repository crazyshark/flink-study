package flink.examples.test;

import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class JavaHashTest {

    public static void main(String[] args) throws NoSuchAlgorithmException {
        //byte[] data = Bytes.toBytes("gege");
        byte[] data = "gege".getBytes();
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] thedigest = md.digest(data);
        String str = new String(data);
        String str1 = new String(thedigest);
        System.out.println(str);
        System.out.println(str1);
        md.reset();

        byte[] data2 = "hehe".getBytes();
        byte[] thedigest2 = md.digest(data2);
        String str2 = new String(data2);
        String str3 = new String(thedigest2);
        System.out.println(str2);
        System.out.println(str3);
        md.reset();

        //byte[] data3 = Bytes.toBytes("gege");
        byte[] data3 = "gege".getBytes();
        //MessageDigest md2 = MessageDigest.getInstance("MD5");
        byte[] thedigest3 = md.digest(data3);
        String str4 = new String(data3);
        String str5 = new String(thedigest3);
        System.out.println(str4);
        System.out.println(str5);


    }
}
