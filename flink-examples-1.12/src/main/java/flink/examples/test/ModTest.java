package flink.examples.test;

public class ModTest {

    public static void main(String[] args){
        long intervalMs = 1000l;
        long now = System.currentTimeMillis();
        long currentBatch = now - now % intervalMs;
        System.out.println(currentBatch);
    }
}
