import java.io.IOException;
import java.util.Arrays;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.*;



public class zkClient implements Watcher{
	static final long maxKey = 268435456;
	
	public static String randKey() {
		long randomKey;
		randomKey = (long)(Math.random() * (zkClient.maxKey) + 1);
		return new Long(randomKey).toString();
	}
	
	public static void main(String[] args) {
        if (args.length > 2) {
            System.err.println("USAGE: Executor hostPort  [args ...]");
            System.exit(2);
        }
        int[] distribution = new int[(int)(maxKey/10000) + 1];  
        System.out.println(distribution[10009]);
        String hostPort = args[0];
        String hostPort1 = args[1];
        
        String znode = "/db1";
        //long key;
        
        
        zkClient zkClientObj = new zkClient();
        zkClient zkClientObj1 = new zkClient();
        ZooKeeper zk = null;
        ZooKeeper zk1 = null;
        int reads = 0, writes = 1;
        byte[] tmp = null;
		try {
			zk = new ZooKeeper(hostPort, 30000, zkClientObj);
			zk1 = new ZooKeeper(hostPort, 30000, zkClientObj1);
			tmp = new String("db1").getBytes("UTF-16");
			if (zk.exists(znode, null) == null) {
				zk.create(znode, tmp, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
        //String filename = args[2];
        //String exec[] = new String[args.length - 3];
        //System.arraycopy(args, 3, exec, 0, exec.length);
        try {
        	
        	long startTime = System.currentTimeMillis();
        	long localStartTime = 0;
        	long totalReadTime = 0;
        	long totalWriteTime = 0;
        	
        	long readMin = 0;
        	long writeMin = 0;
        	long readMax = 0;
        	long writeMax = 0;
        	
        	System.out.println("Start Time : " + startTime);
        	
        	String randKey = null;
        	byte[] retData = null;
        	for (int  i = 0; i < 3000; i++) {

        		randKey = zkClient.randKey();
        		distribution[(int)((Long.parseLong(randKey)/10000))] += 1;
        		
        		if (Long.parseLong(randKey) % 2 == 0) {
        			localStartTime = System.currentTimeMillis();
        			retData = zk.getDataByKey(znode, randKey);
        			totalReadTime += (System.currentTimeMillis() - localStartTime);
        			reads++;
        		    
        		    
        			//zk.setData(znode, randKey.getBytes(), -1);
        		}else {
        			writes++;
        			localStartTime = System.currentTimeMillis();
        			zk1.setData(znode, randKey.getBytes(), -1);
        			totalWriteTime += (System.currentTimeMillis() - localStartTime);
        		    
        		    
        			//retData = zk.getDataByKey(znode, randKey);
        		}
        		
        		if(i % 100 == 0 && reads  > 0 && writes > 0) {      			
        			System.out.println(" Progress : " + (float) i/30+ " % " + " KeyValue:  " + new String(randKey) + " Reads : " +reads  +  " Writes : " + writes +   " *** Read latency : "+ totalReadTime/(reads)+ " *** Write latency : " + totalWriteTime/(writes));
        		}
        		
        		//zk.setData(znode, tmp , 0);   
        		//byte[] tmp1 = zk.getData(znode, false, null);
        		//System.out.println("Output:" + new String(tmp1,"UTF-16"));
        	}
        	System.out.println("Total Time : " + (System.currentTimeMillis() - startTime));
        	System.out.println(Arrays.toString(distribution));
        	System.out.println(" Reads : "+ reads+ " Total Read Time : "  + totalReadTime + "\nWrites : "+ writes +" Total Write Time : "  + totalWriteTime);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		
	}
	
}
