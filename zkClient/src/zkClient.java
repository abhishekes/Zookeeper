import java.io.IOException;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.*;


public class zkClient implements Watcher{
	static final long maxKey = 268435456;
	
	public static String randKey() {
		long randomKey;
		randomKey = (long)(Math.random() * (zkClient.maxKey+1));
		return new Long(randomKey).toString();
	}
	
	public static void main(String[] args) {
        if (args.length > 1) {
            System.err.println("USAGE: Executor hostPort  [args ...]");
            System.exit(2);
        }
        String hostPort = args[0];
        String znode = "/db1";
        long key;
        
        
        zkClient zkClientObj = new zkClient();
        ZooKeeper zk = null;
        byte[] tmp = null;
		try {
			zk = new ZooKeeper(hostPort, 3000, zkClientObj);
			tmp = new String("db1").getBytes("UTF-16");
			zk.create(znode, tmp, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
        	
        	System.out.println("Start Time : " + startTime);
        	
        	String randKey = null;
        	byte[] retData = null;
        	for (int  i = 0; i < 100000; i++) {

        		randKey = zkClient.randKey();
        		retData = zk.getDataByKey(znode, randKey);
        		
        		if(i % 50 == 0) {      			
        			System.out.println("Time : " + (System.currentTimeMillis() - startTime) + ", KeyValue: " + retData.toString());
        		}
        		
        		//zk.setData(znode, tmp , 0);   
        		//byte[] tmp1 = zk.getData(znode, false, null);
        		//System.out.println("Output:" + new String(tmp1,"UTF-16"));
        	}
        	System.out.println("Total Time : " + (System.currentTimeMillis() - startTime));
        	
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		
	}
	
}
