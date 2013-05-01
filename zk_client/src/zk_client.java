import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.*;


public class zk_client implements Watcher{
	public static String randKey() {
		long randomKey;
		randomKey = (long)(Math.random() * (zk_client.maxKey+1));
		return randomKey.toString();
	}
	
	public static void main(String[] args) {
        if (args.length > 1) {
            System.err.println("USAGE: Executor hostPort  [args ...]");
            System.exit(2);
        }
        String hostPort = args[0];
        String znode = "/db1";
        long key;
        static final long maxKey = 268435456;
        
        zk_client zkClientObj = new zk_client();
        ZooKeeper zk = new ZooKeeper(hostPort, 3000, zkClientObj);
        byte[] tmp = new String("db1").getBytes("UTF-16");
        zk.create(znode, tmp, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //String filename = args[2];
        //String exec[] = new String[args.length - 3];
        //System.arraycopy(args, 3, exec, 0, exec.length);
        try {
        	
        	long startTime = System.currentTimeMillis();
        	
        	System.out.println("Start Time : " + startTime);
        	
        	String randKey = null;
        	byte[] retData = null;
        	for (int  i = 0; i < 100000; i++) {

        		randKey = zk_client.randKey();
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
