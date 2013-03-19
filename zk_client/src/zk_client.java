import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.*;

public class zk_client implements Watcher{
	public static void main(String[] args) {
        if (args.length > 1) {
            System.err.println("USAGE: Executor hostPort  [args ...]");
            System.exit(2);
        }
        String hostPort = args[0];
        String znode = "/group";
        
        //String filename = args[2];
        //String exec[] = new String[args.length - 3];
        //System.arraycopy(args, 3, exec, 0, exec.length);
        try {
        	ZooKeeper zk = new ZooKeeper(hostPort, 3000, new zk_client());
        	long startTime = System.currentTimeMillis();
        	
        	System.out.println("Start Time : " + startTime);
        	
        	for (int  i = 501; i < 1000; i++) {


        		byte[] tmp = new String("Test" + i).getBytes("UTF-16");
        		
        		zk.create(znode + i, tmp, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
