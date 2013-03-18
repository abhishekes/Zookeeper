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
        String znode = "/group5";
        //String filename = args[2];
        //String exec[] = new String[args.length - 3];
        //System.arraycopy(args, 3, exec, 0, exec.length);
        try {
            byte[] tmp = new String("Test").getBytes("UTF-16");
        	ZooKeeper zk = new ZooKeeper(hostPort, 3000, new zk_client());
            zk.create(znode, tmp, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        	//zk.setData(znode, tmp , 0);   
            byte[] tmp1 = zk.getData(znode, false, null);
            System.out.println("Output:" + new String(tmp1,"UTF-16"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		
	}
	
}
