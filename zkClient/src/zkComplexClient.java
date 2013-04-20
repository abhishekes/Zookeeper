
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.*;



public class zkComplexClient implements Watcher{
	public ArrayList<Integer> replicas = new ArrayList<Integer>();
	static final String[] machines = {"172.22.138.16", "172.22.138.18", "172.22.138.52"};
	static final String[] ports = {"12181", "22181", "32181"};
	static final String[] znodes = {"/db1", "/db2", "/db3"};
	
	/**
	 * 2D array of dimensions 3*3
	 * each row stands for the replica for which client wants to send read/write request
	 * 1st column in each row stands for the replica which is on SSD
	 * 2nd and 3rd columns are for HDD replicas
	 */
	private ZooKeeper[][] paxosInstances = new ZooKeeper[3][3];
	
	static long HKey = 0;
	static long LKey = 0;
	static final long maxKey = 268435456;		//hex - 10000000
	//static final long maxKey = ;		//hex - 10000000
	
	public static String randKey() {
		long randomKey;
		randomKey = (long)(Math.random() * (zkComplexClient.maxKey) + 1);
		return new Long(randomKey).toString();
	}
	
	public static String randOperation(float readWritePercentage) {
		if(Math.random() >= (double) readWritePercentage) {
			return "READ";
		} else {
			return "WRITE";
		}
	}
	
	public static zkComplexClient initializeZKClient() {
		
		zkComplexClient zkObj = new zkComplexClient();
		zkObj.replicas.add(0, new Integer(1));
		zkObj.replicas.add(1, new Integer(2));
		zkObj.replicas.add(2, new Integer(3));
		
		ZooKeeper read, write = null;
		int i = 0, j = 0, k = 1;
		String hostPort;
		try {

			for(; i < 3; i++) {
				k = 1;
				hostPort = machines[i]+":"+ports[i];
				read = new ZooKeeper(hostPort, 30000, zkObj);
				
				byte[] tmp = new String(znodes[i]).getBytes("UTF-16");
				if (read.exists(znodes[i], null) == null) {
					read.create(znodes[i], tmp, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				zkObj.paxosInstances[i][0] = read;
				System.out.println("ZooKeeper connection for - (" + i +", "+ 0 + "), established with :" + hostPort);
				
				for(j = 0; j < 3; j++) {
					if(j != i) {
						hostPort = machines[j]+":"+ports[i];
						write = new ZooKeeper(hostPort, 30000, zkObj);
						zkObj.paxosInstances[i][k] = write;
						k++;
						System.out.println("ZooKeeper connection for - (" + i +", "+ String.valueOf(k-1) + "), established with :" + hostPort);
					}
				}
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
		
		return zkObj;
	}
	
	
	
	
	private void runReadWriteLoad(float readWritePercentage) {
		/**
		 * readWritePercentage specified the read/write distribution -
		 * 0 - read only
		 * 1 - write only
		 * 0-1 - read/write workload
		 */
		
		int reads = 0, writes = 0;
        
        try {
        	
        	long startTime = System.currentTimeMillis();
        	long localStartTime = 0;
        	long totalReadTime = 0;
        	long totalWriteTime = 0;
        	
        	long counter = 0;
        	long readMin = 0;
        	long writeMin = 0;
        	long readMax = 0;
        	long writeMax = 0;
        	
        	String operation;
        	
        	System.out.println("Start Time : " + startTime);
        	
        	String randKey = null;
        	byte[] retData = null;
        	ByteArrayOutputStream bOut = new ByteArrayOutputStream(100);
        	
        	while(true) {
        		ZooKeeper zkFinal = null;
        		randKey = randKey();
        		operation = randOperation(readWritePercentage);
        		Collections.shuffle(replicas);
        		int randomPartition = replicas.get(0).intValue();
        		//LKey++;
        		//randKey = String.valueOf(HKey) + String.valueOf(LKey);
        		
        		bOut.reset();
        		bOut.write(randKey.getBytes());
        		
        		byte[] key = randKey.getBytes();
        		byte[] value = bOut.toByteArray();
        		
        		byte[] c = new byte[116];
        		Arrays.fill(c, (byte)'0');
        		
        		System.arraycopy(key, 0, c, 16-key.length, key.length);
        		System.arraycopy(value, 0, c, 116-value.length, value.length);
        		
        		
        		if(operation.equals("READ")) {
        			zkFinal = paxosInstances[randomPartition-1][0];
        			localStartTime = System.currentTimeMillis();
        			retData = zkFinal.getDataByKey(znodes[randomPartition-1], randKey);
        			totalReadTime += (System.currentTimeMillis() - localStartTime);        			
        			reads++;
        			
        			System.out.println(" Key : " + randKey + "from Partition : " + randomPartition + "Reads : " + reads + " *** read latency : " + totalReadTime/(reads));
        		} else if(operation.equals("WRITE")) {
        			if(Math.random() < 0.5) {
        				zkFinal = paxosInstances[randomPartition-1][1];
        			} else {
        				zkFinal = paxosInstances[randomPartition-1][2];
        			}
            		localStartTime = System.currentTimeMillis();
        			zkFinal.setData(znodes[randomPartition-1], c, -1);
        			totalWriteTime += (System.currentTimeMillis() - localStartTime);        		
        			writes++;
        			
        			System.out.println(" KeyValue:  " + new String(c) + " KeyValueLength:  " + c.length + "in Partition : " + randomPartition + " Writes : " + writes + " *** Write latency : " + totalWriteTime/(writes));
        		}
        		counter++;
        	
            	
        		if(counter % 100 == 0 && writes > 0) {      			
        			//System.out.println(" Progress : " + (float) i*100/maxKey+ " % " + " KeyValue:  " + new String(c) + " KeyValueLength:  " + c.length + " Writes : " + writes + " *** Write latency : " + totalWriteTime/(writes));
        			System.out.println("Total Time : " + (System.currentTimeMillis() - startTime));
        			System.out.println("Reads : " + reads + "Total Read Time : " + totalReadTime + "Writes :" + writes +" Total Write Time : "  + totalWriteTime);
        		}
 
        		if(counter == 10)
        			break;
        	}

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub

		
	}
	
	public static void main(String[] args) {
		
		
		float readWritePercentage = Float.parseFloat(args[0]);
		System.out.println("The user given load type is - "+String.valueOf(readWritePercentage));
		
        zkComplexClient zkObj = initializeZKClient();
       
        zkObj.runReadWriteLoad(readWritePercentage);

	}
}
