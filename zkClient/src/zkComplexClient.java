/*
 * Difference between Complex and Naive Clients:
 * 		Complex Client:
 * 			Writes:	Leader
 * 			Read: SSD
 * 		Naive Client:
 * 			Writes: Leader
 * 			Read: Round Robin
 */
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.*;

import java.io.File;
import java.io.FileOutputStream;


public class zkComplexClient implements Watcher{
	public ArrayList<Integer> partitions = new ArrayList<Integer>();
	static final String[] machines = {"172.22.138.16", "172.22.138.18", "172.22.138.52"};
	static final String[] ports = {"12181", "22181", "32181"};
	static final String[] znodes = {"/db1", "/db2", "/db3"};
	//BufferedWriter logger;
	String logFileName;
	/**
	 * 2D array of dimensions 3*3
	 * each row stands for the partition for which client wants to send read/write request
	 * 1st column in each row stands for the replica which is on SSD
	 * 2nd and 3rd columns are for HDD replicas
	 * 2nd stands for leader replica
	 */
	private ZooKeeper[][] paxosInstances = new ZooKeeper[3][3];
	private int noOfOps;
	private int naiveMode = -1;
	
	static long HKey = 0;
	static long LKey = 0;
	static final long maxKey = 268435456;		//hex - 10000000
	//static final long maxKey = 53687090;		//hex - 10000000   //TEMP
	
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
	
	private BufferedWriter getLoggerHandle() {
		
		File logFile = new File(this.logFileName);
		FileWriter fop = null;
		BufferedWriter out = null;
		try {
			// if file doesn't exists, then create it
			if (!logFile.exists()) {
				logFile.createNewFile();
			}
			fop = new FileWriter(logFile, false);
			out = new BufferedWriter(fop);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return out;		
	}
	
	private void closeLoggerHandle(BufferedWriter logger) {
		
		try {
			logger.flush();
			logger.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static zkComplexClient initializeZKClient() {
		
		zkComplexClient zkObj = new zkComplexClient();
		zkObj.partitions.add(0, new Integer(1));
		zkObj.partitions.add(1, new Integer(2));
		zkObj.partitions.add(2, new Integer(3));
		
		ZooKeeper read, write = null;
		int i = 0, j = 0, k = 1;
		String hostPort;
		try {
			// i - partition number
			// j - zk connections per replica
			/*
			 * 		j=0			j=1				j=2
			 * i=0	read[ssd]	write(L)/read	read
			 * i=1	read[ssd]	write(L)/read	read
			 * i=2	read[ssd]	wrute(L)/read	read
			 */
			for(i = 0; i < 3; i++) {
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
				}		//TEMP
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
		
		try {
			Thread.sleep(5000);
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
        	long currentWriteTime = 0;
        	
        	long counter = 0;
        	/*long readMin = 0;
        	long writeMin = 0;
        	long readMax = 0;
        	long writeMax = 0;*/
        	
        	String operation;
        	
        	System.out.println("Start Time : " + startTime);
        	BufferedWriter logger = getLoggerHandle();
        	logger.write("********************** START TIME: " + startTime + " ***********************\n");
        	closeLoggerHandle(logger);
        	
        	String randKey = null;
        	byte[] retData = null;
        	ByteArrayOutputStream bOut = new ByteArrayOutputStream(100);
        	
    		byte[] lastWrittenKey = new byte[16];//TEMP
    		Arrays.fill(lastWrittenKey, (byte)'0');//TEMP
			
    		// Round robin counts
			int[] rrCounts = new int[partitions.size()];
			
    		while(true) {
    			retData = null;
    			ZooKeeper zkFinal = null;
    			randKey = randKey();
    			operation = randOperation(readWritePercentage);
    			Collections.shuffle(partitions);
    			int randomPartition = partitions.get(0).intValue();
    			//LKey++;
    			//randKey = String.valueOf(HKey) + String.valueOf(LKey);
    			bOut.reset();
    			bOut.write(randKey.getBytes());

    			byte[] key = randKey.getBytes();
    			byte[] value = bOut.toByteArray();

    			byte[] forGetData = new byte[16];
    			byte[] forSetData = new byte[116];
    			Arrays.fill(forSetData, (byte)'0');
    			Arrays.fill(forGetData, (byte)'0');

    			System.arraycopy(key, 0, forSetData, 16-key.length, key.length);
    			System.arraycopy(value, 0, forSetData, 116-value.length, value.length);
    			System.arraycopy(key, 0, forGetData, 16-key.length, key.length);
    			try {
    				if(operation.equals("READ")) {
    					if ( naiveMode == -1 ) {
    						zkFinal = paxosInstances[randomPartition-1][0];
    					} else {
    						//naive mode
    						if ( naiveMode == 0 ) {
    							// RR
    							zkFinal = paxosInstances[randomPartition-1][rrCounts[randomPartition-1]%3];
    							rrCounts[randomPartition-1]++;
    						}
    					}
    					//System.out.println("TESTING" + /*new String(lastWrittenKey) +*/" "+ new String(new String(forGetData).getBytes()));
    					localStartTime = System.currentTimeMillis();
    					retData = zkFinal.getDataByKey(znodes[randomPartition-1], new String(forGetData));
    					totalReadTime += (System.currentTimeMillis() - localStartTime);        			
    					reads++;

    					//System.out.println(" Key : " + new String(forGetData) + ", from Partition : " + randomPartition + " Read Value - " + new String(retData) + ", Reads : " + reads + " *** read latency : " + totalReadTime/(reads));
    					if(retData == null) {
    						System.err.println("******************** retData is null ****************");
    					}
    				} else if(operation.equals("WRITE")) {
    					// index 1 replica must be the Leader replica
    					//if(Math.random() < 0.5) {
    						zkFinal = paxosInstances[randomPartition-1][1];
    						//System.out.println("******* WRITE: index 1 chosen ***************");
    					//} else {
    					//	zkFinal = paxosInstances[randomPartition-1][2];
    						//System.out.println("******* WRITE: index 2 chosen ***************");
    					//} //zkFinal = paxosInstances[randomPartition-1][0]; //TEMP
    					
    					localStartTime = System.currentTimeMillis();
    					zkFinal.setData(znodes[randomPartition-1], forSetData, -1);
    					currentWriteTime = (System.currentTimeMillis() - localStartTime); 
    					totalWriteTime += currentWriteTime;        		
    					writes++;

    					//retData = zkFinal.getDataByKey(znodes[randomPartition-1], new String(forGetData));
    					//if(retData == null) {
    					//	System.out.println("************ retData is null ******************");
    					//} else {
    					//	System.out.println("TEST: retData - "+new String(retData));
    					//	reads++;
    					//}
    					//Arrays.fill(lastWrittenKey, (byte)'0');//TEMP
    					//System.arraycopy(forSetData, 0, lastWrittenKey, 0, 16);//TEMP
    					//System.out.println("lastwrittenKey - "+new String(lastWrittenKey));
    					//System.out.println(" KeyValue:  " + new String(forSetData) + " KeyValueLength:  " + forSetData.length + "in Partition : " + randomPartition + " Writes : " + writes + " *** Write latency : " + totalWriteTime/(writes));
    					//System.out.println(" KeyValue:  " + new String(forSetData) + " KeyValueLength:  " + forSetData.length + "in Partition : " + randomPartition + " Writes : " + writes + " *** Current Write latency : " + currentWriteTime + " *** cumulativer Write Latency: " + totalWriteTime/writes);
    				}
    				counter++;

    			} catch (ConnectionLossException e) {
    				System.err.println("ConnectionLossException recved for PaxosInstance - ");
    				e.printStackTrace();
    			}
        		if(counter % 1000 == 0) {
        			logger = getLoggerHandle();
        			//System.out.println(" Progress : " + (float) i*100/maxKey+ " % " + " KeyValue:  " + new String(c) + " KeyValueLength:  " + c.length + " Writes : " + writes + " *** Write latency : " + totalWriteTime/(writes));
        			System.out.println("READS: " + reads + " TOTAL READ TIME: " + totalReadTime + " WRITES:" + writes + " TOTAL WRITE TIME: "  + totalWriteTime + " TOTAL TIME: " + (System.currentTimeMillis() - startTime));
        			logger.write("READS: " + reads + "; TOTAL READ TIME: " + totalReadTime + "; WRITES:" + writes + "; TOTAL WRITE TIME: "  + totalWriteTime + "; TOTAL TIME: " + (System.currentTimeMillis() - startTime));
        			//logger.newLine();
        			logger.write("; READ LATENCY: " + totalReadTime/(reads));
        			//logger.newLine();
        			logger.write("; WRITE LATENCY: " + totalWriteTime/(writes));
        			logger.newLine();
        			closeLoggerHandle(logger);
        		}
        		
        		if(counter == noOfOps) {
        			logger = getLoggerHandle();
        			logger.write("READS: " + reads + "; TOTAL READ TIME: " + totalReadTime + "; WRITES:" + writes + "; TOTAL WRITE TIME: "  + totalWriteTime + "; TOTAL TIME: " + (System.currentTimeMillis() - startTime));
        			//logger.newLine();
        			logger.write("; READ LATENCY: " + totalReadTime/(reads));
        			//logger.newLine();
        			logger.write("; WRITE LATENCY: " + totalWriteTime/(writes));
        			logger.newLine();
        			closeLoggerHandle(logger);
        			break;
        		}
        	}

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	
	void close() {
		int i = 0;
		int j = 0;
		for(; i < 3; i++) {
			j = 0;
			for(; j < 3; j++) {
				try {
					paxosInstances[i][j].close();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NullPointerException e) {
					e.printStackTrace();
				}
			}
		}	
	}
	
	
	//@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
	
	}
	
	public static void main(String[] args) {
		
		if(args.length < 3 || args.length > 4) {
			System.out.println("USAGE: <program> <workloadDescription> <logFilePath> <noOfOps> [<naiveMode> {RR=0,...}]");
			return;
		}
		
		float readWritePercentage = Float.parseFloat(args[0]);
		System.out.println("The user given load type is - "+String.valueOf(readWritePercentage));
	
		String logFileName = args[1];

		
        zkComplexClient zkObj = initializeZKClient();
        zkObj.logFileName = logFileName;
        zkObj.noOfOps = Integer.parseInt(args[2]);
        if ( args.length == 4 ) {
        	zkObj.naiveMode  = Integer.parseInt(args[3]);
        }
        zkObj.runReadWriteLoad(readWritePercentage);

        zkObj.close();
	}
}
