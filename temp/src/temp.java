import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;


public class temp {
    
	public static void main(String[] args) {
		Long val = new Long(3463453);
		byte key[] = val.toString().getBytes(); 
		//generateFile();
		System.out.println("1.. " + new String(getData_Disk("/data.", key)));
		setData_Disk("/data.", key);
		System.out.println("2.. " +new String(getData_Disk("/data.", key)));
    }
    public static void setData_Disk(String path, byte data[]) {
    	String filePath = "/home/abhishek/Desktop/tmp/tobedel" + path;
    	Integer numOfRows = 2048;
    	Integer keyLength = 32;
    	//Read existing data from disk, update new data and write to disk
    	try {
    		
    		//Calculate the destination file
    		
    		byte fileData[] = new byte[numOfRows * keyLength]; 
    		Integer chunk = (int) java.lang.Math.ceil(Long.parseLong(new String(data)) / (double) numOfRows) ;
    		//Read existing data
    		FileInputStream input = new FileInputStream(filePath + chunk.toString());
    		input.read(fileData);
    		input.close();
    		byte tempkey[] = new byte[keyLength];
    		System.arraycopy(new Long((Long.parseLong(new String(data)) + 1)).toString().getBytes(), 0, tempkey, 0, data.length);
    		//System.out.println(chunk + " " + (Integer.parseInt(new String(data)) - (chunk-1)*numOfRows - 1) * keyLength);

    		System.arraycopy(tempkey, 0, fileData, (int)((Long.parseLong(new String(data)) - (chunk-1)*numOfRows - 1) * keyLength), keyLength);
    		
    		
    		//System.arraycopy(fileData, 599*keyLength, tempkey, 0, keyLength);
    		//System.out.println(new String(tempkey));
    		
    		
    		FileOutputStream output = new FileOutputStream(new File(filePath + chunk.toString()));
    		output.write(fileData);
    		output.close();
    		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Failed to create data file");
			e.printStackTrace();
		} 
   
    }
    public static byte[] getData_Disk(String path, byte data[]) {
    	String filePath = "/home/abhishek/Desktop/tmp/tobedel/data.";
    	Integer numOfRows = 2048;
    	Integer keyLength = 32;
    	//Read existing data from disk, update new data and write to disk
    	byte[] tempkey = null;
    	try {
    		
    		//Calculate the destination file
    		
    		byte fileData[] = new byte[numOfRows * keyLength]; 
    		Integer chunk = (int) java.lang.Math.ceil(Long.parseLong(new String(data)) / (double) numOfRows) ;
    		//Read existing data
    		FileInputStream input = new FileInputStream(filePath + chunk.toString());
    		input.read(fileData);
    		input.close();
    		tempkey = new byte[keyLength];
    		System.arraycopy(fileData,(int)( (Long.parseLong(new String(data)) - (chunk-1)*numOfRows - 1) * keyLength),tempkey,0, keyLength);
    		System.out.println(new String (tempkey));
    		//System.out.println(chunk + " " + (Integer.parseInt(new String(data)) - (chunk-1)*numOfRows - 1) * keyLength);

    		//System.arraycopy(tempkey, 0, fileData, (int)((Long.parseLong(new String(data)) - (chunk-1)*numOfRows - 1) * keyLength), keyLength);
    		
    		
    		//System.arraycopy(fileData, 599*keyLength, tempkey, 0, keyLength);
    		//System.out.println(new String(tempkey));
    		
    		
    		//FileOutputStream output = new FileOutputStream(new File(filePath + chunk.toString()));
    		//output.write(fileData);
    		//output.close();
    		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Failed to create data file");
			e.printStackTrace();
		} 
    	return tempkey;
    }
    
    public static String getFilePath(Long key) {
    	return null;
    }
    
    
    
    public static void generateFile() {
    	String filePath = /*"/ssd/zk_data/db"*/ "/home/abhishek/Desktop/tmp/tobedel";
    			              
    			                       
    	                     
    	
    	Integer numOfRows = 2048;
    	Integer keyLength = 32;
    	
		
		
        Long j = new Long(1);  
		for (Long i = new Long(1) ; i <= (8 * 1024 * 1024) / 64; i++) {
			try {
				FileOutputStream output;
				
				
				byte fileData[] = new byte[numOfRows * keyLength]; 
				byte tempkey[] = new byte[keyLength];
				for (int k = 0 ; k < 2048; k++) {
					System.arraycopy(j.toString().getBytes(), 0, tempkey, 0, j.toString().getBytes().length);
					System.arraycopy(tempkey, 0, fileData, k * keyLength, keyLength);
					j++;
				}
				output = new FileOutputStream(new File(filePath + "/data." + i  ));
				output.write(fileData);
				
			    
			}catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
   		/*FileOutputStream output;
		try {
			output = new FileOutputStream(new File(filePath));
			output.write(fileData);
			output.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/

    
    }
}
