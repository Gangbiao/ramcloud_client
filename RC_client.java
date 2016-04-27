//package rc2;

import java.io.File;
import java.io.FileInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Locale;
import java.util.Properties;
import java.net.URI;
import java.net.URISyntaxException;

import cz.adamh.utils.*;
import edu.stanford.ramcloud.*;

public class RC_client {
	
		private static final int BUFFER_SIZE =1048576; //1024*1024
		
		public static void main(String[] args) throws IOException {
			// TODO Auto-generated method stub
			String locator = "tcp:host=10.13.30.242,port=11114";
			String clusterName = "main";
			//String srcFileName = "/home/hadoop/test.data";
			//String dstFileName="/home/hadoop/test11.data";
			String cmd = args[0];
			String src = args[1];
			//String dst = args[2];
			if(cmd.equals("put"))
				putToRC(locator, clusterName, src);
			else if(cmd.equals("get")){
				String dst = args[2];
				getFromRC(locator, clusterName, src, dst);
			}
			else
				System.out.println("cmd error,the cmd is put or get");

			//putToRC(locator, clusterName, srcFileName);	
			//getFromRC(locator, clusterName, srcFileName,dstFileName);
			
		}
		
		/**
		 * @param locator
		 * @param clusterName
		 * @param srcFileName
		 * 			full path of the file to be put to the ramcloud.
		 * @throws IOException 
		 */
		public static void putToRC(String locator, String clusterName, String srcFileName) throws IOException {
		/*将一个文件切割成多个碎片。
		 * 1，读取源文件，将源文件的数据分别存储到多个RAMcloud 对象中。
		 * 2，按照指定大小分割（）
		 * 3，每一个碎片都需要编号，格式为 "i.part"。
		 * 4，最后将count也存储到表中，读取数据时候要用到
		 * 5，表名为文件全路径名
		 */
		RAMCloud client = new RAMCloud(locator, clusterName);
		
		File srcFile = new File(srcFileName);
		//判断文件是否存在。
		if(!(srcFile.exists() && srcFile.isFile())){
			throw new RuntimeException("源文件不是正确的文件或者不存在");
		}
		
		//1，使用字节流读取流和源文件关联。
		FileInputStream fis = new FileInputStream(srcFile);
		
		//2，一个文件对应一个表，表名字为文件全路径名
		String tableName = srcFileName;
		client.createTable(tableName);
		//3，定义缓冲区。1M.
		byte[] buf = new byte[BUFFER_SIZE];
		
		//4，频繁读写操作。
		int len = 0;
		int count = 1; //碎片文件的起始编号。
		long tableId = client.getTableId(tableName);
		while((len=fis.read(buf))!=-1){
				//创建输出流对象。只要满足了缓冲区大小，碎片数据确定，直接往碎片文件中写数据 。
				//碎片文件存储到partsDir中，名称为编号+part扩展名。
				String objectName = (count++) + ".part";			
				client.write(tableId, objectName, buf);
			}
		
		//将文件分块的个数也写到表中，读取文件到本地时候会用到
		// "" + count; //将整形转换为字符串,实际part数量为count-1
		client.write(tableId,"countOfPart", ""+count);
		System.out.println("count is "+count );
		
		client.disconnect();
		fis.close();
		}
		
		/**
		 * 将ramcloud中的文件拷贝到本地
		 * @param locator
		 * @param clusterName
		 * @param srcFileName
		 * @param dstFileName
		 * 
		 * @throws IOException 
		 */
		public static void getFromRC(String locator, String clusterName, String srcFileName, String dstFileName) throws IOException {
				RAMCloud client = new RAMCloud(locator, clusterName);
				String tableName = srcFileName;
				long tableId  = client.getTableId(tableName);
				System.out.println("table ID is" +tableId);  //   test
				
				/*获取文件分片数量,转换为整型*/
				RAMCloudObject  object  = client.read(tableId, "countOfPart");
				String str_count = object.getValue();
				int count =  Integer.parseInt(str_count);
				System.out.println("get count is "+str_count);  //   test
				
				File file=new File(dstFileName);
				FileOutputStream out=new FileOutputStream(file);
				//遍历表，并追加写到out流中
				for(int i=1; i<count; i++){
					object = client.read(tableId,  i+".part");
					out.write(object.getValueBytes());
				}
				//关闭连接和输出流
				client.disconnect();
				out.close();
		}
}
