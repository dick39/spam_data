import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {
	private static Configuration conf = HBaseConfiguration.create();
	
	// 批量插入数据的参数配置
	private static boolean autoFlush = false;
	private static Durability durability = Durability.SKIP_WAL;
	private static long writeBuffer = 24 * 1024 * 1024;
	
	/**
	 * 创建表
	 * 
	 * @throws IOException
	 */
	public static void createTable(String tableName, String... colFamily) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			if(admin.tableExists(tableName)) {
				System.out.println("表已经存在！");
			}
			else {
				HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
				for(String column : colFamily) {
					desc.addFamily(new HColumnDescriptor(column));
				}
				admin.createTable(desc);
				System.out.println("表创建成功！");
			}
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally {
			if(null != admin) {
				try {
					admin.close();
				}
				catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 插入一行记录
	 * 
	 * @param tablename 表名
	 * @param row 行名称
	 * @param columnFamily 列族名
	 * @param columns （列族名：column）组合成列名
	 * @param values 行与列确定的值
	 */
	public static void insertRecord(String tablename, String row, String columnFamily, String[] columns, String[] values) {
		HTable table = null;
		try {
			table = new HTable(conf, tablename);
			Put put = new Put(Bytes.toBytes(row));
			for(int i = 0; i < columns.length; i++) {
				put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(String.valueOf(columns[i])), Bytes.toBytes(values[i]));
				table.put(put);
			}
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally {
			if(null != table) {
				try {
					table.close();
				}
				catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 删除一行记录
	 * 
	 * @param tablename 表名
	 * @param rowkey 行名
	 * @throws IOException
	 */
	public static void deleteRow(String tablename, String rowkey) {
		HTable table = null;
		try {
			table = new HTable(conf, tablename);
			List<Delete> list = new ArrayList<Delete>();
			Delete d1 = new Delete(rowkey.getBytes());
			list.add(d1);
			table.delete(list);
			System.out.println("删除行成功！");
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally {
			if(null != table) {
				try {
					table.close();
				}
				catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 查找一行记录
	 * 
	 * @param tablename 表名
	 * @param rowkey 行名
	 */
	public static void selectRow(String tablename, String rowKey) {
		HTable table = null;
		try {
			table = new HTable(conf, tablename);
			long all = System.currentTimeMillis();
			Get g = new Get(rowKey.getBytes());
			Result rs = table.get(g);
			List<Cell> listCells = rs.listCells(); // 指定行、全部列族的全部列
			
			for(Cell cell : listCells) {
				System.out.println("列  族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
				System.out.println("列  名:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
				System.out.println("列  值：" + Bytes.toString(CellUtil.cloneValue(cell)));
				System.out.println("时间戳：" + cell.getTimestamp());
			}
			long end = System.currentTimeMillis();
			System.out.println("total need time = " + (end - all) * 1.0 / 1000 + "s");
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally {
			if(null != table) {
				try {
					table.close();
				}
				catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void selectByFilter(String tablename, List<String> arr) throws IOException {
		HTable table = null;
		try {
			long all = System.currentTimeMillis();
			table = new HTable(conf, tablename);
			FilterList filterList = new FilterList();
			Scan s1 = new Scan();
			for(String v : arr) { // 各个条件之间是“与”的关系
				String[] s = v.split(",");
				filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]), CompareOp.EQUAL, Bytes.toBytes(s[2])));
				// 添加下面这一行后，则只返回指定的cell，同一行中的其他cell不返回
				// s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
			}
			s1.setFilter(filterList);
			ResultScanner ResultScannerFilterList = table.getScanner(s1);
			for(Result rr = ResultScannerFilterList.next(); rr != null; rr = ResultScannerFilterList.next()) {
				List<Cell> listCells = rr.listCells(); // 指定行、全部列族的全部列
				
				for(Cell cell : listCells) {
					System.out.println("列  族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
					System.out.println("列  名:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
					System.out.println("列  值：" + Bytes.toString(CellUtil.cloneValue(cell)));
					System.out.println("时间戳：" + cell.getTimestamp());
				}
			}
			long end = System.currentTimeMillis();
			System.out.println("total need time = " + (end - all) * 1.0 / 1000 + "s");
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally {
			if(null != table) {
				try {
					table.close();
				}
				catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void selectColumnsByFilter(String tablename, List<String> arr) throws IOException {
		HTable table = null;
		try {
			table = new HTable(conf, tablename);
			FilterList filterList = new FilterList();
			Scan s1 = new Scan();
			// s1.setCaching(10000);
			for(String v : arr) { // 各个条件之间是“与”的关系
				String[] s = v.split(",");
				if(null != s[2] && !"%".equals(s[2])) {
					filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]), CompareOp.EQUAL, Bytes.toBytes(s[2])));
				}
				// 添加下面这一行后，则只返回指定的cell，同一行中的其他cell不返回
				s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
			}
			s1.setFilter(filterList);
			long all = System.currentTimeMillis();
			ResultScanner ResultScannerFilterList = table.getScanner(s1);
			for(Result rr = ResultScannerFilterList.next(); rr != null; rr = ResultScannerFilterList.next()) {
				List<Cell> listCells = rr.listCells(); // 指定行、全部列族的全部列
				
				System.out.println("行号：" + Bytes.toString(rr.getRow()));
				for(Cell cell : listCells) {
					System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + ",");
					System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell)) + ",");
					System.out.print(Bytes.toString(CellUtil.cloneValue(cell)) + ",");
					System.out.print(cell.getTimestamp() + "\r\n");
				}
			}
			long end = System.currentTimeMillis();
			System.out.println("total need time = " + (end - all) * 1.0 / 1000 + "s");
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally {
			if(null != table) {
				try {
					table.close();
				}
				catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 查询表中所有行
	 * 
	 * @param tablename
	 */
	public static void scanAllRecord(String tablename) {
		HTable table = null;
		try {
			table = new HTable(conf, tablename);
			Scan s = new Scan();
			ResultScanner rs = table.getScanner(s);
			for(Result r : rs) {
				List<Cell> listCells = r.listCells(); // 指定行、全部列族的全部列
				
				for(Cell cell : listCells) {
					System.out.println("列  族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
					System.out.println("列  名:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
					System.out.println("列  值：" + Bytes.toString(CellUtil.cloneValue(cell)));
					System.out.println("时间戳：" + cell.getTimestamp());
				}
			}
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally {
			if(null != table) {
				try {
					table.close();
				}
				catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void scanRecord(String tablename, String startRow, String stopRow) {
		HTable table = null;
		try {
			table = new HTable(conf, tablename);
			Scan s = new Scan();
			s.setStartRow(Bytes.toBytes(startRow));
			s.setStopRow(Bytes.toBytes(stopRow));
			ResultScanner rs = table.getScanner(s);
			for(Result r : rs) {
				List<Cell> listCells = r.listCells(); // 指定行、全部列族的全部列
				
				System.out.println("行号：" + Bytes.toString(r.getRow()));
				for(Cell cell : listCells) {
					System.out.print(" 列  族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
					System.out.print(" 列  名:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
					System.out.print(" 列  值：" + Bytes.toString(CellUtil.cloneValue(cell)));
					System.out.print(" 时间戳：" + cell.getTimestamp());
					System.out.println();
				}
			}
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally {
			if(null != table) {
				try {
					table.close();
				}
				catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 删除表操作
	 * 
	 * @param tablename
	 * @throws IOException
	 */
	public static void deleteTable(String tablename) throws IOException {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			admin.disableTable(tablename);
			admin.deleteTable(tablename);
			System.out.println("表删除成功！");
		}
		catch(MasterNotRunningException e) {
			e.printStackTrace();
		}
		catch(ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
		finally {
			if(null != admin) {
				try {
					admin.close();
				}
				catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static long rowCount(String tableName, String family) {
		String coprocessorClassName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
		addTableCoprocessor(tableName, coprocessorClassName);
		
		AggregationClient ac = new AggregationClient(conf);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(family));
		long rowCount = 0;
		try {
			rowCount = ac.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), scan);
		}
		catch(Throwable e) {
			e.printStackTrace();
		}
		return rowCount;
	}
	
	private static void addTableCoprocessor(String tableName, String coprocessorClassName) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			boolean isTableAvailable = admin.isTableAvailable(tableName);
			if(!isTableAvailable) {
				admin.enableTable(tableName);
			}
			admin.disableTable(tableName);
			
			HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(tableName));
			boolean hasCoprocessor = htd.hasCoprocessor(coprocessorClassName);
			if(hasCoprocessor) {
				htd.removeCoprocessor(coprocessorClassName);
			}
			htd.addCoprocessor(coprocessorClassName);
			
			admin.modifyTable(Bytes.toBytes(tableName), htd);
			admin.enableTable(tableName);
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally {
			if(null != admin) {
				try {
					admin.close();
				}
				catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static void insert(long count, boolean wal, boolean autoFlush, long writeBuffer) {
		HBaseAdmin admin = null;
		HTable table = null;
		try {
			String tableName = "etltest";
			
			admin = new HBaseAdmin(conf);
			
			if(admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
			
			HTableDescriptor t = new HTableDescriptor(TableName.valueOf(tableName));
			t.addFamily(new HColumnDescriptor("f1"));
			t.addFamily(new HColumnDescriptor("f2"));
			t.addFamily(new HColumnDescriptor("f3"));
			t.addFamily(new HColumnDescriptor("f4"));
			admin.createTable(t);
			System.out.println("table created");
			
			table = new HTable(conf, tableName);
			table.setAutoFlush(autoFlush, true);
			if(writeBuffer != 0) {
				table.setWriteBufferSize(writeBuffer);
			}
			List<Put> lp = new ArrayList<Put>();
			long all = System.currentTimeMillis();
			
			System.out.println("start time = " + all);
			for(int i = 1; i <= count; ++i) {
				Put p = new Put(new String("row_" + i).getBytes());
				p.add("f1".getBytes(), null, Bytes.toBytes(i + ""));
				p.add("f2".getBytes(), null, Bytes.toBytes(i + ""));
				p.add("f3".getBytes(), null, Bytes.toBytes(i + ""));
				p.add("f4".getBytes(), null, Bytes.toBytes(i + ""));
				p.setDurability(Durability.SKIP_WAL);
				lp.add(p);
				if(i % 10000 == 0) {
					table.put(lp);
					table.flushCommits();
					lp.clear();
				}
				else
					if(count == i) {
						table.put(lp);
						table.flushCommits();
						lp.clear();
					}
			}
			
			System.out.println("WAL=" + wal + ",autoFlush=" + autoFlush + ",buffer=" + writeBuffer + ",count=" + count);
			long end = System.currentTimeMillis();
			System.out.println("total need time = " + (end - all) * 1.0 / 1000 + "s");
			
			System.out.println("insert complete" + ",costs:" + (System.currentTimeMillis() - all) * 1.0 / 1000 + "ms");
		}
		catch(MasterNotRunningException e) {
			e.printStackTrace();
		}
		catch(ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally {
			if(null != admin) {
				try {
					admin.close();
				}
				catch(IOException e) {
					e.printStackTrace();
				}
			}
			if(null != table) {
				try {
					table.close();
				}
				catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static void insertTest(long count, String tableName, String colFamily, String[] columns, List<String[]> valList) {
		HBaseAdmin admin = null;
		HTable table = null;
		try {
			admin = new HBaseAdmin(conf);
			
			if(admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
			
			createTable(tableName, colFamily);
			System.out.println("table created");
			
			table = new HTable(conf, tableName);
			table.setAutoFlush(autoFlush, true);
			if(writeBuffer != 0) {
				table.setWriteBufferSize(writeBuffer);
			}
			
			List<Put> lp = new ArrayList<Put>();
			long all = System.currentTimeMillis();
			
			System.out.println("start time = " + all);
			for(int i = 0; i < count; ++i) {
				Put p = new Put(new String("row_" + i).getBytes());
				
				for(int j = 0; j < columns.length; j++) {
					p.add(colFamily.getBytes(), Bytes.toBytes(columns[j]), Bytes.toBytes(valList.get(i)[j]));
				}
				
				p.setDurability(durability);
				lp.add(p);
				if(i % 10000 == 0) {
					table.put(lp);
					table.flushCommits();
					lp.clear();
				}
				else
					if(count == i) {
						table.put(lp);
						table.flushCommits();
						lp.clear();
					}
			}
			
			System.out.println("Durability=" + durability + ",autoFlush=" + autoFlush + ",buffer=" + writeBuffer + ",count=" + count);
			long end = System.currentTimeMillis();
			System.out.println("total need time = " + (end - all) * 1.0 / 1000 + "s");
			
			System.out.println("insert complete" + ",costs:" + (System.currentTimeMillis() - all) * 1.0 / 1000 + "ms");
		}
		catch(MasterNotRunningException e) {
			e.printStackTrace();
		}
		catch(ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally {
			if(null != admin) {
				try {
					admin.close();
				}
				catch(IOException e) {
					e.printStackTrace();
				}
			}
			if(null != table) {
				try {
					table.close();
				}
				catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private void createPutList(long count, String colFamily, String... columns) {
		List<Put> lp = new ArrayList<Put>();
		long all = System.currentTimeMillis();
		
		System.out.println("start time = " + all);
		for(int i = 1; i <= count; ++i) {
			Put p = new Put(new String("row_" + i).getBytes());
			
			for(int j = 0; j < columns.length; j++) {
				p.add(colFamily.getBytes(), Bytes.toBytes(columns[j]), Bytes.toBytes(i + ""));
			}
			
			p.setDurability(Durability.SKIP_WAL);
			lp.add(p);
		}
	}
	
	private static void createData(int count) {
		createTable("fullmessage", "colFamily");
		String[] cols = {"msgId", "userId", "sendId", "date", "md5", "contentType", "contentLength", "content", "filePath", "filterResult",
				"policyType", "policyDetail", "policyId", "svcType" };
		String[] values = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14" };
		for(int i = 0; i < count; i++) {
			insertRecord("fullmessage", "row" + (i + 1), "colFamily", cols, values);
		}
	}
	
	public static void main(String[] args) throws Throwable {
		String[] cols = {"msgId", "userId", "sendId", "date", "md5", "contentType", "contentLength", "content", "filePath", "filterResult",
				"policyType", "policyDetail", "policyId", "svcType" };
				
		long count = 1000000;
		List<String[]> valList = new ArrayList<String[]>();
		
		for(int i = 0; i < count; i++) {
			Random r = new Random();
			String msgId = UuidUtil.getUuid();
			String md5 = EncryptUtil.encodeMD5String(r.nextInt(10000) + "");
			String[] values = {msgId, "2", "3", "4", md5, "6", "7", "8", "9", "10", "11", "12", "13", "14" };
			valList.add(values);
		}
		
		// insertTest(count, "fullmessage", "full", cols, valList);
		//
		// System.out.println(rowCount("fullmessage", "full"));
		// scanRecord("fullmessage", "row_1", "row_2");
		
		List<String> arr = new ArrayList<String>();
		// arr.add("full,msgId,c5f9878d705440879b18235e6d0e92c5");
		arr.add("full,userId,%");
		arr.add("full,md5,1e0f65eb20acbfb27ee05ddc000b50ec");
		// selectByFilter("fullmessage", arr);
		selectColumnsByFilter("fullmessage", arr);
		
		selectRow("fullmessage", "row_555555");
		// System.out.println("1e0f65eb20acbfb27ee05ddc000b50ec".getBytes());
	}
}
