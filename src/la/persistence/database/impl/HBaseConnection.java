package la.persistence.database.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseConnection {
	private HBaseAdmin hba;
	private Configuration hconfig;
	private HashMap<String, HTableDescriptor> tables;
	private Configuration conf;

	public HBaseConnection() throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		conf = new Configuration();
		this.hconfig = HBaseConfiguration.create(conf);
		this.hba = new HBaseAdmin(hconfig);
		this.tables = new HashMap<String, HTableDescriptor>();
	}

	/******************************************************************
	 * CREATE
	 ******************************************************************/

	@SuppressWarnings("deprecation")
	public void createTable(String tableName, String columnFamily)
			throws IOException {
		HTableDescriptor ht;
		if ((ht = tables.get(tableName)) == null)
			ht = new HTableDescriptor(tableName);

		ht.addFamily(new HColumnDescriptor(columnFamily));
		hba.createTable(ht);
	}

	/******************************************************************
	 * WRITE
	 ******************************************************************/

	@SuppressWarnings("resource")
	public void writeValue(String tablename, int row, String columnfamily,
			String column, Object value, long timestamp)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			HTable table = new HTable(conf, tablename);
			Put put = new Put(Bytes.toBytes(row));
			if (value instanceof Double) {
				put.add(Bytes.toBytes(columnfamily), Bytes.toBytes(column),
						timestamp, Bytes.toBytes((double) value));
			} else if (value instanceof Integer) {
				put.add(Bytes.toBytes(columnfamily), Bytes.toBytes(column),
						timestamp, Bytes.toBytes((int) value));
			} else if (value instanceof String) {
				put.add(Bytes.toBytes(columnfamily), Bytes.toBytes(column),
						timestamp, Bytes.toBytes((String) value));
			} else {
				throw new IOException();
			}
			table.put(put);
		} finally {
			admin.close();
		}
	}

	@SuppressWarnings("resource")
	public void writeValues(String tablename, int row, String columnfamily,
			Map<String, String> values, long startTimestamp) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			HTable table = new HTable(conf, tablename);
			Put put = new Put(Bytes.toBytes(row));
			long timestamp = startTimestamp;
			for (String column : values.keySet()) {
				put.add(Bytes.toBytes(columnfamily), Bytes.toBytes(column),
						timestamp, Bytes.toBytes(values.get(column)));
				timestamp++;
			}
			table.put(put);
		} finally {
			admin.close();
		}
	}

	/******************************************************************
	 * READ
	 ******************************************************************/

	/**
	 * Liefert ein Value eines Keys mit einem bestimmten Timestamp
	 * 
	 * @param tablename
	 * @param row
	 * @param columnfamily
	 * @param column
	 * @param timestamp
	 * @return
	 * @throws java.io.IOException
	 */
	@SuppressWarnings("resource")
	public byte[] readValue(String tablename, int row, String columnfamily,
			String column, long timestamp) throws IOException {
		HTable table = new HTable(conf, tablename);
		Get get = new Get(Bytes.toBytes(row));
		Scan scan = new Scan(get);
		scan.setTimeStamp(timestamp);
		ResultScanner resultScanner = table.getScanner(scan);
		Result result = resultScanner.next();
		return (result == null) ? null : result.value();

	}

	/**
	 * Liefert aktuellsten Value einer Row
	 * 
	 * @param tablename
	 * @param row
	 * @param columnfamily
	 * @param column
	 * @return
	 * @throws java.io.IOException
	 */
	@SuppressWarnings("resource")
	public byte[] readValue(String tablename, int row, String columnfamily,
			String column) throws IOException {
		HTable table = new HTable(conf, tablename);
		Get get = new Get(Bytes.toBytes(row));
		return table.get(get).value();
	}

	/**
	 * liefert alle akteullen Values einer Column aller Rows
	 * 
	 * @param tablename
	 * @param columnfamily
	 * @param column
	 * @return
	 * @throws java.io.IOException
	 */
	@SuppressWarnings("resource")
	public List<byte[]> readValues(String tablename, String columnfamily,
			String column) throws IOException {
		HTable table = new HTable(conf, tablename);
		Scan scan = new Scan();
		ResultScanner resultScanner = table.getScanner(scan);
		List<byte[]> resultList = new ArrayList<byte[]>();
		for (Result result : resultScanner) {
			resultList.add(result.getValue(Bytes.toBytes(columnfamily),
					Bytes.toBytes(column)));
		}
		return resultList;
	}

	/**
	 * Liefert alle Values einer bestimmten Column einer Row
	 * 
	 * @param tablename
	 * @param row
	 * @param columnfamily
	 * @param column
	 * @param startTimestamp
	 * @return
	 * @throws java.io.IOException
	 */
	@SuppressWarnings("resource")
	public List<byte[]> readValues(String tablename, int row,
			String columnfamily, String column, long startTimestamp)
			throws IOException {
		long timestamp = startTimestamp;
		HTable table = new HTable(conf, tablename);
		Get get = new Get(Bytes.toBytes(row));
		Scan scan = new Scan(get);
		scan.setTimeStamp(timestamp);
		List<byte[]> resultList = new ArrayList<byte[]>();
		ResultScanner resultScanner = table.getScanner(scan);
		Result result = resultScanner.next();
		while (result != null) {
			resultList.add(result.value());
			timestamp++;
			scan.setTimeStamp(timestamp);
			resultScanner = table.getScanner(scan);
			result = resultScanner.next();
		}
		return resultList;
	}
	
	@SuppressWarnings("resource")
	public List<List<byte[]>> readAllColumn(String tablename, int row,
			String columnfamily, List<String> columns) throws IOException {
		long timestamp = 1;
		HTable table = new HTable(conf, tablename);
		Get get = new Get(Bytes.toBytes(row));
		Scan scan = new Scan(get);
		
		scan.setTimeStamp(timestamp);
		for (String column : columns) {
			scan.addColumn(Bytes.toBytes(columnfamily), Bytes.toBytes(column));			
		}
		List<List<byte[]>> allResultsList = new ArrayList<List<byte[]>>();		
		ResultScanner resultScanner = table.getScanner(scan);
		Result result = resultScanner.next();
		while (result != null) {
			List<byte[]> resultList = new ArrayList<byte[]>();
			for (String column : columns) {
				resultList.add(result.getValue(Bytes.toBytes(columnfamily), Bytes.toBytes(column)));				
			}
			allResultsList.add(resultList);
			timestamp++;
			scan.setTimeStamp(timestamp);
			resultScanner = table.getScanner(scan);
			result = resultScanner.next();
		}
		return allResultsList;
	}
	
	/******************************************************************
	 * Other
	 ******************************************************************/

	public void deleteTable(String tablename) throws IOException {
		hba.disableTable(Bytes.toBytes(tablename));
		hba.deleteTable(Bytes.toBytes(tablename));
	}

	public boolean exists(String tablename) throws IOException {
		return hba.tableExists(Bytes.toBytes(tablename));
	}

	public void close() throws IOException {
		this.hba.close();
	}

	@SuppressWarnings("resource")
	public long getNextTimestamp(String tablename, int row,
			String columnfamily, String column) throws IOException {
		HTable table = new HTable(conf, tablename);
		Get get = new Get(Bytes.toBytes(row));
		Result result = table.get(get);

		return (result.value() == null) ? 1 : result.getColumnLatestCell(
				Bytes.toBytes(columnfamily), Bytes.toBytes(column))
				.getTimestamp();
	}

	
}
