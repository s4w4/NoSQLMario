package la.persistence.database.impl;

import java.io.IOException;


public class AppCreateHBase {
	public static void main(String[] args) {

		try {
			HBaseConnection hbase = new HBaseConnection();
			createTable(hbase, "try", "cf");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void createTable(HBaseConnection hbase, String tablename,
			String columnfamily) throws IOException {
		hbase.createTable(tablename, columnfamily);
	}
}
