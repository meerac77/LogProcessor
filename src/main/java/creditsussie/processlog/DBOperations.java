package creditsussie.processlog;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOperations {

	private static final Logger logger = LoggerFactory.getLogger(DBOperations.class);

	Connection con = null;
	Statement stmt = null;

	public DBOperations() {
		createConnection();
	}

	public boolean writeToDB(List<ProcessedLogObject> procList) {
		createTable();
		for (ProcessedLogObject procObj : procList) {
			insertTable(procObj);
		}
		logger.info("Inserted " + procList.size() + " rows!");
		closeConnection();
		return true;
	}

	private void createConnection() {
		try {
			if (null == con) {
				Class.forName("org.hsqldb.jdbc.JDBCDriver");
				con = DriverManager.getConnection("jdbc:hsqldb:hsql://localhost/testdb", "SA", "");
				if (con != null) {
					logger.info("Connection created successfully!");

				}
			}
		} catch (Exception e) {
			logger.error("Exception while creating connection: " + e.getStackTrace());
		}
	}

	private void createTable() {
		try {
			stmt = con.createStatement();
			stmt.executeUpdate(
					"CREATE TABLE IF NOT EXISTS log_table (id VARCHAR(20) NOT NULL, type VARCHAR(20), host VARCHAR(20), duration INT, alert BOOLEAN, PRIMARY KEY (id));");
			logger.info("Table created successfully!");
		} catch (SQLException e) {
			logger.error("SQLException when creating table: " + e.getStackTrace());
		}
	}

	private void insertTable(ProcessedLogObject processedLogObject) {
		try {
			stmt = con.createStatement();
			stmt.executeUpdate("INSERT INTO log_table VALUES ('" + processedLogObject.getId() + "','"
					+ processedLogObject.getType() + "','" + processedLogObject.getHost() + "',"
					+ processedLogObject.getDuration() + "," + processedLogObject.isAlert() + ")");
			con.commit();
		} catch (SQLException e) {
			logger.error("SQLException when inserting to table: " + e.getStackTrace());
		}
	}

	private void closeConnection() {
		try {
			if (null != con) {
				con.close();
			}
		} catch (SQLException e) {
			logger.error("SQLException while closing connection: " + e.getStackTrace());
		}
	}

}
