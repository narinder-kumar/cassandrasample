package com.intelligrape.bigdata.cassandrasample;

import java.util.Set;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class LoadDataClient {

	private Cluster cluster;

	private Session session;
	
	private Logger logger = Logger.getLogger(LoadDataClient.class);

	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		logger.debug("Connected to cluster: "+metadata.getClusterName());
		Set<Host> allHosts = metadata.getAllHosts();
		for (Host host : allHosts) {
			logger.debug("DataCenter : "+host.getDatacenter()+", Rack : "+host.getRack()+", Node of Cluster: "+host.getAddress());
		}
		session = cluster.connect();
	}
	
	public void createSchema() {
		logger.debug("Creating Schema for initial data load");
		session.execute("CREATE KEYSPACE meetup_db WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };");
		session.execute("create table meetup_db.departments (dep_id int, dep_name varchar, dep_head varchar, dep_head_email varchar, primary key (dep_id));");
		session.execute("create table meetup_db.employees (emp_id int, dep_id int, name varchar, email varchar, gender varchar, primary key (emp_id, dep_id))");		
	}

	public void insertData() {
		logger.debug("Populating tables with sample data-set");
		
		session.execute("insert into meetup_db.departments (dep_id, dep_name, dep_head, dep_head_email) values (1, 'hr', 'charu', 'charu@ig.com');");
		session.execute("insert into meetup_db.departments (dep_id, dep_name, dep_head, dep_head_email) values (2, 'admin', 'deepak handuja', 'deepak@ig.com');");
		
		
		session.execute("insert into meetup_db.employees (emp_id, dep_id, name, email, gender) values (1, 1, 'Charu', 'charu@ig.com', 'F');");
		session.execute("insert into meetup_db.employees (emp_id, dep_id, name, email, gender) values (2, 2, 'Deepak', 'deepak@ig.com', 'm');");
	}

	public void queryTables() {
		logger.debug("Querying tables...");
		ResultSet departmentsResultSet = session.execute("select * from meetup_db.departments;");
		logger.debug("Result set for departments tables");
		for (Row row : departmentsResultSet) {
			logger.debug("dep_id :" +row.getInt("dep_id"));
			logger.debug("dep_name :" +row.getString("dep_name"));
			logger.debug("dep_head :" +row.getString("dep_head"));
		}

		ResultSet employeesResultSet = session.execute("select * from meetup_db.employees where emp_id = 2;");
		logger.debug("Result set for employees tables");
		for (Row row : employeesResultSet) {
			logger.debug("emp_id :" +row.getInt("emp_id"));
			logger.debug("dep_id :" +row.getInt("dep_id"));
			logger.debug("name :" +row.getString("name"));
			logger.debug("gender :" +row.getString("gender"));
		}

	}
	
	public static void main(String[] args) {
		LoadDataClient client = new LoadDataClient();
		client.connect("127.0.0.1");
		client.dropSchema();
		client.createSchema();
		client.insertData();
		client.queryTables();
		client.close();
	}

	private void close() {
		cluster.close();
		logger.debug("closed connection with cluster");
	}

	private void dropSchema() {
		logger.debug("Cleaning up. Removing schema");
		session.execute("drop keyspace meetup_db;");
	}
}
