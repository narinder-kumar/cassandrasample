package com.intelligrape.bigdata.cassandrasample;

import java.util.Set;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

public class SimpleClient {

	private Cluster cluster;

	private Logger logger = Logger.getLogger(SimpleClient.class);

	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		logger.debug("Connected to cluster: "+metadata.getClusterName());
		Set<Host> allHosts = metadata.getAllHosts();
		for (Host host : allHosts) {
			logger.debug("DataCenter : "+host.getDatacenter()+", Rack : "+host.getRack()+", Node of Cluster: "+host.getAddress());
		}
	}
	
	public void close() {
		cluster.close();
		logger.debug("closed connection with cluster");
	}

	public static void main(String[] args) {
		SimpleClient client = new SimpleClient();
		client.connect("127.0.0.1");
		client.close();
	}

}
