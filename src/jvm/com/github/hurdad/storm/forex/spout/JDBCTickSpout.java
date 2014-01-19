package com.github.hurdad.storm.forex.spout;

import java.sql.*;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class JDBCTickSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	String _jdbc_driver;
	String _jdbc_url;
	String _username;
	String _password;

	public JDBCTickSpout(String jdbc_driver, String jdbc_url, String username, String password) {
		_jdbc_driver = jdbc_driver;
		_jdbc_url = jdbc_url;
		_username = username;
		_password = password;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void nextTuple() {

		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName(_jdbc_driver);
			conn = DriverManager.getConnection(_jdbc_url, _username, _password);
			stmt = conn.createStatement();
			String sql = "SELECT pair, bid, offer, ROUND(unix_timestamp(ts) * 1000) as timestamp FROM quotes WHERE pair = 'EUR/USD' ORDER BY ts ASC LIMIT 0, 800000 ";
			ResultSet rs = stmt.executeQuery(sql);

			// loop
			while (rs.next()) {

				String pair = rs.getString("pair");
				Double bid = rs.getDouble("bid");
				Double offer = rs.getDouble("offer");
				Long timestamp = rs.getLong("timestamp");

				// emit
				_collector.emit(new Values(pair, bid, offer, timestamp));

				//Utils.sleep(5);// wait
			}
			System.out.print("Query Finished");

			rs.close();
			stmt.close();
			conn.close();
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}// nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}

		while (true)
			Utils.sleep(1000);// wait
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "bid", "offer", "timestamp"));
	}

}
