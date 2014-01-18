package com.github.hurdad.storm.forex;

import com.github.hurdad.storm.forex.bolt.*;
import com.github.hurdad.storm.forex.spout.*;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class BacktestExample {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		// only run one JDBC Spout or get duplicate tuples
		builder.setSpout("jdbc", new JDBCTickSpout("com.mysql.jdbc.Driver",
				"jdbc:mysql://127.0.0.1/forex", "forex", "forex"), 1);

		// one minute candles
		builder.setBolt("ohlc_60", new OHLCBolt(5 * 60), 1).fieldsGrouping("jdbc",
				new Fields("pair"));

		// moving averages
		builder.setBolt("sma_5", new SMABolt(5), 2).fieldsGrouping("ohlc_60", new Fields("pair"));
		builder.setBolt("ema_5", new EMABolt(5), 2).fieldsGrouping("ohlc_60", new Fields("pair"));

		// technical analysis
		builder.setBolt("rsi_14", new RSIBolt(14), 2).fieldsGrouping("ohlc_60", new Fields("pair"));
	//	builder.setBolt("atr_14", new ATRBolt(14), 2).fieldsGrouping("ohlc_60", new Fields("pair"));
		builder.setBolt("adx_14", new ADXBolt(14), 2).fieldsGrouping("ohlc_60", new Fields("pair"));
		builder.setBolt("bbp_13", new BullBearPowerBolt(13), 2).fieldsGrouping("ohlc_60",
				new Fields("pair"));
		builder.setBolt("cci_14", new CCIBolt(14), 2).fieldsGrouping("ohlc_60", new Fields("pair"));
		builder.setBolt("hl_14", new HighsLowsBolt(14), 2).fieldsGrouping("ohlc_60",
				new Fields("pair"));
		builder.setBolt("r_14", new PercRBolt(14), 2).fieldsGrouping("ohlc_60", new Fields("pair"));
		builder.setBolt("stoch_14_3", new STOCHBolt(14, 3), 2).fieldsGrouping("ohlc_60",
				new Fields("pair"));

	/*	builder.setBolt("summary", new SummaryBolt(), 1)
				.fieldsGrouping("sma_5", new Fields("pair", "timeslice"))
				.fieldsGrouping("ema_5", new Fields("pair", "timeslice"))
				.fieldsGrouping("rsi_14", new Fields("pair", "timeslice"));
*/
		Config conf = new Config();
		//conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("forex", conf, builder.createTopology());
			Utils.sleep(5 * 60 * 1000);
			cluster.killTopology("forex");
			cluster.shutdown();
		}
	}
}
