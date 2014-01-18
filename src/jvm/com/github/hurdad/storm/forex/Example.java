package com.github.hurdad.storm.forex;

import com.github.hurdad.storm.forex.bolt.*;
import com.github.hurdad.storm.forex.spout.*;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class Example {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("truefx", new TrueFXSpout(50), 2);
		builder.setBolt("ohlc_10", new OHLCBolt(10), 1)
				.fieldsGrouping("truefx", new Fields("pair"));
		builder.setBolt("sma_5", new SMABolt(5), 2).fieldsGrouping("ohlc_10", new Fields("pair"));
		builder.setBolt("ema_5", new EMABolt(5), 2).fieldsGrouping("ohlc_10", new Fields("pair"));
		builder.setBolt("rsi_5", new RSIBolt(5), 2).fieldsGrouping("ohlc_10", new Fields("pair"));

		builder.setBolt("summary", new SummaryBolt(), 1)
				.fieldsGrouping("sma_5", new Fields("pair", "timeslice"))
				.fieldsGrouping("ema_5", new Fields("pair", "timeslice"))
				.fieldsGrouping("rsi_5", new Fields("pair", "timeslice"));

		Config conf = new Config();
		// conf.setDebug(true);

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
