package com.github.hurdad.storm.forex;

import com.github.hurdad.storm.forex.bolt.*;
import com.github.hurdad.storm.forex.spout.*;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class LiveExample {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		// truefx.com live tick spout
		builder.setSpout("truefx", new TrueFXTickSpout(50), 2);

		// 10 second candles
		builder.setBolt("ohlc_10", new OHLCBolt(10), 1)
				.fieldsGrouping("truefx", new Fields("pair"));

		// moving averages
		builder.setBolt("sma_5", new SMABolt(5), 2).fieldsGrouping("ohlc_10", new Fields("pair"));
		builder.setBolt("ema_5", new EMABolt(5), 2).fieldsGrouping("ohlc_10", new Fields("pair"));

		// technical analysis
		builder.setBolt("rsi_5", new RSIBolt(5), 2).fieldsGrouping("ohlc_10", new Fields("pair"));
		builder.setBolt("atr_5", new ATRBolt(5), 2).fieldsGrouping("ohlc_10", new Fields("pair"));
		builder.setBolt("adx_5", new ADXBolt(5), 2).fieldsGrouping("ohlc_10", new Fields("pair"));
		builder.setBolt("bbp_5", new BullBearPowerBolt(5), 2).fieldsGrouping("ohlc_10",
				new Fields("pair"));
		builder.setBolt("cci_5", new CCIBolt(5), 2).fieldsGrouping("ohlc_10", new Fields("pair"));
		builder.setBolt("hl_5", new HighsLowsBolt(5), 2).fieldsGrouping("ohlc_10",
				new Fields("pair"));
		builder.setBolt("r_5", new PercRBolt(5), 2).fieldsGrouping("ohlc_10", new Fields("pair"));
		builder.setBolt("stoch_5", new STOCHBolt(5, 3), 2).fieldsGrouping("ohlc_10",
				new Fields("pair"));

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
