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

		// five minute candles (300 sec)
		builder.setBolt("ohlc_300", new OHLCBolt(5 * 60), 1).fieldsGrouping("jdbc",
				new Fields("pair"));

		// moving averages
		builder.setBolt("sma_5", new SMABolt(5), 2).fieldsGrouping("ohlc_300", new Fields("pair"));
		builder.setBolt("ema_5", new EMABolt(5), 2).fieldsGrouping("ohlc_300", new Fields("pair"));

		// technical analysis
		builder.setBolt("adx_14", new ADXBolt(14), 2)
				.fieldsGrouping("ohlc_300", new Fields("pair"));
		builder.setBolt("atr_14", new ATRBolt(14), 2)
				.fieldsGrouping("ohlc_300", new Fields("pair"));
		builder.setBolt("bbp_13", new BullBearPowerBolt(13), 2).fieldsGrouping("ohlc_300",
				new Fields("pair"));
		builder.setBolt("cci_14", new CCIBolt(14), 2)
				.fieldsGrouping("ohlc_300", new Fields("pair"));
		builder.setBolt("hl_14", new HighsLowsBolt(14), 2).fieldsGrouping("ohlc_300",
				new Fields("pair"));
		builder.setBolt("macd_12_26_9", new MACDBolt(12, 26, 9), 2).fieldsGrouping("ohlc_300",
				new Fields("pair"));
		builder.setBolt("r_14", new PercRBolt(14), 2)
				.fieldsGrouping("ohlc_300", new Fields("pair"));
		builder.setBolt("roc_12", new ROCBolt(12), 2)
				.fieldsGrouping("ohlc_300", new Fields("pair"));
		builder.setBolt("rsi_14", new RSIBolt(14), 2)
				.fieldsGrouping("ohlc_300", new Fields("pair"));
		builder.setBolt("stoch_14_3", new StochBolt(14, 3), 2).fieldsGrouping("ohlc_300",
				new Fields("pair"));
		builder.setBolt("stochrsi_14", new StochRSIBolt(14), 2).fieldsGrouping("ohlc_300",
				new Fields("pair"));
		builder.setBolt("uo_7)14_28", new UOBolt(7, 14, 28), 2).fieldsGrouping("ohlc_300",
				new Fields("pair"));

		Config conf = new Config();
		// conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("forex", conf, builder.createTopology());
			Utils.sleep(5 * 60 * 1000); // Run for 5 minutes
			cluster.killTopology("forex");
			cluster.shutdown();
		}
	}
}
