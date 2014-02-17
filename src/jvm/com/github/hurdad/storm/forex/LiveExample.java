package com.github.hurdad.storm.forex;

import java.util.Map;

import com.github.hurdad.storm.forex.bolt.*;
import com.github.hurdad.storm.forex.spout.*;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class LiveExample {

	public static class BidOrOfferBolt extends BaseRichBolt {
		OutputCollector _collector;
		int _type;

		public class Type {
			public static final int BID = 1;
			public static final int OFFER = 5;

		}

		public BidOrOfferBolt(String type) {
			if (type.equals("BID"))
				_type = Type.BID;

			if (type.equals("OFFER"))
				_type = Type.OFFER;

		}

		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void execute(Tuple tuple) {
			if (_type == Type.BID)
				_collector.emit(tuple, new Values(tuple.getStringByField("pair"), tuple.getLongByField("timestamp"), tuple.getDoubleByField("bid")));

			if (_type == Type.OFFER)
				_collector.emit(tuple, new Values(tuple.getStringByField("pair"), tuple.getLongByField("timestamp"), tuple.getDoubleByField("offer")));

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("pair", "timestamp", "price"));
		}

	}

	public static void main(String[] args) throws Exception {
		
		TopologyBuilder builder = new TopologyBuilder();

		// truefx.com live tick spout
		builder.setSpout("truefx", new TrueFXTickSpout(50), 2);
		
		//forward bid or offer price
		builder.setBolt("price", new BidOrOfferBolt("BID")).shuffleGrouping("truefx");

		// 10 second candles
		builder.setBolt("ohlc_10", new OHLCBolt(10), 1)
				.fieldsGrouping("price", new Fields("pair"));

		// moving averages
		builder.setBolt("sma_5", new SMABolt(5, 5), 2)
				.fieldsGrouping("ohlc_10", new Fields("pair"));
		builder.setBolt("ema_5", new EMABolt(5, 5), 2)
				.fieldsGrouping("ohlc_10", new Fields("pair"));

		// technical analysis
		builder.setBolt("adx_14", new ADXBolt(14), 2).fieldsGrouping("ohlc_10", new Fields("pair"));
		builder.setBolt("atr_14", new ATRBolt(14), 2).fieldsGrouping("ohlc_10", new Fields("pair"));
		builder.setBolt("bbp_13", new BullBearPowerBolt(13), 2).fieldsGrouping("ohlc_10",
				new Fields("pair"));
		builder.setBolt("cci_14", new CCIBolt(14), 2).fieldsGrouping("ohlc_10", new Fields("pair"));
		builder.setBolt("hl_14", new HighsLowsBolt(14), 2).fieldsGrouping("ohlc_10",
				new Fields("pair"));
		builder.setBolt("macd_12_26_9", new MACDBolt(12, 26, 9), 2).fieldsGrouping("ohlc_10",
				new Fields("pair"));
		builder.setBolt("r_14", new PercRBolt(14), 2).fieldsGrouping("ohlc_10", new Fields("pair"));
		builder.setBolt("roc_12", new ROCBolt(12), 2).fieldsGrouping("ohlc_10", new Fields("pair"));
		builder.setBolt("rsi_14", new RSIBolt(14), 2).fieldsGrouping("ohlc_10", new Fields("pair"));
		builder.setBolt("stochrsi_14", new StochRSIBolt(14), 2).fieldsGrouping("ohlc_10",
				new Fields("pair"));
		builder.setBolt("stoch_14_3", new StochBolt(14, 3), 2).fieldsGrouping("ohlc_10",
				new Fields("pair"));
		builder.setBolt("uo_7_14_28", new UOBolt(7, 14, 28), 2).fieldsGrouping("ohlc_10",
				new Fields("pair"));

		Config conf = new Config();
		conf.setDebug(true);

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
