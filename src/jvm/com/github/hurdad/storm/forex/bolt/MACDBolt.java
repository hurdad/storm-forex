package com.github.hurdad.storm.forex.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class MACDBolt extends BaseRichBolt {
	OutputCollector _collector;
	Integer _ema1, _ema2, _signal;

	public MACDBolt(Integer ema1, Integer ema2, Integer signal) {
		_ema1 = ema1;
		_ema2 = ema2;
		_signal = signal;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {

		// input vars
		String pair = tuple.getStringByField("pair");
		Double high = tuple.getDoubleByField("high");
		Double low = tuple.getDoubleByField("low");
		Integer timeslice = tuple.getIntegerByField("timeslice");

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "macd"));
	}

}