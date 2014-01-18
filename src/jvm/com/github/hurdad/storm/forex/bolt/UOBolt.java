package com.github.hurdad.storm.forex.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class UOBolt extends BaseRichBolt {

	OutputCollector _collector;
	Integer _period1, _period2, _period3;
	
	public UOBolt(Integer period1, Integer period2, Integer period3) {
		_period1 = period1;
		_period2 = period2;
		_period3 = period3;
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
		declarer.declare(new Fields("pair", "timeslice", "uo"));
	}

}