package com.github.hurdad.storm.forex.bolt;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class SummaryBolt extends BaseRichBolt {
	OutputCollector _collector;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		//List<Object> id = tuple.select(_idFields);
		
		String pair = tuple.getStringByField("pair");
		Integer timeslice = tuple.getIntegerByField("timeslice");
		
		
		Double val = tuple.getDouble(3);
		List<String> f = tuple.getFields().toList();
		
	
		
		if(pair.equals("EUR/USD"))
			System.out.println(pair + " " + timeslice + " " + val + " " + f.get(1));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("test"));
	}

}