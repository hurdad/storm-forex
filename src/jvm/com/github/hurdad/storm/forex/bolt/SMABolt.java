package com.github.hurdad.storm.forex.bolt;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;



public class SMABolt extends BaseRichBolt {
	OutputCollector _collector;
	Integer _period;
	Map<String, Queue<Double>> _queues;

	public SMABolt(Integer period){
		_period = period;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_queues = new HashMap<String, Queue<Double>>();
	}
	

	@Override
	public void execute(Tuple tuple) {
		String pair = tuple.getStringByField("pair");
		Double close = tuple.getDoubleByField("close");
		Integer timeslice = tuple.getIntegerByField("timeslice");
		
		if(_queues.get(pair) == null)
			_queues.put(pair, new LinkedList<Double>());
		
		Queue<Double> q = _queues.get(pair);
		q.add(close);
		
		if(q.size() >= _period){
		
			//sum
			Double sum = 0d;
			//access via new for-loop
			for(Double val : q) {
			    sum = sum + val;
			}
			Double sma = sum / _period;
			System.out.println("pair " + pair);
			System.out.println("sma " + sma);
			System.out.println("ts " + timeslice);
		;
			_collector.emit(new Values(pair, sma, timeslice));
			
			q.poll();
		}
		_queues.put(pair, q);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "sma", "timeslice"));
	}

}