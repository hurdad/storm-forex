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
	Map<String, Queue<Double>> _close_queues;

	public SMABolt(Integer period) {
		_period = period;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_close_queues = new HashMap<String, Queue<Double>>();
	}

	@Override
	public void execute(Tuple tuple) {

		// input vars
		String pair = tuple.getStringByField("pair");
		Double close = tuple.getDoubleByField("close");
		Integer timeslice = tuple.getIntegerByField("timeslice");

		// init
		if (_close_queues.get(pair) == null)
			_close_queues.put(pair, new LinkedList<Double>());

		// get queue for pair
		Queue<Double> q = _close_queues.get(pair);

		// push close price onto queue
		q.add(close);
		
		//pop back if too long
		if(q.size() > _period)
			q.poll();

		// check if we have enough data to calc sma
		if (q.size() >= _period) {

			// calc sma
			Double sum = 0d;
			for (Double val : q) {
				sum = sum + val;
			}
			Double sma = sum / _period;
			sma = Math.round(sma * 100000) / 100000.0d;

			if (pair.equals("EUR/USD"))
				System.out.println(pair + " sma:" + sma + " " + timeslice);

			// emit
			_collector.emit(new Values(pair, timeslice, sma));

		}

		// save
		_close_queues.put(pair, q);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "sma"));
	}

}