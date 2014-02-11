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

public class CCIBolt extends BaseRichBolt {
	OutputCollector _collector;
	Integer _period;
	Map<String, Queue<Double>> _tp_queues;

	public CCIBolt(Integer period) {
		_period = period;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_tp_queues = new HashMap<String, Queue<Double>>();
	}

	@Override
	public void execute(Tuple tuple) {

		// input vars
		String pair = tuple.getStringByField("pair");
		String high = tuple.getStringByField("high");
		String low = tuple.getStringByField("low");
		String close = tuple.getStringByField("close");
		Integer timeslice = tuple.getIntegerByField("timeslice");

		// init
		if (_tp_queues.get(pair) == null)
			_tp_queues.put(pair, new LinkedList<Double>());

		// pair tp q
		Queue<Double> q = _tp_queues.get(pair);

		// typical price calc
		Double tp = (Double.parseDouble(high) + Double.parseDouble(low) + Double.parseDouble(close)) / 3;

		// add to front
		q.add(tp);

		// pop back if too long
		if (q.size() > _period)
			q.poll();

		// have enough data to calc cci
		if (q.size() == _period) {

			// TP moving average
			Double sum = 0d;
			for (Double val : q) {
				sum += val;
			}
			Double tp_sma = sum / _period;

			// calc mean dev
			Double sum_abs = 0d;
			for (Double val : q) {
				sum_abs += Math.abs(tp_sma - val);
			}
			Double mean_dev = sum_abs / _period;

			// calc cci
			Double cci = (tp - tp_sma) / (0.015 * mean_dev);

			// emit
			_collector.emit(new Values(pair, timeslice, String.format("%.2f", cci)));

		}

		// save
		_tp_queues.put(pair, q);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "cci"));
	}

}