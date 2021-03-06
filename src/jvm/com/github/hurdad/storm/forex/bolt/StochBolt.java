package com.github.hurdad.storm.forex.bolt;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StochBolt extends BaseRichBolt {
	OutputCollector _collector;
	Integer _period;
	Integer _sma_period;
	Map<String, Queue<Double>> _high_queues;
	Map<String, Queue<Double>> _low_queues;
	Map<String, Queue<Double>> _k_queues;

	public StochBolt(Integer period, Integer sma_period) {
		_period = period;
		_sma_period = sma_period;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_high_queues = new HashMap<String, Queue<Double>>();
		_low_queues = new HashMap<String, Queue<Double>>();
		_k_queues = new HashMap<String, Queue<Double>>();
	}

	@Override
	public void execute(Tuple tuple) {

		// input vars
		String pair = tuple.getStringByField("pair");
		String high = tuple.getStringByField("high");
		String low = tuple.getStringByField("low");
		String close = tuple.getStringByField("close");
		Integer timeslice = tuple.getIntegerByField("timeslice");

		Double k = null, sma = null;

		// init
		if (_high_queues.get(pair) == null)
			_high_queues.put(pair, new LinkedList<Double>());

		if (_low_queues.get(pair) == null)
			_low_queues.put(pair, new LinkedList<Double>());

		if (_k_queues.get(pair) == null)
			_k_queues.put(pair, new LinkedList<Double>());

		// pair highs / lows
		Queue<Double> highs = _high_queues.get(pair);
		Queue<Double> lows = _low_queues.get(pair);
		Queue<Double> ks = _k_queues.get(pair);

		// add to front
		highs.add(Double.parseDouble(high));
		lows.add(Double.parseDouble(low));

		// pop back if too long
		if (highs.size() > _period)
			highs.poll();

		if (lows.size() > _period)
			lows.poll();

		// have enough data to calc stoch
		if (highs.size() == _period) {

			// get high
			Double h = highs.peek();
			// loop highs
			for (Double val : highs) {
				h = Math.max(val, h);
			}

			// get low
			Double l = lows.peek();
			// loop lows
			for (Double val : lows) {
				l = Math.min(val, l);
			}

			// calc
			k = (Double.parseDouble(close) - l) / (h - l) * 100;

			// add to front
			ks.add(k);

			// pop back if too long
			if (ks.size() > _sma_period)
				ks.poll();

		}

		// have enough data to calc sma
		if (ks.size() == _sma_period) {

			// k moving average
			Double sum = 0d;
			for (Double val : ks) {
				sum = sum + val;
			}
			sma = sum / _sma_period;

		}

		// have enough data to emit
		if (k != null && sma != null) {

			// emit
			_collector.emit(new Values(pair, timeslice, String.format("%.2f", k), String.format(
					"%.2f", sma)));
		}

		// save
		_high_queues.put(pair, highs);
		_low_queues.put(pair, lows);
		_k_queues.put(pair, ks);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "k", "d"));
	}

}