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

public class ATRBolt extends BaseRichBolt {
	OutputCollector _collector;
	Integer _period;
	Map<String, Double> _prev_closes;
	Map<String, Double> _prev_atrs;
	Map<String, Queue<Double>> _true_ranges;
	Integer _counter;

	public ATRBolt(Integer period) {
		_period = period;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_prev_closes = new HashMap<String, Double>();
		_prev_atrs = new HashMap<String, Double>();
		_true_ranges = new HashMap<String, Queue<Double>>();
		_counter = 0;
	}

	@Override
	public void execute(Tuple tuple) {

		// input vars
		String pair = tuple.getStringByField("pair");
		Double high = tuple.getDoubleByField("high");
		Double low = tuple.getDoubleByField("low");
		Double close = tuple.getDoubleByField("close");
		Integer timeslice = tuple.getIntegerByField("timeslice");

		// init
		if (_true_ranges.get(pair) == null)
			_true_ranges.put(pair, new LinkedList<Double>());

		Queue<Double> true_ranges = _true_ranges.get(pair);

		Double tr = null;
		Double high_minus_low = high - low;

		if (_prev_closes.get(pair) != null) {

			Double high_minus_close_past = Math.abs(high - _prev_closes.get(pair));
			Double low_minus_close_past = Math.abs(low - _prev_closes.get(pair));

			// calc tr
			tr = Math.max(high_minus_low, Math.max(high_minus_close_past, low_minus_close_past));

			// add to front
			true_ranges.add(tr);

			// pop back if too long
			if (true_ranges.size() > _period)
				true_ranges.poll();
		}

		// first ATR
		if (_prev_atrs.get(pair) == null && true_ranges.size() == _period) {

			// tr sum
			Double sum_true_range = 0d;
			for (Double val : true_ranges) {
				sum_true_range = sum_true_range + val;
			}

			// calc first
			Double atr = sum_true_range / _period;
			atr = Math.round(atr * 100) / 100.0d;

			if (pair.equals("EUR/USD"))
				System.out.println(timeslice + " atr:" + atr);

			// emit
			_collector.emit(new Values(pair, timeslice, atr));

			// save
			_prev_atrs.put(pair, atr);
		}

		// remaining ATR
		if (_prev_atrs.get(pair) != null) {

			// calc atr
			Double atr = ((_prev_atrs.get(pair) * (_period - 1)) + tr) / _period;
			atr = Math.round(atr * 100) / 100.0d;

			if (pair.equals("EUR/USD"))
				System.out.println(timeslice + " atr:" + atr);

			// emit
			_collector.emit(new Values(pair, timeslice, atr));

			// save
			_prev_atrs.put(pair, atr);
		}

		// save
		_true_ranges.put(pair, true_ranges);
		_prev_closes.put(pair, close);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "atr"));
	}

}