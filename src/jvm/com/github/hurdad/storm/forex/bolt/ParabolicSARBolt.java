package com.github.hurdad.storm.forex.bolt;

import java.math.BigDecimal;
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

public class ParabolicSARBolt extends BaseRichBolt {
	OutputCollector _collector;
	BigDecimal _acc_factor;
	BigDecimal _max_step;
	Map<String, Double> _prev_sar;
	Map<String, Double> _prev_ep;
	Map<String, Double> _prev_af;
	Map<String, Double> _prev_af_diff;
	Map<String, Integer> _prev_direction;
	Map<String, Integer> _prev_prev_direction;
	Map<String, Queue<Double>> _high_queues;
	Map<String, Queue<Double>> _low_queues;

	public ParabolicSARBolt(BigDecimal acc_factor, BigDecimal max_step) {
		_acc_factor = acc_factor;
		_max_step = max_step;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_prev_sar = new HashMap<String, Double>();
		_prev_ep = new HashMap<String, Double>();
		_prev_af = new HashMap<String, Double>();
		_prev_af_diff = new HashMap<String, Double>();
		_prev_direction = new HashMap<String, Integer>();
		_prev_prev_direction = new HashMap<String, Integer>();
		_high_queues = new HashMap<String, Queue<Double>>();
		_low_queues = new HashMap<String, Queue<Double>>();
	}

	@Override
	public void execute(Tuple tuple) {

		// input vars
		String pair = tuple.getStringByField("pair");
		String high = tuple.getStringByField("high");
		String low = tuple.getStringByField("low");
		Integer timeslice = tuple.getIntegerByField("timeslice");

		// init
		if (_high_queues.get(pair) == null)
			_high_queues.put(pair, new LinkedList<Double>());
		if (_low_queues.get(pair) == null)
			_low_queues.put(pair, new LinkedList<Double>());
		if (_prev_direction.get(pair) == null)
			_prev_direction.put(pair, 1);
		if (_prev_ep.get(pair) == null)
			_prev_ep.put(pair, 0d);
		if (_prev_af.get(pair) == null)
			_prev_af.put(pair, 0d);

		// queues
		Queue<Double> highs = _high_queues.get(pair);
		Queue<Double> lows = _low_queues.get(pair);

		// add to front
		highs.add(Double.parseDouble(high));
		lows.add(Double.parseDouble(low));

		// pop back if too long
		if (highs.size() > 5)
			highs.poll();

		if (lows.size() > 5)
			lows.poll();

		// check if we have enough data for sar
		if (highs.size() == 5 && lows.size() == 5) {

			Integer direction = null;
			Double sar = null;
			Double ep = null;
			Double af = null;
			Double af_diff = null;

			// first sar
			if (_prev_sar.get(pair) == null) {

				// get low
				Double l = lows.peek();
				// loop lows
				for (Double val : lows) {
					l = Math.min(val, l);
				}
				sar = l;

				// get high
				Double h = highs.peek();
				// loop highs
				for (Double val : highs) {
					h = Math.max(val, h);
				}
				ep = h;

			} else {

				// rising
				if (_prev_direction.get(pair) == 1) {
					ep = (Double.parseDouble(high) > _prev_ep.get(pair)) ? Double.parseDouble(high)
							: _prev_ep.get(pair);
				}

				// falling
				if (_prev_direction.get(pair) == -1) {
					ep = (Double.parseDouble(low) < _prev_ep.get(pair)) ? Double.parseDouble(low)
							: _prev_ep.get(pair);
				}

				// min_lows
				Double min_lows = null;
				Integer counter = 0;
				for (Double val : lows) {
					if (counter == 2)
						min_lows = val;
					if (counter == 3)
						min_lows = Math.min(min_lows, val);

					counter++;
				}

				// max_highs
				Double max_highs = null;
				counter = 0;
				for (Double val : highs) {
					if (counter == 2)
						max_highs = val;
					if (counter == 3)
						max_highs = Math.max(max_highs, val);

					counter++;
				}

				if (_prev_direction.get(pair) == _prev_prev_direction.get(pair)) {
					if (_prev_direction.get(pair) == 1) {
						sar = ((_prev_sar.get(pair) + _prev_af_diff.get(pair)) < min_lows) ? _prev_sar
								.get(pair) + _prev_af_diff.get(pair)
								: min_lows;

					} else {
						sar = ((_prev_sar.get(pair) + _prev_af_diff.get(pair)) > max_highs) ? _prev_sar
								.get(pair) + _prev_af_diff.get(pair)
								: max_highs;
					}
				} else {
					sar = _prev_ep.get(pair);
				}
			}

			// calc direction
			if (_prev_direction.get(pair) == 1) {
				direction = (Double.parseDouble(low) > sar) ? 1 : -1;
			} else {
				direction = (Double.parseDouble(high) < sar) ? -1 : 1;
			}

			// Delta
			Double delta = ep - sar;

			// acc fact
			if (direction == _prev_direction.get(pair)) {
				if (direction == 1) {
					if (ep > _prev_ep.get(pair)) {
						af = (_max_step.compareTo(new BigDecimal(String.format("%.2f",
								_prev_af.get(pair)))) == 0) ? _prev_af.get(pair) : _acc_factor
								.doubleValue() + _prev_af.get(pair);
					} else {
						af = _prev_af.get(pair);
					}

				} else {
					if (ep < _prev_ep.get(pair)) {
						af = (_max_step.compareTo(new BigDecimal(String.format("%.2f",
								_prev_af.get(pair)))) == 0) ? _prev_af.get(pair) : _acc_factor
								.doubleValue() + _prev_af.get(pair);
					} else {
						af = _prev_af.get(pair);
					}
				}
			} else {
				af = _acc_factor.doubleValue();
			}

			// af_diff
			af_diff = af * delta;

			// emit
			_collector.emit(new Values(pair, timeslice, String.format("%.5f", sar)));

			// save
			_prev_sar.put(pair, sar);
			_prev_ep.put(pair, ep);
			_prev_af.put(pair, af);
			_prev_af_diff.put(pair, af_diff);
			_prev_prev_direction.put(pair, _prev_direction.get(pair));
			_prev_direction.put(pair, direction);
		}

		// save
		_high_queues.put(pair, highs);
		_low_queues.put(pair, lows);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "sar"));
	}
}
