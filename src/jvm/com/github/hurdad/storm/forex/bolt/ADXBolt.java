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

public class ADXBolt extends BaseRichBolt {
	OutputCollector _collector;
	Integer _period;
	Map<String, Double> _prev_closes;
	Map<String, Double> _prev_highs;
	Map<String, Double> _prev_lows;
	Map<String, Queue<Double>> _true_ranges;
	Map<String, Queue<Double>> _plus_dms;
	Map<String, Queue<Double>> _minus_dms;
	Map<String, Queue<Double>> _dxs;
	Map<String, Double> _prev_adxs;

	public ADXBolt(Integer period) {
		_period = period;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_true_ranges = new HashMap<String, Queue<Double>>();
		_plus_dms = new HashMap<String, Queue<Double>>();
		_minus_dms = new HashMap<String, Queue<Double>>();
		_dxs = new HashMap<String, Queue<Double>>();
	}

	@Override
	public void execute(Tuple tuple) {

		// input vars
		String pair = tuple.getStringByField("pair");
		Double high = tuple.getDoubleByField("high");
		Double low = tuple.getDoubleByField("low");
		Integer timeslice = tuple.getIntegerByField("timeslice");

		// init
		if (_true_ranges.get(pair) == null)
			_true_ranges.put(pair, new LinkedList<Double>());

		if (_plus_dms.get(pair) == null)
			_plus_dms.put(pair, new LinkedList<Double>());

		if (_minus_dms.get(pair) == null)
			_minus_dms.put(pair, new LinkedList<Double>());

		if (_dxs.get(pair) == null)
			_dxs.put(pair, new LinkedList<Double>());

		// queues
		Queue<Double> true_ranges = _true_ranges.get(pair);
		Queue<Double> plus_dms = _plus_dms.get(pair);
		Queue<Double> minus_dms = _minus_dms.get(pair);
		Queue<Double> dxs = _dxs.get(pair);

		// need 2 data points
		if (_prev_closes.get(pair) != null && _prev_lows.get(pair) != null
				&& _prev_closes.get(pair) != null) {

			Double prev_high = _prev_highs.get(pair);
			Double prev_low = _prev_lows.get(pair);
			Double prev_close = _prev_closes.get(pair);

			// calc true range
			Double tr = Math.max(high - low,
					Math.max(Math.abs(high - prev_close), Math.abs(low - prev_close)));

			// calc +DM 1
			Double plus_dm_1 = ((high - prev_high) > (prev_low - low)) ? Math.max(high - prev_high,
					0) : 0;

			// calc -DM 1
			Double minus_dm_1 = ((prev_low - low) > (high - prev_high)) ? Math.max(prev_low - low,
					0) : 0;

			// add to front
			true_ranges.add(tr);
			plus_dms.add(plus_dm_1);
			minus_dms.add(minus_dm_1);

			// pop back if too long
			if (true_ranges.size() > _period)
				true_ranges.poll();
			if (plus_dms.size() > _period)
				plus_dms.poll();
			if (minus_dms.size() > _period)
				minus_dms.poll();

		}

		// calc dx
		Double dx1 = null;
		if (true_ranges.size() == _period) {

			// tr sum
			Double sum_true_range = 0d;
			for (Double val : true_ranges) {
				sum_true_range = sum_true_range + val;
			}

			// plus dm sum
			Double sum_plus_dm = 0d;
			for (Double val : plus_dms) {
				sum_plus_dm = sum_true_range + val;
			}

			// minux dm sum
			Double sum_minus_dm = 0d;
			for (Double val : minus_dms) {
				sum_minus_dm = sum_true_range + val;
			}

			// calc dx
			Double plus_di = (sum_plus_dm / sum_true_range) * 100;
			Double minus_di = (sum_minus_dm / sum_true_range) * 100;
			Double di_diff = Math.abs(plus_di - minus_di);
			Double di_sum = plus_di + minus_di;
			dx1 = (di_diff / di_sum) * 100;

			// add to front
			dxs.add(dx1);

			// pop back if too long
			if (dxs.size() > _period)
				dxs.poll();

		}

		// calc first adx
		if (dxs.size() == _period) {

			// dx sum
			Double sum = 0d;
			for (Double val : dxs) {
				sum = sum + val;
			}
			Double adx = sum / _period;

			if (pair.equals("EUR/USD"))
				System.out.println(timeslice + " adx:" + adx);

			// emit
			_collector.emit(new Values(pair, timeslice, adx));

			// save
			_prev_adxs.put(pair, adx);

		}

		// calc further adx
		if (_prev_adxs.get(pair) != null) {

			Double adx = ((_prev_adxs.get(pair) * (_period - 1)) + dx1) / _period;

			if (pair.equals("EUR/USD"))
				System.out.println(timeslice + " adx:" + adx);

			// emit
			_collector.emit(new Values(pair, timeslice, adx));

			// save
			_prev_adxs.put(pair, adx);
		}

		// save
		_true_ranges.put(pair, true_ranges);
		_minus_dms.put(pair, minus_dms);
		_dxs.put(pair, dxs);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "adx"));
	}

}