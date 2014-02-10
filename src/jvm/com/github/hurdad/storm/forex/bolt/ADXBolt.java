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

public class ADXBolt extends BaseRichBolt {
	OutputCollector _collector;
	Integer _period;
	Map<String, BigDecimal> _prev_closes;
	Map<String, BigDecimal> _prev_highs;
	Map<String, BigDecimal> _prev_lows;
	Map<String, Queue<BigDecimal>> _true_ranges;
	Map<String, Queue<BigDecimal>> _plus_dms;
	Map<String, Queue<BigDecimal>> _minus_dms;
	Map<String, Queue<Double>> _dxs;
	Map<String, Double> _prev_true_range;
	Map<String, Double> _prev_plus_dm;
	Map<String, Double> _prev_minus_dm;
	Map<String, Double> _prev_adxs;

	public ADXBolt(Integer period) {
		_period = period;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_prev_closes = new HashMap<String, BigDecimal>();
		_prev_highs = new HashMap<String, BigDecimal>();
		_prev_lows = new HashMap<String, BigDecimal>();
		_true_ranges = new HashMap<String, Queue<BigDecimal>>();
		_plus_dms = new HashMap<String, Queue<BigDecimal>>();
		_minus_dms = new HashMap<String, Queue<BigDecimal>>();
		_dxs = new HashMap<String, Queue<Double>>();
		_prev_true_range = new HashMap<String, Double>();
		_prev_plus_dm = new HashMap<String, Double>();
		_prev_minus_dm = new HashMap<String, Double>();
		_prev_adxs = new HashMap<String, Double>();
	}

	@Override
	public void execute(Tuple tuple) {

		// input vars
		String pair = tuple.getStringByField("pair");
		String high = tuple.getStringByField("high");
		String low = tuple.getStringByField("low");
		String close = tuple.getStringByField("close");
		Integer timeslice = tuple.getIntegerByField("timeslice");

		// init big decimals
		BigDecimal high_bd = new BigDecimal(high);
		BigDecimal low_bd = new BigDecimal(low);
		BigDecimal close_bd = new BigDecimal(close);

		// init queues
		if (_true_ranges.get(pair) == null)
			_true_ranges.put(pair, new LinkedList<BigDecimal>());

		if (_plus_dms.get(pair) == null)
			_plus_dms.put(pair, new LinkedList<BigDecimal>());

		if (_minus_dms.get(pair) == null)
			_minus_dms.put(pair, new LinkedList<BigDecimal>());

		if (_dxs.get(pair) == null)
			_dxs.put(pair, new LinkedList<Double>());

		// queues
		Queue<BigDecimal> true_ranges = _true_ranges.get(pair);
		Queue<BigDecimal> plus_dms = _plus_dms.get(pair);
		Queue<BigDecimal> minus_dms = _minus_dms.get(pair);
		Queue<Double> dxs = _dxs.get(pair);

		Double plus_di = null;
		Double minus_di = null;
		BigDecimal tr = null;
		BigDecimal plus_dm_1 = null;
		BigDecimal minus_dm_1 = null;

		// need 2 data points
		if (_prev_closes.get(pair) != null && _prev_lows.get(pair) != null
				&& _prev_closes.get(pair) != null) {

			BigDecimal prev_high = _prev_highs.get(pair);
			BigDecimal prev_low = _prev_lows.get(pair);
			BigDecimal prev_close = _prev_closes.get(pair);

			// calc true range
			tr = high_bd.subtract(low_bd).max(
					high_bd.subtract(prev_close).abs().max(low_bd.subtract(prev_close).abs()));

			// calc +DM 1
			plus_dm_1 = (high_bd.subtract(prev_high).compareTo(prev_low.subtract(low_bd)) > 0) ? high_bd
					.subtract(prev_high).max(new BigDecimal("0")) : new BigDecimal("0");

			// calc -DM 1
			minus_dm_1 = (prev_low.subtract(low_bd).compareTo(high_bd.subtract(prev_high)) > 0) ? prev_low
					.subtract(low_bd).max(new BigDecimal("0")) : new BigDecimal("0");

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

			// use sum of queues if prev not set
			if (_prev_true_range.get(pair) == null && _prev_plus_dm.get(pair) == null
					&& _prev_minus_dm.get(pair) == null) {

				// tr sum
				BigDecimal sum_true_range = BigDecimal.ZERO;
				for (BigDecimal val : true_ranges) {
					sum_true_range = sum_true_range.add(val);
				}

				// plus dm sum
				BigDecimal sum_plus_dm = BigDecimal.ZERO;
				for (BigDecimal val : plus_dms) {
					sum_plus_dm = sum_plus_dm.add(val);
				}

				// minus dm sum
				BigDecimal sum_minus_dm = BigDecimal.ZERO;
				for (BigDecimal val : minus_dms) {
					sum_minus_dm = sum_minus_dm.add(val);
				}

				plus_di = (sum_plus_dm.doubleValue() / sum_true_range.doubleValue()) * 100;
				minus_di = (sum_minus_dm.doubleValue() / sum_true_range.doubleValue()) * 100;

				_prev_true_range.put(pair, sum_true_range.doubleValue());
				_prev_plus_dm.put(pair, sum_plus_dm.doubleValue());
				_prev_minus_dm.put(pair, sum_minus_dm.doubleValue());

			} else {

				// tr period
				Double tr_period = _prev_true_range.get(pair)
						- (_prev_true_range.get(pair) / _period) + tr.doubleValue();

				// plus dm period
				Double plus_dm_period = _prev_plus_dm.get(pair)
						- (_prev_plus_dm.get(pair) / _period) + plus_dm_1.doubleValue();

				// minus dm period
				Double minus_dm_period = _prev_minus_dm.get(pair)
						- (_prev_minus_dm.get(pair) / _period) + minus_dm_1.doubleValue();

				plus_di = (plus_dm_period / tr_period) * 100;
				minus_di = (minus_dm_period / tr_period) * 100;

				_prev_true_range.put(pair, tr_period);
				_prev_plus_dm.put(pair, plus_dm_period);
				_prev_minus_dm.put(pair, minus_dm_period);

			}

			Double di_diff = Math.abs(plus_di - minus_di);
			Double di_sum = plus_di + minus_di;
			dx1 = (di_diff / di_sum) * 100;

			// add to front
			dxs.add(dx1);

			// pop back if too long
			if (dxs.size() > _period)
				dxs.poll();

		}

		// calc adx
		if (dxs.size() == _period) {

			// calc first adx
			if (_prev_adxs.get(pair) == null) {

				// dx sum
				Double sum_dx = 0d;
				for (Double val : dxs) {
					sum_dx += val;
				}

				// calc adx
				Double adx = sum_dx / _period;

				// emit
				_collector.emit(new Values(pair, timeslice, String.format("%.2f", plus_di), String
						.format("%.2f", minus_di), String.format("%.2f", adx)));

				// save
				_prev_adxs.put(pair, adx);

			} else {

				// calc further adx
				Double adx = ((_prev_adxs.get(pair) * (_period - 1)) + dx1) / _period;

				// emit
				_collector.emit(new Values(pair, timeslice, String.format("%.2f", plus_di), String
						.format("%.2f", minus_di), String.format("%.2f", adx)));

				// save
				_prev_adxs.put(pair, adx);

			}

		}

		// save
		_prev_closes.put(pair, close_bd);
		_prev_lows.put(pair, low_bd);
		_prev_highs.put(pair, high_bd);
		_true_ranges.put(pair, true_ranges);
		_plus_dms.put(pair, plus_dms);
		_minus_dms.put(pair, minus_dms);
		_dxs.put(pair, dxs);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "plus_di", "minus_di", "adx"));
	}

}