package com.github.hurdad.storm.forex.bolt;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
	Map<String, BigDecimal> _prev_closes;
	Map<String, Double> _prev_atrs;
	Map<String, Queue<BigDecimal>> _true_ranges;

	public ATRBolt(Integer period) {
		_period = period;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_prev_closes = new HashMap<String, BigDecimal>();
		_prev_atrs = new HashMap<String, Double>();
		_true_ranges = new HashMap<String, Queue<BigDecimal>>();

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
		if (_true_ranges.get(pair) == null)
			_true_ranges.put(pair, new LinkedList<BigDecimal>());

		Queue<BigDecimal> true_ranges = _true_ranges.get(pair);

		BigDecimal tr = null;
		BigDecimal high_minus_low = new BigDecimal(high).subtract(new BigDecimal(low));

		if (_prev_closes.get(pair) != null) {

			BigDecimal high_minus_close_past = new BigDecimal(high)
					.subtract(_prev_closes.get(pair)).abs();

			BigDecimal low_minus_close_past = new BigDecimal(low).subtract(_prev_closes.get(pair))
					.abs();

			// calc tr
			tr = high_minus_low.max(high_minus_close_past.max(low_minus_close_past));

			// add to front
			true_ranges.add(tr);

			// pop back if too long
			if (true_ranges.size() > _period)
				true_ranges.poll();

		} else {

			// first tr
			tr = high_minus_low;

			// add to front
			true_ranges.add(tr);
		}

		if (true_ranges.size() == _period) {

			// first atr
			if (_prev_atrs.get(pair) == null) {

				// tr sum
				BigDecimal sum_true_range = BigDecimal.ZERO;
				for (BigDecimal val : true_ranges) {
					sum_true_range = sum_true_range.add(val);
				}

				// calc first
				BigDecimal atr = sum_true_range.divide(new BigDecimal(_period),
						RoundingMode.HALF_UP);

				// emit
				_collector.emit(new Values(pair, timeslice,
						String.format("%.2f", atr.doubleValue())));

				// save
				_prev_atrs.put(pair, atr.doubleValue());

			} else {
				// remaining atr

				// calc atr
				Double atr = ((_prev_atrs.get(pair) * (_period - 1)) + tr.doubleValue()) / _period;

				// emit
				_collector.emit(new Values(pair, timeslice, String.format("%.2f", atr)));

				// save
				_prev_atrs.put(pair, atr);
			}

		}

		// save
		_true_ranges.put(pair, true_ranges);
		_prev_closes.put(pair, new BigDecimal(close));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "atr"));
	}

}