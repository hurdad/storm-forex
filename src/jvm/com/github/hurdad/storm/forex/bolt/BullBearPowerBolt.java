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

public class BullBearPowerBolt extends BaseRichBolt {
	OutputCollector _collector;
	Integer _period;
	Integer _smoothing_constant;
	Map<String, Queue<Double>> _close_queues;
	Map<String, Double> _prev_emas;

	public BullBearPowerBolt(Integer period) {
		_period = period;
		_smoothing_constant = 2 / (period + 1);
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_close_queues = new HashMap<String, Queue<Double>>();
		_prev_emas = new HashMap<String, Double>();
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
		if (_close_queues.get(pair) == null)
			_close_queues.put(pair, new LinkedList<Double>());

		// get queue for pair
		Queue<Double> q = _close_queues.get(pair);

		// push close price onto queue
		q.add(close);

		// pop back if too long
		if (q.size() > _period)
			q.poll();

		// check if we have enough data to calc ema
		if (q.size() == _period) {

			// use sma if prev ema not set
			if (_prev_emas.get(pair) == null) {

				// calc sma
				Double sum = 0d;
				for (Double val : q) {
					sum = sum + val;
				}
				Double sma = sum / _period;

				// save
				_prev_emas.put(pair, sma);

			} else {

				// calc ema
				Double ema = (close - _prev_emas.get(pair)) * _smoothing_constant
						+ _prev_emas.get(pair);

				// save
				_prev_emas.put(pair, ema);
			}

			// calc bull bear power
			Double bull_power = high - _prev_emas.get(pair);
			Double bear_power = low - _prev_emas.get(pair);
			Double diff = bull_power - bear_power;
			diff = Math.round(diff * 100) / 100.0d;

			if (pair.equals("EUR/USD"))
				System.out.println(timeslice + " bbp:" + diff);

			// emit
			_collector.emit(new Values(pair, timeslice, diff));
		}
		// save
		_close_queues.put(pair, q);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "bullbearpower"));
	}

}