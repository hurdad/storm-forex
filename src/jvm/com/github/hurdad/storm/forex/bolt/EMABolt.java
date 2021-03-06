package com.github.hurdad.storm.forex.bolt;

import java.math.BigDecimal;
import java.math.MathContext;
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

public class EMABolt extends BaseRichBolt {
	OutputCollector _collector;
	Integer _period;
	Integer _scale;
	Double _smoothing_constant;
	Map<String, Queue<BigDecimal>> _close_queues;
	Map<String, BigDecimal> _prev_emas;

	public EMABolt(Integer period, Integer scale) {
		_period = period;
		_scale = scale;
		_smoothing_constant =  (2 / (period.doubleValue() + 1));
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_close_queues = new HashMap<String, Queue<BigDecimal>>();
		_prev_emas = new HashMap<String, BigDecimal>();
	}

	@Override
	public void execute(Tuple tuple) {

		// input vars
		String pair = tuple.getStringByField("pair");
		String close = tuple.getStringByField("close");
		Integer timeslice = tuple.getIntegerByField("timeslice");

		// init
		if (_close_queues.get(pair) == null)
			_close_queues.put(pair, new LinkedList<BigDecimal>());

		// get queue for pair
		Queue<BigDecimal> q = _close_queues.get(pair);

		// push close price onto queue
		q.add(new BigDecimal(close));

		// pop back if too long
		if (q.size() > _period)
			q.poll();

		// check if we have enough data to calc ema
		if (q.size() == _period) {

			// use sma if prev ema not set
			if (_prev_emas.get(pair) == null) {

				// calc sma
				BigDecimal sum = BigDecimal.ZERO;
				for (BigDecimal val : q) {
					sum = sum.add(val);
				}
				BigDecimal sma = sum.divide(new BigDecimal(_period), _scale, RoundingMode.HALF_UP);
				
				// emit
				_collector.emit(new Values(pair, timeslice, sma.toString()));

				// save
				_prev_emas.put(pair, sma);

			} else {

				// calc ema
				BigDecimal ema = (new BigDecimal(close).subtract(_prev_emas.get(pair))).multiply(new BigDecimal(_smoothing_constant)).add(_prev_emas.get(pair));
				ema = ema.setScale(_scale,  RoundingMode.HALF_UP);

				// emit
				_collector.emit(new Values(pair, timeslice, ema.toString()));

				// save
				_prev_emas.put(pair, ema);
			}

		}
		// save
		_close_queues.put(pair, q);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "ema"));
	}

}