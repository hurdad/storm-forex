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

public class SMABolt extends BaseRichBolt {
	OutputCollector _collector;
	Integer _period;
	Map<String, Queue<BigDecimal>> _close_queues;

	public SMABolt(Integer period) {
		_period = period;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_close_queues = new HashMap<String, Queue<BigDecimal>>();
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

		// check if we have enough data to calc sma
		if (q.size() == _period) {

			// calc sma
			BigDecimal sum = BigDecimal.ZERO;
			for (BigDecimal val : q) {
				sum = sum.add(val);
			}
			BigDecimal sma = sum.divide(new BigDecimal(_period), RoundingMode.HALF_UP);

			// emit
			_collector.emit(new Values(pair, timeslice, sma.toString()));

		}

		// save
		_close_queues.put(pair, q);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "sma"));
	}

}