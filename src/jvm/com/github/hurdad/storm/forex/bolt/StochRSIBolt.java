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

public class StochRSIBolt extends BaseRichBolt {
	OutputCollector _collector;
	Integer _period;
	Map<String, Queue<Double>> _change_queues;
	Map<String, Queue<Double>> _rsi_queues;
	Map<String, Double> _prev_close;
	Integer _counter;

	public StochRSIBolt(Integer period) {
		_period = period;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_change_queues = new HashMap<String, Queue<Double>>();
		_rsi_queues = new HashMap<String, Queue<Double>>();
		_prev_close = new HashMap<String, Double>();
	}

	@Override
	public void execute(Tuple tuple) {

		// input vars
		String pair = tuple.getStringByField("pair");
		Double close = tuple.getDoubleByField("close");
		Integer timeslice = tuple.getIntegerByField("timeslice");

		// init
		if (_change_queues.get(pair) == null)
			_change_queues.put(pair, new LinkedList<Double>());
		if (_rsi_queues.get(pair) == null)
			_rsi_queues.put(pair, new LinkedList<Double>());

		// pair change q
		Queue<Double> q = _change_queues.get(pair);
		Queue<Double> rsiq = _rsi_queues.get(pair);

		// need 2 points to get change
		if (_prev_close.get(pair) != null) {

			// calc change
			Double change = close - _prev_close.get(pair);

			// add to front
			q.add(change);

			// pop back if too long
			if (q.size() > _period)
				q.poll();

		}

		// have enough data to calc rsi
		Double rsi = null;
		if (q.size() == _period) {

			Double sum_gain = 0d;
			Double sum_loss = 0d;

			// loop change
			for (Double change : q) {

				if (change >= 0)
					sum_gain += change;

				if (change < 0)
					sum_loss += Math.abs(change);

			}

			Double avg_gain = sum_gain / _period;
			Double avg_loss = sum_loss / _period;

			// check divide by zero
			if (avg_loss == 0) {
				rsi = 100.00d;
			} else {
				// calc and normalize
				Double rs = avg_gain / avg_loss;
				rsi = 100 - (100 / (1 + rs));
				rsi = Math.round(rsi * 100) / 100.0d;
			}
			
			// add to front
			rsiq.add(rsi);

			// pop back if too long
			if (rsiq.size() > _period)
				rsiq.poll();

		}

		// have enough data to calc stochrsi
		if (rsiq.size() == _period && rsi != null) {
			
			// loop rsi
			Double min_rsi = rsiq.peek();
			Double max_rsi = rsiq.peek();
			for (Double val : rsiq) {
				min_rsi = Math.min(val, min_rsi);
				max_rsi = Math.max(val, max_rsi);
			}
			
			//stoch rsi calc
			Double stochRSI = (rsi - min_rsi) / (max_rsi - min_rsi);
			stochRSI = Math.round(stochRSI * 100) / 100.0d;
			
			if (pair.equals("EUR/USD"))
				System.out.println(timeslice + " stochrsi:" + stochRSI);
			
			// emit
			_collector.emit(new Values(pair, timeslice, stochRSI));

		}
		
		// save
		_change_queues.put(pair, q);
		_rsi_queues.put(pair, rsiq);
		_prev_close.put(pair, close);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "stochrsi"));
	}

}
