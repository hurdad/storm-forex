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

public class MACDBolt extends BaseRichBolt {
	OutputCollector _collector;
	Integer _ema1, _ema2, _signal;
	Double _smoothing_constant_1, _smoothing_constant_2, _smoothing_constant_3;
	Map<String, Queue<Double>> _close_queues1;
	Map<String, Queue<Double>> _close_queues2;
	Map<String, Queue<Double>> _macd_queues;
	Map<String, Double> _prev_ema1;
	Map<String, Double> _prev_ema2;
	Map<String, Double> _prev_macd;

	public MACDBolt(Integer ema1, Integer ema2, Integer signal) {
		_ema1 = ema1;
		_ema2 = ema2;
		_signal = signal;
		_smoothing_constant_1 = (2 / (ema1.doubleValue() + 1));
		_smoothing_constant_2 = (2 / (ema2.doubleValue() + 1));
		_smoothing_constant_3 = (2 / (signal.doubleValue() + 1));
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_close_queues1 = new HashMap<String, Queue<Double>>();
		_close_queues2 = new HashMap<String, Queue<Double>>();
		_macd_queues = new HashMap<String, Queue<Double>>();
		_prev_ema1 = new HashMap<String, Double>();
		_prev_ema2 = new HashMap<String, Double>();
		_prev_macd = new HashMap<String, Double>();
	}

	@Override
	public void execute(Tuple tuple) {

		// input vars
		String pair = tuple.getStringByField("pair");
		String close = tuple.getStringByField("close");
		Integer timeslice = tuple.getIntegerByField("timeslice");

		// init
		if (_close_queues1.get(pair) == null)
			_close_queues1.put(pair, new LinkedList<Double>());

		if (_close_queues2.get(pair) == null)
			_close_queues2.put(pair, new LinkedList<Double>());

		if (_macd_queues.get(pair) == null)
			_macd_queues.put(pair, new LinkedList<Double>());

		// get queue for pair
		Queue<Double> close1 = _close_queues1.get(pair);
		Queue<Double> close2 = _close_queues2.get(pair);
		Queue<Double> macd = _macd_queues.get(pair);

		Double ema1_value = null;
		Double ema2_value = null;
		Double macd_line = null;
		Double macd_line_ema = null;

		// push close price onto queue
		close1.add(Double.parseDouble(close));
		close2.add(Double.parseDouble(close));

		// pop back if too long
		if (close1.size() > _ema1)
			close1.poll();

		if (close2.size() > _ema2)
			close2.poll();

		// ema 1
		if (close1.size() == _ema1) {

			if (_prev_ema1.get(pair) == null) {

				// calc sma
				Double sum = 0d;
				for (Double val : close1) {
					sum = sum + val;
				}
				Double sma = sum / _ema1;

				// save
				_prev_ema1.put(pair, sma);
				ema1_value = sma;

			} else {

				// ema formula
				Double ema = (Double.parseDouble(close) - _prev_ema1.get(pair))
						* _smoothing_constant_1 + _prev_ema1.get(pair);

				// save
				_prev_ema1.put(pair, ema);
				ema1_value = ema;
			}

		}

		// ema 2
		if (close2.size() == _ema2) {

			// first
			if (_prev_ema2.get(pair) == null) {

				// calc sma
				Double sum = 0d;
				for (Double val : close2) {
					sum = sum + val;
				}
				Double sma = sum / _ema2;

				// save
				_prev_ema2.put(pair, sma);
				ema2_value = sma;

			} else {

				// ema formula
				Double ema = (Double.parseDouble(close) - _prev_ema2.get(pair))
						* _smoothing_constant_2 + _prev_ema2.get(pair);

				// save
				_prev_ema2.put(pair, ema);
				ema2_value = ema;

			}
		}

		// check if we have 2 values to calc MACD Line
		if (ema1_value != null && ema2_value != null) {

			// calc macd line
			macd_line = ema1_value - ema2_value;

			// add to front
			macd.add(macd_line);

			// pop back if too long
			if (macd.size() > _signal)
				macd.poll();

		}

		// have enough data to calc signal sma
		if (macd.size() == _signal) {

			// first
			if (_prev_macd.get(pair) == null) {

				// k moving average
				Double sum = 0d;
				for (Double val : macd) {
					sum = sum + val;
				}
				Double macd_line_sma = sum / _signal;

				_prev_macd.put(pair, macd_line_sma);
				macd_line_ema = macd_line_sma;

			} else {

				// ema formula
				macd_line_ema = (macd_line - _prev_macd.get(pair)) * _smoothing_constant_3
						+ _prev_macd.get(pair);

				_prev_macd.put(pair, macd_line_ema);

			}
		}

		//check if we have values to emit
		if (macd_line != null && macd_line_ema != null) {

			// histogram calc
			Double histogram = macd_line - macd_line_ema;

			// emit
			_collector.emit(new Values(pair, timeslice, String.format("%.3f", macd_line), String
					.format("%.3f", macd_line_ema), String.format("%.3f", histogram)));
		}

		// save
		_close_queues1.put(pair, close1);
		_close_queues2.put(pair, close2);
		_macd_queues.put(pair, macd);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "macd", "signal", "histogram"));
	}

}