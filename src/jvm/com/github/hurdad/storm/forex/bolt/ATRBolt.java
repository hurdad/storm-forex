package com.github.hurdad.storm.forex.bolt;

import java.util.HashMap;
import java.util.Map;

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
	Map<String, Double> _tr_sums;
	Integer _counter;

	public ATRBolt(Integer period) {
		_period = period;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_prev_closes = new HashMap<String, Double>();
		_prev_atrs = new HashMap<String, Double>();
		_tr_sums = new HashMap<String, Double>();
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

		Double tr = null;
		Double high_minus_low = high - low;

		if (_prev_closes.get(pair) != null) {

			Double high_minus_close_past = Math.abs(high - _prev_closes.get(pair));
			Double low_minus_close_past = Math.abs(low - _prev_closes.get(pair));

			// calc tr
			tr = Math.max(high_minus_low, Math.max(high_minus_close_past, low_minus_close_past));

			// sum first TRs for first ATR avg
			if (_counter <= _period) {
				if (_tr_sums.get(pair) != null) {
					_tr_sums.put(pair, tr);
				} else {
					Double sum = _tr_sums.get(pair);
					sum += tr;
					_tr_sums.put(pair, sum);
				}
			}
		}

		// first ATR
		if (_counter == _period) {

			// calc first
			Double atr = _tr_sums.get(pair) / _period;

			if (pair.equals("EUR/USD"))
				System.out.println(pair + " atr:" +  atr);
			
			// emit
			_collector.emit(new Values(pair, timeslice, atr));

			// save
			_prev_atrs.put(pair, atr);

		}

		// remaining ATR
		if (_counter > _period) {

			// calc atr
			Double atr = ((_prev_atrs.get(pair) * (_period - 1)) + tr) / _period;

	    	if (pair.equals("EUR/USD"))
				System.out.println(pair + " atr:" +  atr);
	    	
			// emit
			_collector.emit(new Values(pair, timeslice, atr));

			// save
			_prev_atrs.put(pair, atr);
		}

		// save
		_prev_closes.put(pair, close);

		// incrs
		_counter++;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "atr"));
	}

}