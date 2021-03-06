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

public class OHLCBolt extends BaseRichBolt {
	OutputCollector _collector;
	Integer _time_window;
	Map<String, Double> _opens;
	Map<String, Double> _lows;
	Map<String, Double> _highs;
	Map<String, Long> _vols;
	Map<String, Integer> _lastTimeMap;
	Map<String, Double> _previousPriceMap;
	Map<String, Integer> _currentTimesliceMap;

	public OHLCBolt(Integer time_window) {
		_time_window = time_window;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;

		_opens = new HashMap<String, Double>();
		_lows = new HashMap<String, Double>();
		_highs = new HashMap<String, Double>();
		_vols = new HashMap<String, Long>();

		_lastTimeMap = new HashMap<String, Integer>();
		_previousPriceMap = new HashMap<String, Double>();
		_currentTimesliceMap = new HashMap<String, Integer>();
	}

	@Override
	public void execute(Tuple tuple) {

		// init input tuple vars
		String pair = tuple.getStringByField("pair");
		Double price = tuple.getDoubleByField("price");
		Long ts = (long) Math.floor((tuple.getLongByField("timestamp") / 1000));

		// filter duplicate or old ts
		if (_lastTimeMap.get(pair) != null && ts <= _lastTimeMap.get(pair))
			return;

		// calculate timeslice from timestamp
		Long timeslice = (long) Math.floor((ts / _time_window)) * _time_window;

		// timeslice change - output previous candle OHLCV
		if (_currentTimesliceMap.get(pair) != null
				&& timeslice.intValue() != _currentTimesliceMap.get(pair)) {

			// emit
			_collector.emit(new Values(pair, _opens.get(pair).toString(), _highs.get(pair)
					.toString(), _lows.get(pair).toString(),
					_previousPriceMap.get(pair).toString(), _vols.get(pair), _currentTimesliceMap
							.get(pair), _time_window));

			// reset
			_opens.put(pair, price);
			_highs.put(pair, price);
			_lows.put(pair, price);
			_vols.put(pair, 0l);

		} else {
			// update ohlcv maps

			if (_opens.get(pair) == null)
				_opens.put(pair, price);

			if (_highs.get(pair) == null || price > _highs.get(pair))
				_highs.put(pair, price);

			if (_lows.get(pair) == null || price < _lows.get(pair))
				_lows.put(pair, price);

			if (_vols.get(pair) == null)
				_vols.put(pair, 0l);

			Long counter = _vols.get(pair);
			counter++;
			_vols.put(pair, counter);
		}

		// save
		_previousPriceMap.put(pair, price);
		_lastTimeMap.put(pair, ts.intValue());
		_currentTimesliceMap.put(pair, timeslice.intValue());

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "open", "high", "low", "close", "vol", "timeslice",
				"timeduration"));
	}

}