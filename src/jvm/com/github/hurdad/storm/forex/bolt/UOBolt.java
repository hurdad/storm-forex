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

public class UOBolt extends BaseRichBolt {

	OutputCollector _collector;
	Integer _period1, _period2, _period3;
	Map<String, Double> _prev_closes;
	Map<String, Queue<Double>> _buying_pressure1, _buying_pressure2, _buying_pressure3;
	Map<String, Queue<Double>> _true_ranges1, _true_ranges2, _true_ranges3;

	public UOBolt(Integer period1, Integer period2, Integer period3) {
		_period1 = period1;
		_period2 = period2;
		_period3 = period3;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_prev_closes = new HashMap<String, Double>();
		_buying_pressure1 = new HashMap<String, Queue<Double>>();
		_buying_pressure2 = new HashMap<String, Queue<Double>>();
		_buying_pressure3 = new HashMap<String, Queue<Double>>();
		_true_ranges1 = new HashMap<String, Queue<Double>>();
		_true_ranges2 = new HashMap<String, Queue<Double>>();
		_true_ranges3 = new HashMap<String, Queue<Double>>();
	}

	@Override
	public void execute(Tuple tuple) {

		// input vars
		String pair = tuple.getStringByField("pair");
		Double high = tuple.getDoubleByField("high");
		Double low = tuple.getDoubleByField("low");
		Double close = tuple.getDoubleByField("low");
		Integer timeslice = tuple.getIntegerByField("timeslice");

		// init
		if (_buying_pressure1.get(pair) == null)
			_buying_pressure1.put(pair, new LinkedList<Double>());
		if (_buying_pressure2.get(pair) == null)
			_buying_pressure2.put(pair, new LinkedList<Double>());
		if (_buying_pressure2.get(pair) == null)
			_buying_pressure2.put(pair, new LinkedList<Double>());
		if (_true_ranges1.get(pair) == null)
			_true_ranges1.put(pair, new LinkedList<Double>());
		if (_true_ranges2.get(pair) == null)
			_true_ranges2.put(pair, new LinkedList<Double>());
		if (_true_ranges3.get(pair) == null)
			_true_ranges3.put(pair, new LinkedList<Double>());

		// queues
		Queue<Double> bp1 = _buying_pressure1.get(pair);
		Queue<Double> bp2 = _buying_pressure1.get(pair);
		Queue<Double> bp3 = _buying_pressure1.get(pair);
		Queue<Double> tr1 = _true_ranges1.get(pair);
		Queue<Double> tr2 = _true_ranges2.get(pair);
		Queue<Double> tr3 = _true_ranges3.get(pair);

		// need 2 data points
		if (_prev_closes.get(pair) != null) {

			// calc buying pressure
			Double buying_pressure = close - Math.min(low, _prev_closes.get(pair));

			// add to front
			bp1.add(buying_pressure);
			bp2.add(buying_pressure);
			bp3.add(buying_pressure);

			// pop back if too long
			if (bp1.size() > _period1)
				bp1.poll();
			if (bp2.size() > _period2)
				bp2.poll();
			if (bp3.size() > _period3)
				bp3.poll();

			// calc true range
			Double tr = Math.max(high, _prev_closes.get(pair))
					- Math.min(low, _prev_closes.get(pair));

			// add to front
			tr1.add(tr);
			tr2.add(tr);
			tr3.add(tr);

			// pop back if too long
			if (tr1.size() > _period1)
				tr1.poll();
			if (tr2.size() > _period2)
				tr2.poll();
			if (tr3.size() > _period3)
				tr3.poll();
		}

		// calc Ultimate
		if (tr3.size() == _period3 && bp3.size() == _period3) {

			Double sum_true_range_1 = 0d;
			for (Double val : tr1) {
				sum_true_range_1 += val;
			}

			Double sum_buying_pressure_1 = 0d;
			for (Double val : bp1) {
				sum_buying_pressure_1 += val;
			}

			// calc avg of period1
			Double avg_1 = sum_buying_pressure_1 / sum_true_range_1;

			Double sum_true_range_2 = 0d;
			for (Double val : tr2) {
				sum_true_range_2 += val;
			}
			Double sum_buying_pressure_2 = 0d;
			for (Double val : bp2) {
				sum_buying_pressure_2 += val;
			}

			// calc average period2
			Double avg_2 = sum_buying_pressure_2 / sum_true_range_2;

			Double sum_true_range_3 = 0d;
			for (Double val : tr3) {
				sum_true_range_3 += val;
			}
			Double sum_buying_pressure_3 = 0d;
			for (Double val : bp3) {
				sum_buying_pressure_3 += val;
			}

			// calc avg_28
			Double avg_3 = sum_buying_pressure_3 / sum_true_range_3;

			// calc Ulitimate Oscillator
			Double uo = 100 * ((4 * avg_1) + (2 * avg_2) + avg_3) / (4 + 2 + 1);
			uo = Math.round(uo * 100) / 100.0d;

			if (pair.equals("EUR/USD"))
				System.out.println(timeslice + " uo:" + uo);

			// emit
			_collector.emit(new Values(pair, timeslice, uo));
		}

		// save
		_prev_closes.put(pair, close);
		_buying_pressure1.put(pair, bp1);
		_buying_pressure2.put(pair, bp2);
		_buying_pressure3.put(pair, bp3);
		_true_ranges1.put(pair, tr1);
		_true_ranges2.put(pair, tr2);
		_true_ranges3.put(pair, tr3);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "uo"));
	}

}