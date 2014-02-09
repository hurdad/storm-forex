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

/*
 * Reference : http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:relative_strength_index_rsi
 */
public class RSIBolt extends BaseRichBolt {
	OutputCollector _collector;
	Integer _period;
	Map<String, Queue<BigDecimal>> _change_queues;
	Map<String, BigDecimal> _prev_close;
	Map<String, Double> _prev_avg_gain;
	Map<String, Double> _prev_avg_loss;
	Integer _counter;

	public RSIBolt(Integer period) {
		_period = period;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_change_queues = new HashMap<String, Queue<BigDecimal>>();
		_prev_close = new HashMap<String, BigDecimal>();
		_prev_avg_gain = new HashMap<String, Double>();
		_prev_avg_loss = new HashMap<String, Double>();
	}

	@Override
	public void execute(Tuple tuple) {

		// input vars
		String pair = tuple.getStringByField("pair");
		String close = tuple.getStringByField("close");
		Integer timeslice = tuple.getIntegerByField("timeslice");

		// init
		if (_change_queues.get(pair) == null)
			_change_queues.put(pair, new LinkedList<BigDecimal>());

		BigDecimal change = null;
		Double avg_gain = null;
		Double avg_loss = null;

		// pair change q
		Queue<BigDecimal> q = _change_queues.get(pair);

		// prev close required to calc change
		if (_prev_close.get(pair) != null) {

			// calc change
			change = new BigDecimal(close).subtract(_prev_close.get(pair));

			// add to front
			q.add(change);

			// pop back if too long
			if (q.size() > _period)
				q.poll();

		}

		// have enough data to calc first sma
		if (q.size() == _period && _prev_avg_gain.get(pair) == null
				&& _prev_avg_loss.get(pair) == null) {

			BigDecimal sum_gain = BigDecimal.ZERO;
			BigDecimal sum_loss = BigDecimal.ZERO;

			// loop change
			for (BigDecimal c : q) {

				if (c.compareTo(BigDecimal.ZERO) >= 0)
					sum_gain = sum_gain.add(c);

				if (c.compareTo(BigDecimal.ZERO) < 0)
					sum_loss = sum_loss.add(c.abs());
			}

			// calc avg gain/loss
			avg_gain = sum_gain.doubleValue() / _period;
			avg_loss = sum_loss.doubleValue() / _period;
		}

		// subsequent calcs
		if (q.size() == _period && _prev_avg_gain.get(pair) != null
				&& _prev_avg_loss.get(pair) != null && change != null) {

			BigDecimal gain = (change.compareTo(BigDecimal.ZERO) > 0) ? change
					: new BigDecimal("0");
			BigDecimal loss = (change.compareTo(BigDecimal.ZERO) < 0) ? change
					: new BigDecimal("0");

			// calc avg gain/loss
			avg_gain = (_prev_avg_gain.get(pair).doubleValue() * (_period - 1) + gain.doubleValue())
					/ _period;
			avg_loss = (_prev_avg_loss.get(pair).doubleValue() * (_period - 1) - loss.doubleValue())
					/ _period;

		}

		// avg_gain & avg_loss required for rs/rsi calc
		if (avg_gain != null & avg_loss != null) {

			// check divide by zero
			BigDecimal rsi;
			if (avg_loss == 0) {
				rsi = new BigDecimal("100.00");
			} else {

				// calc rs
				BigDecimal rs = new BigDecimal(avg_gain / avg_loss);

				// calc rsi
				rsi = new BigDecimal("100.00").subtract(new BigDecimal("100.00").divide(
						rs.add(new BigDecimal("1")), RoundingMode.HALF_UP));

			}

			// emit
			_collector.emit(new Values(pair, timeslice, rsi.toString()));

			// save
			_prev_avg_gain.put(pair, avg_gain);
			_prev_avg_loss.put(pair, avg_loss);

		}

		// save
		_change_queues.put(pair, q);
		_prev_close.put(pair, new BigDecimal(close));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "timeslice", "rsi"));
	}

}
