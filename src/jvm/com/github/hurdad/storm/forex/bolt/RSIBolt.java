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
	Integer _counter;

	public RSIBolt(Integer period) {
		_period = period;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_change_queues = new HashMap<String, Queue<BigDecimal>>();
		_prev_close = new HashMap<String, BigDecimal>();
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

		// pair change q
		Queue<BigDecimal> q = _change_queues.get(pair);

		// need 2 points to get change
		if (_prev_close.get(pair) != null) {

			BigDecimal bg1 = new BigDecimal(close);
			BigDecimal bg2 = _prev_close.get(pair);

			BigDecimal change = bg1.subtract(bg2);

			// calc change
			// Double change = close - _prev_close.get(pair);
			// System.out.println(close);
			// System.out.println(_prev_close.get(pair));
			// System.out.println(change);

			// add to front
			q.add(change);

			// pop back if too long
			if (q.size() > _period)
				q.poll();

		}

		// have enough data to calc rsi
		if (q.size() >= _period) {

			BigDecimal sum_gain = BigDecimal.ZERO;
			BigDecimal sum_loss = BigDecimal.ZERO;

			// loop change
			for (BigDecimal change : q) {

				if (change.compareTo(BigDecimal.ZERO) >= 0)
					sum_gain = sum_gain.add(change);
				// sum_gain += change;

				if (change.compareTo(BigDecimal.ZERO) < 0)
					sum_loss = sum_loss.add(change.abs());
				// sum_loss += change.abs()

			}

			// System.out.println(sum_gain);
			// System.out.println(sum_loss);

			BigDecimal avg_gain = sum_gain.divide(new BigDecimal("14.00"), RoundingMode.HALF_UP);
			BigDecimal avg_loss = sum_loss.divide(new BigDecimal("14.00"), RoundingMode.HALF_UP);

			System.out.println(avg_gain);
			System.out.println(avg_loss);

			// Double avg_gain = sum_gain / _period;
			// Double avg_loss = sum_loss / _period;

			// check divide by zero
			// Double rsi;
			BigDecimal rsi;
			if (avg_loss.compareTo(BigDecimal.ZERO) == 0) {
				rsi = new BigDecimal("100.00");
			} else {
				// calc and normalize
				BigDecimal rs = avg_gain.divide(avg_loss, RoundingMode.HALF_UP);
				// System.out.println(rs);
				// Double rs = avg_gain / avg_loss;

				BigDecimal t1 = new BigDecimal("100.00");
				BigDecimal t2 = new BigDecimal("100.00");

				rsi = t1.subtract(t2.divide(rs.add(new BigDecimal("1")), RoundingMode.HALF_UP));
				// System.out.println(rsi);
				// rsi = 100 - (100 / (1 + rs));
				// rsi = Math.round(rsi * 100) / 100.0d;
			}

			// if (pair.equals("EUR/USD"))
			// System.out.println(timeslice + " rsi:" + rsi);

			// emit
			_collector.emit(new Values(pair, timeslice, rsi));

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
