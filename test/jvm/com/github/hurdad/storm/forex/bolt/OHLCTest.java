package com.github.hurdad.storm.forex.bolt;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class OHLCTest {

	class TupleType {
		public Double price;
		public Long timestamp;

		public TupleType(Long timestamp, Double price) {
			this.price = price;
			this.timestamp = timestamp;
		}
	}

	@Test
	public void shouldEmitOHLCValues() {

		// given
		OHLCBolt bolt = new OHLCBolt(10); // 10 second candles
		Map conf = mock(Map.class);
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(conf, context, collector);

		List<TupleType> data = new ArrayList<TupleType>();

		/*----------------------------------------------------------------------*/
		data.add(new TupleType(1357122621208l, 153.77)); // 2013-01-02
															// 05:30:21.208
		data.add(new TupleType(1357122622816l, 156.17)); // 2013-01-02
															// 05:30:22.816
		data.add(new TupleType(1357122623126l, 154.82)); // 2013-01-02
															// 05:30:23.126
		data.add(new TupleType(1357122626294l, 154.63)); // 2013-01-02
															// 05:30:26.294
		data.add(new TupleType(1357122628127l, 154.68)); // 2013-01-02
															// 05:30:28.127
		data.add(new TupleType(1357122629809l, 154.59)); // 2013-01-02
															// 05:30:29.809
		/*----------------------------------------------------------------------*/
		data.add(new TupleType(1357122631636l, 154.55)); // 2013-01-02
															// 05:30:31.636
		data.add(new TupleType(1357122634377l, 154.51)); // 2013-01-02
															// 05:30:34.377
		data.add(new TupleType(1357122634627l, 154.60)); // 2013-01-02
															// 05:30:34.627
		data.add(new TupleType(1357122638516l, 154.63)); // 2013-01-02
															// 05:30:38.516
		data.add(new TupleType(1357122639195l, 154.69)); // 2013-01-02
															// 05:30:39.195
		/*----------------------------------------------------------------------*/
		data.add(new TupleType(1357122641015l, 154.57)); // 2013-01-02
															// 05:30:41.015

		// when
		for (TupleType r : data) {
			Tuple tuple = mock(Tuple.class);
			when(tuple.getStringByField("pair")).thenReturn("mypair");
			when(tuple.getDoubleByField("price")).thenReturn(r.price);
			when(tuple.getLongByField("timestamp")).thenReturn(r.timestamp);

			// send tuple to bolt
			bolt.execute(tuple);
		}

		// then
		verify(collector).emit(
				new Values("mypair", "153.77", "156.17", "153.77", "154.59", 6l, 1357122620, 10));
		verify(collector).emit(
				new Values("mypair", "154.55", "154.69", "154.51", "154.69", 3l, 1357122630, 10));

	}

}
