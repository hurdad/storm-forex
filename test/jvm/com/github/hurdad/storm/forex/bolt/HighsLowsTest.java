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

public class HighsLowsTest {

	class TupleType {
		public String high;
		public String low;

		public Integer ts;

		public TupleType(Integer ts, String high, String low) {
			this.high = high;
			this.low = low;
			this.ts = ts;
		}
	}

	@Test
	public void shouldEmitHighsLowsValues() {

		// given
		HighsLowsBolt bolt = new HighsLowsBolt(14);
		Map conf = mock(Map.class);
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(conf, context, collector);

		List<TupleType> data = new ArrayList<TupleType>();

		data.add(new TupleType(1363046400, "155.35", "153.77"));
		data.add(new TupleType(1363132800, "156.17", "155.09"));
		data.add(new TupleType(1363219200, "157.03", "154.82"));
		data.add(new TupleType(1363305600, "157.21", "156.37"));
		data.add(new TupleType(1363564800, "156.91", "155.67"));
		data.add(new TupleType(1363651200, "156.85", "155.75"));
		data.add(new TupleType(1363737600, "156.24", "155.00"));
		data.add(new TupleType(1363824000, "156.23", "155.42"));
		data.add(new TupleType(1363910400, "156.27", "154.35"));
		data.add(new TupleType(1364169600, "155.60", "154.73"));
		data.add(new TupleType(1364256000, "155.64", "154.10"));
		data.add(new TupleType(1364342400, "155.95", "155.26"));
		data.add(new TupleType(1364428800, "155.51", "153.59"));
		data.add(new TupleType(1364774400, "155.64", "154.20"));
		data.add(new TupleType(1364860800, "156.04", "155.31"));
		data.add(new TupleType(1364947200, "156.80", "155.91"));
		data.add(new TupleType(1365033600, "156.12", "155.23"));
		data.add(new TupleType(1365120000, "156.10", "155.21"));

		// when
		for (TupleType r : data) {
			Tuple tuple = mock(Tuple.class);
			when(tuple.getStringByField("pair")).thenReturn("mypair");
			when(tuple.getStringByField("high")).thenReturn(r.high);
			when(tuple.getStringByField("low")).thenReturn(r.low);
			when(tuple.getIntegerByField("timeslice")).thenReturn(r.ts);

			// send tuple to bolt
			bolt.execute(tuple);
		}

		// then
		verify(collector).emit(new Values("mypair", 1364774400, "0.0085"));
		verify(collector).emit(new Values("mypair", 1364860800, "0.0081"));
		verify(collector).emit(new Values("mypair", 1364947200, "0.0080"));
		verify(collector).emit(new Values("mypair", 1365033600, "0.0074"));
		verify(collector).emit(new Values("mypair", 1365120000, "0.0075"));

	}
}
