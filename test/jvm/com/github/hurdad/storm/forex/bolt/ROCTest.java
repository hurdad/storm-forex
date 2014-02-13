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

/*
 * Reference : http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:rate_of_change_roc_a
 */
public class ROCTest {

	class TupleType {
		public String close;
		public Integer ts;

		public TupleType(Integer ts, String close) {
			this.close = close;
			this.ts = ts;
		}
	}

	@Test
	public void shouldEmitROCValues() {

		// given
		ROCBolt bolt = new ROCBolt(12);
		Map conf = mock(Map.class);
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(conf, context, collector);

		List<TupleType> data = new ArrayList<TupleType>();
		data.add(new TupleType(1272412800, "11045.27"));
		data.add(new TupleType(1272499200, "11167.32"));
		data.add(new TupleType(1272585600, "11008.61"));
		data.add(new TupleType(1272844800, "11151.83"));
		data.add(new TupleType(1272931200, "10926.77"));
		data.add(new TupleType(1273017600, "10868.12"));
		data.add(new TupleType(1273104000, "10520.32"));
		data.add(new TupleType(1273190400, "10380.43"));
		data.add(new TupleType(1273449600, "10785.14"));
		data.add(new TupleType(1273536000, "10748.26"));
		data.add(new TupleType(1273622400, "10896.91"));
		data.add(new TupleType(1273708800, "10782.95"));
		data.add(new TupleType(1273795200, "10620.16"));
		data.add(new TupleType(1274054400, "10625.83"));
		data.add(new TupleType(1274140800, "10510.95"));
		data.add(new TupleType(1274227200, "10444.37"));
		data.add(new TupleType(1274313600, "10068.01"));
		data.add(new TupleType(1274400000, "10193.39"));
		data.add(new TupleType(1274659200, "10066.57"));
		data.add(new TupleType(1274745600, "10043.75"));

		// when
		for (TupleType r : data) {
			Tuple tuple = mock(Tuple.class);
			when(tuple.getStringByField("pair")).thenReturn("mypair");
			when(tuple.getStringByField("close")).thenReturn(r.close);
			when(tuple.getIntegerByField("timeslice")).thenReturn(r.ts);

			// send tuple to bolt
			bolt.execute(tuple);
		}

		// then
		verify(collector).emit(new Values("mypair", 1273795200, "-3.85"));
		verify(collector).emit(new Values("mypair", 1274054400, "-4.85"));
		verify(collector).emit(new Values("mypair", 1274140800, "-4.52"));
		verify(collector).emit(new Values("mypair", 1274227200, "-6.34"));
		verify(collector).emit(new Values("mypair", 1274313600, "-7.86"));
		verify(collector).emit(new Values("mypair", 1274400000, "-6.21"));
		verify(collector).emit(new Values("mypair", 1274659200, "-4.31"));
		verify(collector).emit(new Values("mypair", 1274745600, "-3.24"));

	}
}
