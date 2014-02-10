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
 * Reference :  http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:average_true_range_a
 */
public class ATRTest {
	class TupleType {
		public String high;
		public String low;
		public String close;
		public Integer ts;

		public TupleType(Integer ts, String high, String low, String close) {
			this.high = high;
			this.low = low;
			this.close = close;
			this.ts = ts;
		}
	}

	@Test
	public void shouldEmitATRValues() {

		// given
		ATRBolt bolt = new ATRBolt(14);
		Map conf = mock(Map.class);
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(conf, context, collector);

		List<TupleType> data = new ArrayList<TupleType>();

		data.add(new TupleType(1270080000, "48.7", "47.79", "48.16"));
		data.add(new TupleType(1270425600, "48.72", "48.14", "48.61"));
		data.add(new TupleType(1270512000, "48.9", "48.39", "48.75"));
		data.add(new TupleType(1270598400, "48.87", "48.37", "48.63"));
		data.add(new TupleType(1270684800, "48.82", "48.24", "48.74"));
		data.add(new TupleType(1270771200, "49.05", "48.635", "49.03"));
		data.add(new TupleType(1271030400, "49.2", "48.94", "49.07"));
		data.add(new TupleType(1271116800, "49.35", "48.86", "49.32"));
		data.add(new TupleType(1271203200, "49.92", "49.5", "49.91"));
		data.add(new TupleType(1271289600, "50.19", "49.87", "50.13"));
		data.add(new TupleType(1271376000, "50.12", "49.2", "49.53"));
		data.add(new TupleType(1271635200, "49.66", "48.9", "49.5"));
		data.add(new TupleType(1271721600, "49.88", "49.43", "49.75"));
		data.add(new TupleType(1271808000, "50.19", "49.725", "50.03"));
		data.add(new TupleType(1271894400, "50.36", "49.26", "50.31"));
		data.add(new TupleType(1271980800, "50.57", "50.09", "50.52"));
		data.add(new TupleType(1272240000, "50.65", "50.3", "50.41"));
		data.add(new TupleType(1272326400, "50.43", "49.21", "49.34"));
		data.add(new TupleType(1272412800, "49.63", "48.98", "49.37"));
		data.add(new TupleType(1272499200, "50.33", "49.61", "50.23"));
		data.add(new TupleType(1272585600, "50.29", "49.2", "49.2375"));
		data.add(new TupleType(1272844800, "50.17", "49.43", "49.93"));
		data.add(new TupleType(1272931200, "49.32", "48.08", "48.43"));
		data.add(new TupleType(1273017600, "48.5", "47.64", "48.18"));
		data.add(new TupleType(1273104000, "48.3201", "41.55", "46.57"));
		data.add(new TupleType(1273190400, "46.8", "44.2833", "45.41"));
		data.add(new TupleType(1273449600, "47.8", "47.31", "47.77"));
		data.add(new TupleType(1273536000, "48.39", "47.2", "47.72"));
		data.add(new TupleType(1273622400, "48.66", "47.9", "48.62"));
		data.add(new TupleType(1273708800, "48.79", "47.7301", "47.85"));

		// when
		for (TupleType r : data) {
			Tuple tuple = mock(Tuple.class);
			when(tuple.getStringByField("pair")).thenReturn("mypair");
			when(tuple.getStringByField("high")).thenReturn(r.high);
			when(tuple.getStringByField("low")).thenReturn(r.low);
			when(tuple.getStringByField("close")).thenReturn(r.close);
			when(tuple.getIntegerByField("timeslice")).thenReturn(r.ts);

			// send tuple to bolt
			bolt.execute(tuple);
		}

		// then
		verify(collector).emit(new Values("mypair", 1271808000, "0.56"));
		verify(collector).emit(new Values("mypair", 1271894400, "0.59"));
		verify(collector).emit(new Values("mypair", 1271980800, "0.59"));
		verify(collector).emit(new Values("mypair", 1272240000, "0.57"));
		verify(collector).emit(new Values("mypair", 1272326400, "0.62"));
		verify(collector).emit(new Values("mypair", 1272412800, "0.62"));
		verify(collector).emit(new Values("mypair", 1272499200, "0.64"));
		verify(collector).emit(new Values("mypair", 1272585600, "0.67"));
		verify(collector).emit(new Values("mypair", 1272844800, "0.69"));
		verify(collector).emit(new Values("mypair", 1272931200, "0.78"));
		verify(collector).emit(new Values("mypair", 1273017600, "0.78"));
		verify(collector).emit(new Values("mypair", 1273104000, "1.21"));
		verify(collector).emit(new Values("mypair", 1273190400, "1.30"));
		verify(collector).emit(new Values("mypair", 1273449600, "1.38"));
		verify(collector).emit(new Values("mypair", 1273536000, "1.37"));
		verify(collector).emit(new Values("mypair", 1273622400, "1.34"));
		verify(collector).emit(new Values("mypair", 1273708800, "1.32"));

	}

}
