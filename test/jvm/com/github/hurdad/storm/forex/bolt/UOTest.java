package com.github.hurdad.storm.forex.bolt;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import com.github.hurdad.storm.forex.bolt.PercRTest.TupleType;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * Reference : http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:ultimate_oscillator
 */
public class UOTest {

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
	public void shouldEmitUOValues() {

		// given
		UOBolt bolt = new UOBolt(7, 14, 28);
		Map conf = mock(Map.class);
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(conf, context, collector);

		List<TupleType> data = new ArrayList<TupleType>();
		data.add(new TupleType(1287532800, "57.9342", "56.5199", "57.5657"));
		data.add(new TupleType(1287619200, "58.4620", "57.0677", "57.6653"));
		data.add(new TupleType(1287705600, "57.7649", "56.4403", "56.9183"));
		data.add(new TupleType(1287964800, "59.8763", "57.5258", "58.4720"));
		data.add(new TupleType(1288051200, "59.0198", "57.5756", "58.7409"));
		data.add(new TupleType(1288137600, "60.1751", "57.8943", "60.0058"));
		data.add(new TupleType(1288224000, "60.2946", "58.0139", "58.4521"));
		data.add(new TupleType(1288310400, "59.8564", "58.4322", "59.1791"));
		data.add(new TupleType(1288569600, "59.7767", "58.4521", "58.6712"));
		data.add(new TupleType(1288656000, "59.7269", "58.5816", "58.8704"));
		data.add(new TupleType(1288742400, "59.5994", "58.5417", "59.2986"));
		data.add(new TupleType(1288828800, "62.9637", "59.6173", "62.5653"));
		data.add(new TupleType(1288915200, "62.2666", "61.3602", "62.0176"));
		data.add(new TupleType(1289174400, "63.0633", "61.2507", "62.0474"));
		data.add(new TupleType(1289260800, "63.7406", "62.1869", "62.5155"));
		data.add(new TupleType(1289347200, "62.7446", "61.0216", "62.3661"));
		data.add(new TupleType(1289433600, "63.4816", "61.5694", "63.4019"));
		data.add(new TupleType(1289520000, "63.2326", "60.7926", "61.8981"));
		data.add(new TupleType(1289779200, "62.1371", "60.3444", "60.5436"));
		data.add(new TupleType(1289865600, "60.5037", "58.2031", "59.0895"));
		data.add(new TupleType(1289952000, "59.8862", "58.9102", "59.0098"));
		data.add(new TupleType(1290038400, "60.3245", "59.0905", "59.3883"));
		data.add(new TupleType(1290124800, "59.7070", "58.5915", "59.2090"));
		data.add(new TupleType(1290384000, "62.2168", "59.4401", "59.6572"));
		data.add(new TupleType(1290470400, "59.7368", "57.3267", "59.0696"));
		data.add(new TupleType(1290556800, "59.9360", "59.1094", "59.8962"));
		data.add(new TupleType(1290729600, "59.6470", "58.8714", "59.2887"));
		data.add(new TupleType(1290988800, "59.3683", "58.2429", "59.1194"));
		data.add(new TupleType(1291075200, "60.2149", "58.2628", "59.6771"));
		data.add(new TupleType(1291161600, "61.6989", "60.5834", "61.4798"));

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
		verify(collector).emit(new Values("mypair", 1291075200, "53.40"));
		verify(collector).emit(new Values("mypair", 1291161600, "57.16"));
	}
}
