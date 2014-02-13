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
 * Reference :  http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:stochastic_oscillato
 */
public class StochTest {

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
	public void shouldEmitStochValues() {

		// given
		StochBolt bolt = new StochBolt(14, 3);
		Map conf = mock(Map.class);
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(conf, context, collector);

		List<TupleType> data = new ArrayList<TupleType>();
		data.add(new TupleType(1266883200, "127.0090", "125.3574", ""));
		data.add(new TupleType(1266969600, "127.6159", "126.1633", ""));
		data.add(new TupleType(1267056000, "126.5911", "124.9296", ""));
		data.add(new TupleType(1267142400, "127.3472", "126.0937", ""));
		data.add(new TupleType(1267401600, "128.1730", "126.8199", ""));
		data.add(new TupleType(1267488000, "128.4317", "126.4817", ""));
		data.add(new TupleType(1267574400, "127.3671", "126.0340", ""));
		data.add(new TupleType(1267660800, "126.4220", "124.8301", ""));
		data.add(new TupleType(1267747200, "126.8995", "126.3921", ""));
		data.add(new TupleType(1268006400, "126.8498", "125.7156", ""));
		data.add(new TupleType(1268092800, "125.6460", "124.5615", ""));
		data.add(new TupleType(1268179200, "125.7156", "124.5715", ""));
		data.add(new TupleType(1268265600, "127.1582", "125.0689", ""));
		data.add(new TupleType(1268352000, "127.7154", "126.8597", "127.2876"));
		data.add(new TupleType(1268611200, "127.6855", "126.6309", "127.1781"));
		data.add(new TupleType(1268697600, "128.2228", "126.8001", "128.0138"));
		data.add(new TupleType(1268784000, "128.2725", "126.7105", "127.1085"));
		data.add(new TupleType(1268870400, "128.0934", "126.8001", "127.7253"));
		data.add(new TupleType(1268956800, "128.2725", "126.1335", "127.0587"));
		data.add(new TupleType(1269216000, "127.7353", "125.9245", "127.3273"));
		data.add(new TupleType(1269302400, "128.7700", "126.9891", "128.7103"));
		data.add(new TupleType(1269388800, "129.2873", "127.8148", "127.8745"));
		data.add(new TupleType(1269475200, "130.0633", "128.4715", "128.5809"));
		data.add(new TupleType(1269561600, "129.1182", "128.0641", "128.6008"));
		data.add(new TupleType(1269820800, "129.2873", "127.6059", "127.9342"));
		data.add(new TupleType(1269907200, "128.4715", "127.5960", "128.1133"));
		data.add(new TupleType(1269993600, "128.0934", "126.9990", "127.5960"));
		data.add(new TupleType(1270080000, "128.6506", "126.8995", "127.5960"));
		data.add(new TupleType(1270425600, "129.1381", "127.4865", "128.6904"));
		data.add(new TupleType(1270512000, "128.6406", "127.3970", "128.2725"));

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
		verify(collector).emit(new Values("mypair", 1268697600, "89.20", "75.75"));
		verify(collector).emit(new Values("mypair", 1268784000, "65.81", "74.21"));
		verify(collector).emit(new Values("mypair", 1268870400, "81.75", "78.92"));
		verify(collector).emit(new Values("mypair", 1268956800, "64.52", "70.69"));
		verify(collector).emit(new Values("mypair", 1269216000, "74.53", "73.60"));
		verify(collector).emit(new Values("mypair", 1269302400, "98.58", "79.21"));
		verify(collector).emit(new Values("mypair", 1269388800, "70.10", "81.07"));
		verify(collector).emit(new Values("mypair", 1269475200, "73.06", "80.58"));
		verify(collector).emit(new Values("mypair", 1269561600, "73.42", "72.19"));
		verify(collector).emit(new Values("mypair", 1269820800, "61.23", "69.24"));
		verify(collector).emit(new Values("mypair", 1269907200, "60.96", "65.20"));
		verify(collector).emit(new Values("mypair", 1269993600, "40.39", "54.19"));
		verify(collector).emit(new Values("mypair", 1270080000, "40.39", "47.24"));
		verify(collector).emit(new Values("mypair", 1270425600, "66.83", "49.20"));
		verify(collector).emit(new Values("mypair", 1270512000, "56.73", "54.65"));

	}
}
