package com.github.hurdad.storm.forex.bolt;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.mockito.ArgumentMatcher;
import org.testng.annotations.Test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.github.hurdad.storm.forex.bolt.EMATest.TupleType;

/*
 * Reference : http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:moving_averages
 */
public class EMATest {

	class TupleType {
		public String close;
		public Integer ts;

		public TupleType(Integer ts, String close) {
			this.close = close;
			this.ts = ts;
		}
	}

	class IsMyList extends ArgumentMatcher<List> {
		public boolean matches(Object o) {

			if (o instanceof List) {
				List<Object> list = (List<Object>) o;
				if (list.size() != 3)
					return false;
				if (!list.get(0).equals("mypair"))
					return false;
				if (!list.get(1).equals(1270598400))
					return false;
				if (!list.get(2).equals("22.23"))
					return false;
			}
			return true;
		}
	}

	@Test
	public void shouldEmitEMAValues() {

		// given
		EMABolt bolt = new EMABolt(10);
		Map conf = mock(Map.class);
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(conf, context, collector);

		List<TupleType> data = new ArrayList<TupleType>();

		data.add(new TupleType(1269388800, "22.27"));
		data.add(new TupleType(1269475200, "22.19"));
		data.add(new TupleType(1269561600, "22.08"));
		data.add(new TupleType(1269820800, "22.17"));
		data.add(new TupleType(1269907200, "22.18"));
		data.add(new TupleType(1269993600, "22.13"));
		data.add(new TupleType(1270080000, "22.23"));
		data.add(new TupleType(1270425600, "22.43"));
		data.add(new TupleType(1270512000, "22.24"));
		data.add(new TupleType(1270598400, "22.29"));

		data.add(new TupleType(1270684800, "22.15"));
		data.add(new TupleType(1270771200, "22.39"));
		data.add(new TupleType(1271030400, "22.38"));
		data.add(new TupleType(1271116800, "22.61"));
		data.add(new TupleType(1271203200, "23.36"));
		data.add(new TupleType(1271289600, "24.05"));
		data.add(new TupleType(1271376000, "23.75"));
		data.add(new TupleType(1271635200, "23.83"));
		data.add(new TupleType(1271721600, "23.95"));
		data.add(new TupleType(1271808000, "23.63"));
		data.add(new TupleType(1271894400, "23.82"));
		data.add(new TupleType(1271980800, "23.87"));
		data.add(new TupleType(1272240000, "23.65"));
		data.add(new TupleType(1272326400, "23.19"));
		data.add(new TupleType(1272412800, "23.10"));
		data.add(new TupleType(1272499200, "23.33"));
		data.add(new TupleType(1272585600, "22.68"));
		data.add(new TupleType(1272844800, "23.10"));
		data.add(new TupleType(1272931200, "22.40"));
		data.add(new TupleType(1273017600, "22.17"));

		// when
		for (TupleType r : data) {
			Tuple tuple = mock(Tuple.class);
			when(tuple.getStringByField("pair")).thenReturn("mypair");
			when(tuple.getStringByField("close")).thenReturn(r.close);
			when(tuple.getIntegerByField("timeslice")).thenReturn(r.ts);

			bolt.execute(tuple);
		}

		// then

		verify(collector).emit(new Values("mypair", 1270598400, "22.22"));
		// verify(collector).emit(argThat(new IsMyList()));

		verify(collector).emit(new Values("mypair", 1270684800, "22.21"));
		verify(collector).emit(new Values("mypair", 1270771200, "22.24"));
		verify(collector).emit(new Values("mypair", 1271030400, "22.27"));
		verify(collector).emit(new Values("mypair", 1271116800, "22.33"));
		verify(collector).emit(new Values("mypair", 1271203200, "22.52"));
		verify(collector).emit(new Values("mypair", 1271289600, "22.80"));
		verify(collector).emit(new Values("mypair", 1271376000, "22.97"));
		verify(collector).emit(new Values("mypair", 1271635200, "23.13"));
		verify(collector).emit(new Values("mypair", 1271721600, "23.28"));
		verify(collector).emit(new Values("mypair", 1271808000, "23.34"));
		verify(collector).emit(new Values("mypair", 1271894400, "23.43"));
		verify(collector).emit(new Values("mypair", 1271980800, "23.51"));
		verify(collector).emit(new Values("mypair", 1272240000, "23.54"));
		verify(collector).emit(new Values("mypair", 1272326400, "23.47"));
		verify(collector).emit(new Values("mypair", 1272412800, "23.40"));
		verify(collector).emit(new Values("mypair", 1272499200, "23.39"));
		verify(collector).emit(new Values("mypair", 1272585600, "23.26"));
		verify(collector).emit(new Values("mypair", 1272844800, "23.23"));
		verify(collector).emit(new Values("mypair", 1272931200, "23.08"));
		verify(collector).emit(new Values("mypair", 1273017600, "22.92"));

	}
}
