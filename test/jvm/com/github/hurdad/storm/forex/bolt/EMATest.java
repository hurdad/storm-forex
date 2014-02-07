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

	@Test
	public void shouldEmitEMAValues() {

		// given
		EMABolt bolt = new EMABolt(10, 5);
		Map conf = mock(Map.class);
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(conf, context, collector);

		List<TupleType> data = new ArrayList<TupleType>();
		data.add(new TupleType(1269388800, "22.2734"));
		data.add(new TupleType(1269475200, "22.1940"));
		data.add(new TupleType(1269561600, "22.0847"));
		data.add(new TupleType(1269820800, "22.1741"));
		data.add(new TupleType(1269907200, "22.1840"));
		data.add(new TupleType(1269993600, "22.1344"));
		data.add(new TupleType(1270080000, "22.2337"));
		data.add(new TupleType(1270425600, "22.4323"));
		data.add(new TupleType(1270512000, "22.2436"));
		data.add(new TupleType(1270598400, "22.2933"));
		data.add(new TupleType(1270684800, "22.1542"));
		data.add(new TupleType(1270771200, "22.3926"));
		data.add(new TupleType(1271030400, "22.3816"));
		data.add(new TupleType(1271116800, "22.6109"));
		data.add(new TupleType(1271203200, "23.3558"));
		data.add(new TupleType(1271289600, "24.0519"));
		data.add(new TupleType(1271376000, "23.7530"));
		data.add(new TupleType(1271635200, "23.8324"));
		data.add(new TupleType(1271721600, "23.9516"));
		data.add(new TupleType(1271808000, "23.6338"));
		data.add(new TupleType(1271894400, "23.8225"));
		data.add(new TupleType(1271980800, "23.8722"));
		data.add(new TupleType(1272240000, "23.6537"));
		data.add(new TupleType(1272326400, "23.1870"));
		data.add(new TupleType(1272412800, "23.0976"));
		data.add(new TupleType(1272499200, "23.3260"));
		data.add(new TupleType(1272585600, "22.6805"));
		data.add(new TupleType(1272844800, "23.0976"));
		data.add(new TupleType(1272931200, "22.4025"));
		data.add(new TupleType(1273017600, "22.1725"));

		// when
		for (TupleType r : data) {
			Tuple tuple = mock(Tuple.class);
			when(tuple.getStringByField("pair")).thenReturn("mypair");
			when(tuple.getStringByField("close")).thenReturn(r.close);
			when(tuple.getIntegerByField("timeslice")).thenReturn(r.ts);

			//send tuple to bolt
			bolt.execute(tuple);
		}

		// then
		verify(collector).emit(new Values("mypair", 1270598400, "22.22475"));
		verify(collector).emit(new Values("mypair", 1270684800, "22.21192"));
		verify(collector).emit(new Values("mypair", 1270771200, "22.24477"));
		verify(collector).emit(new Values("mypair", 1271030400, "22.26965"));
		verify(collector).emit(new Values("mypair", 1271116800, "22.33170"));
		verify(collector).emit(new Values("mypair", 1271203200, "22.51790"));
		verify(collector).emit(new Values("mypair", 1271289600, "22.79681"));
		verify(collector).emit(new Values("mypair", 1271376000, "22.97066"));
		verify(collector).emit(new Values("mypair", 1271635200, "23.12734"));
		verify(collector).emit(new Values("mypair", 1271721600, "23.27721"));
		verify(collector).emit(new Values("mypair", 1271808000, "23.34204"));
		verify(collector).emit(new Values("mypair", 1271894400, "23.42940"));
		verify(collector).emit(new Values("mypair", 1271980800, "23.50991"));
		verify(collector).emit(new Values("mypair", 1272240000, "23.53605"));
		verify(collector).emit(new Values("mypair", 1272326400, "23.47259"));
		verify(collector).emit(new Values("mypair", 1272412800, "23.40441"));
		verify(collector).emit(new Values("mypair", 1272499200, "23.39015"));
		verify(collector).emit(new Values("mypair", 1272585600, "23.26112"));
		verify(collector).emit(new Values("mypair", 1272844800, "23.23139"));
		verify(collector).emit(new Values("mypair", 1272931200, "23.08068"));
		verify(collector).emit(new Values("mypair", 1273017600, "22.91556"));

	}
}
