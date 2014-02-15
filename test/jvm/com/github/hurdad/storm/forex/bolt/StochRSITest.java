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
 * Reference : http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:stochrsi
 */
public class StochRSITest {

	class TupleType {
		public String close;
		public Integer ts;

		public TupleType(Integer ts, String close) {
			this.close = close;
			this.ts = ts;
		}
	}

	@Test
	public void shouldEmitStochRSIValues() {

		// given
		StochRSIBolt bolt = new StochRSIBolt(14);
		Map conf = mock(Map.class);
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(conf, context, collector);

		List<TupleType> data = new ArrayList<TupleType>();

		data.add(new TupleType(1260748800, "44.3389"));
		data.add(new TupleType(1260835200, "44.0902"));
		data.add(new TupleType(1260921600, "44.1497"));
		data.add(new TupleType(1261008000, "43.6124"));
		data.add(new TupleType(1261094400, "44.3278"));
		data.add(new TupleType(1261353600, "44.8264"));
		data.add(new TupleType(1261440000, "45.0955"));
		data.add(new TupleType(1261526400, "45.4245"));
		data.add(new TupleType(1261612800, "45.8433"));
		data.add(new TupleType(1261958400, "46.0826"));
		data.add(new TupleType(1262044800, "45.8931"));
		data.add(new TupleType(1262131200, "46.0328"));
		data.add(new TupleType(1262217600, "45.6140"));
		data.add(new TupleType(1262563200, "46.2820"));
		data.add(new TupleType(1262649600, "46.2820"));
		data.add(new TupleType(1262736000, "46.0028"));
		data.add(new TupleType(1262822400, "46.0328"));
		data.add(new TupleType(1262908800, "46.4116"));
		data.add(new TupleType(1263168000, "46.2222"));
		data.add(new TupleType(1263254400, "45.6439"));
		data.add(new TupleType(1263340800, "46.2122"));
		data.add(new TupleType(1263427200, "46.2521"));
		data.add(new TupleType(1263513600, "45.7137"));
		data.add(new TupleType(1263859200, "46.4515"));
		data.add(new TupleType(1263945600, "45.7835"));
		data.add(new TupleType(1264032000, "45.3548"));
		data.add(new TupleType(1264118400, "44.0288"));
		data.add(new TupleType(1264377600, "44.1783"));
		data.add(new TupleType(1264464000, "44.2181"));
		data.add(new TupleType(1264550400, "44.5672"));
		data.add(new TupleType(1264636800, "43.4205"));
		data.add(new TupleType(1264723200, "42.6628"));
		data.add(new TupleType(1264982400, "43.1314"));

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
		verify(collector).emit(new Values("mypair", 1264377600, "0.05"));
		verify(collector).emit(new Values("mypair", 1264464000, "0.06"));
		verify(collector).emit(new Values("mypair", 1264550400, "0.19"));
		verify(collector).emit(new Values("mypair", 1264636800, "0.00"));
		verify(collector).emit(new Values("mypair", 1264723200, "0.00"));
		verify(collector).emit(new Values("mypair", 1264982400, "0.16"));

	}

}
