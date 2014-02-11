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
 * Reference :  http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:commodity_channel_in
 */
public class CCITest {

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
	public void shouldEmitCCIValues() {

		// given
		CCIBolt bolt = new CCIBolt(20);
		Map conf = mock(Map.class);
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(conf, context, collector);

		List<TupleType> data = new ArrayList<TupleType>();
		data.add(new TupleType(1282608000, "24.2013", "23.8534", "23.8932"));
		data.add(new TupleType(1282694400, "24.0721", "23.7242", "23.9528"));
		data.add(new TupleType(1282780800, "24.0423", "23.6447", "23.6745"));
		data.add(new TupleType(1282867200, "23.8733", "23.3664", "23.7839"));
		data.add(new TupleType(1283126400, "23.6745", "23.4559", "23.4956"));
		data.add(new TupleType(1283212800, "23.5851", "23.1776", "23.3217"));
		data.add(new TupleType(1283299200, "23.8037", "23.3962", "23.7540"));
		data.add(new TupleType(1283385600, "23.8036", "23.5652", "23.7938"));
		data.add(new TupleType(1283472000, "24.3007", "24.0522", "24.1417"));
		data.add(new TupleType(1283817600, "24.1516", "23.7739", "23.8137"));
		data.add(new TupleType(1283904000, "24.0522", "23.5950", "23.7839"));
		data.add(new TupleType(1283990400, "24.0622", "23.8435", "23.8634"));
		data.add(new TupleType(1284076800, "23.8833", "23.6447", "23.7044"));
		data.add(new TupleType(1284336000, "25.1356", "23.9429", "24.9567"));
		data.add(new TupleType(1284422400, "25.1952", "24.7380", "24.8771"));
		data.add(new TupleType(1284508800, "25.0660", "24.7678", "24.9616"));
		data.add(new TupleType(1284595200, "25.2151", "24.8970", "25.1753"));
		data.add(new TupleType(1284681600, "25.3741", "24.9268", "25.0660"));
		data.add(new TupleType(1284940800, "25.3642", "24.9567", "25.2747"));
		data.add(new TupleType(1285027200, "25.2648", "24.9268", "24.9964"));
		data.add(new TupleType(1285113600, "24.8175", "24.2112", "24.4597"));
		data.add(new TupleType(1285200000, "24.4398", "24.2112", "24.2808"));
		data.add(new TupleType(1285286400, "24.6485", "24.4299", "24.6237"));
		data.add(new TupleType(1285545600, "24.8374", "24.4398", "24.5815"));
		data.add(new TupleType(1285632000, "24.7479", "24.2013", "24.5268"));
		data.add(new TupleType(1285718400, "24.5094", "24.2510", "24.3504"));
		data.add(new TupleType(1285804800, "24.6784", "24.2112", "24.3404"));
		data.add(new TupleType(1285891200, "24.6684", "24.1516", "24.2311"));
		data.add(new TupleType(1286150400, "23.8435", "23.6348", "23.7640"));
		data.add(new TupleType(1286236800, "24.3007", "23.7640", "24.2013"));

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
		verify(collector).emit(new Values("mypair", 1285027200, "102.31"));
		verify(collector).emit(new Values("mypair", 1285113600, "30.74"));
		verify(collector).emit(new Values("mypair", 1285200000, "6.55"));
		verify(collector).emit(new Values("mypair", 1285286400, "33.30"));
		verify(collector).emit(new Values("mypair", 1285545600, "34.95"));
		verify(collector).emit(new Values("mypair", 1285632000, "13.84"));
		verify(collector).emit(new Values("mypair", 1285718400, "-10.75"));
		verify(collector).emit(new Values("mypair", 1285804800, "-11.58"));
		verify(collector).emit(new Values("mypair", 1285891200, "-29.35"));
		verify(collector).emit(new Values("mypair", 1286150400, "-129.36"));
		verify(collector).emit(new Values("mypair", 1286236800, "-73.07"));

	}
}
