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
 * Reference :  http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:moving_average_conve
 *   			http://investexcel.net/how-to-calculate-macd-in-excel/
 */
public class MACDTest {

	class TupleType {
		public String close;
		public Integer ts;

		public TupleType(Integer ts, String close) {
			this.close = close;
			this.ts = ts;
		}
	}

	@Test
	public void shouldEmitMACDValues() {

		// given
		MACDBolt bolt = new MACDBolt(12, 26, 9);
		Map conf = mock(Map.class);
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(conf, context, collector);

		List<TupleType> data = new ArrayList<TupleType>();
		data.add(new TupleType(1361232000, "459.99"));
		data.add(new TupleType(1361318400, "448.85"));
		data.add(new TupleType(1361404800, "446.06"));
		data.add(new TupleType(1361491200, "450.81"));
		data.add(new TupleType(1361750400, "442.8"));
		data.add(new TupleType(1361836800, "448.97"));
		data.add(new TupleType(1361923200, "444.57"));
		data.add(new TupleType(1362009600, "441.4"));
		data.add(new TupleType(1362096000, "430.47"));
		data.add(new TupleType(1362355200, "420.05"));
		data.add(new TupleType(1362441600, "431.14"));
		data.add(new TupleType(1362528000, "425.66"));
		data.add(new TupleType(1362614400, "430.58"));
		data.add(new TupleType(1362700800, "431.72"));
		data.add(new TupleType(1362960000, "437.87"));
		data.add(new TupleType(1363046400, "428.43"));
		data.add(new TupleType(1363132800, "428.35"));
		data.add(new TupleType(1363219200, "432.5"));
		data.add(new TupleType(1363305600, "443.66"));
		data.add(new TupleType(1363564800, "455.72"));
		data.add(new TupleType(1363651200, "454.49"));
		data.add(new TupleType(1363737600, "452.08"));
		data.add(new TupleType(1363824000, "452.73"));
		data.add(new TupleType(1363910400, "461.91"));
		data.add(new TupleType(1364169600, "463.58"));
		data.add(new TupleType(1364256000, "461.14"));
		data.add(new TupleType(1364342400, "452.08"));
		data.add(new TupleType(1364428800, "442.66"));
		data.add(new TupleType(1364774400, "428.91"));
		data.add(new TupleType(1364860800, "429.79"));
		data.add(new TupleType(1364947200, "431.99"));
		data.add(new TupleType(1365033600, "427.72"));
		data.add(new TupleType(1365120000, "423.2"));
		data.add(new TupleType(1365379200, "426.21"));
		data.add(new TupleType(1365465600, "426.98"));
		data.add(new TupleType(1365552000, "435.69"));
		data.add(new TupleType(1365638400, "434.33"));
		data.add(new TupleType(1365724800, "429.8"));
		data.add(new TupleType(1365984000, "419.85"));
		data.add(new TupleType(1366070400, "426.24"));
		data.add(new TupleType(1366156800, "402.8"));
		data.add(new TupleType(1366243200, "392.05"));
		data.add(new TupleType(1366329600, "390.53"));
		data.add(new TupleType(1366588800, "398.67"));
		data.add(new TupleType(1366675200, "406.13"));
		data.add(new TupleType(1366761600, "405.46"));
		data.add(new TupleType(1366848000, "408.38"));
		data.add(new TupleType(1366934400, "417.2"));
		data.add(new TupleType(1367193600, "430.12"));
		data.add(new TupleType(1367280000, "442.78"));
		data.add(new TupleType(1367366400, "439.29"));
		data.add(new TupleType(1367452800, "445.52"));
		data.add(new TupleType(1367539200, "449.98"));
		data.add(new TupleType(1367798400, "460.71"));
		data.add(new TupleType(1367884800, "458.66"));
		data.add(new TupleType(1367971200, "463.84"));
		data.add(new TupleType(1368057600, "456.77"));
		data.add(new TupleType(1368144000, "452.97"));
		data.add(new TupleType(1368403200, "454.74"));
		data.add(new TupleType(1368489600, "443.86"));
		data.add(new TupleType(1368576000, "428.85"));
		data.add(new TupleType(1368662400, "434.58"));
		data.add(new TupleType(1368748800, "433.26"));
		data.add(new TupleType(1369008000, "442.93"));
		data.add(new TupleType(1369094400, "439.66"));
		data.add(new TupleType(1369180800, "441.35"));

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
		verify(collector).emit(new Values("mypair", 1365379200, "-2.071", "3.038", "-5.108"));
		verify(collector).emit(new Values("mypair", 1365465600, "-2.622", "1.906", "-4.527"));
		verify(collector).emit(new Values("mypair", 1365552000, "-2.329", "1.059", "-3.388"));
		verify(collector).emit(new Values("mypair", 1365638400, "-2.182", "0.411", "-2.592"));
		verify(collector).emit(new Values("mypair", 1365724800, "-2.403", "-0.152", "-2.251"));
		verify(collector).emit(new Values("mypair", 1365984000, "-3.342", "-0.790", "-2.552"));
		verify(collector).emit(new Values("mypair", 1366070400, "-3.530", "-1.338", "-2.192"));
		verify(collector).emit(new Values("mypair", 1366156800, "-5.507", "-2.172", "-3.335"));
		verify(collector).emit(new Values("mypair", 1366243200, "-7.851", "-3.308", "-4.543"));
		verify(collector).emit(new Values("mypair", 1366329600, "-9.719", "-4.590", "-5.129"));
		verify(collector).emit(new Values("mypair", 1366588800, "-10.423", "-5.757", "-4.666"));
		verify(collector).emit(new Values("mypair", 1366675200, "-10.260", "-6.657", "-3.603"));
		verify(collector).emit(new Values("mypair", 1366761600, "-10.069", "-7.340", "-2.729"));
		verify(collector).emit(new Values("mypair", 1366848000, "-9.572", "-7.786", "-1.786"));
		verify(collector).emit(new Values("mypair", 1366934400, "-8.370", "-7.903", "-0.467"));
		verify(collector).emit(new Values("mypair", 1367193600, "-6.302", "-7.583", "1.281"));
		verify(collector).emit(new Values("mypair", 1367280000, "-3.600", "-6.786", "3.186"));
		verify(collector).emit(new Values("mypair", 1367366400, "-1.720", "-5.773", "4.053"));
		verify(collector).emit(new Values("mypair", 1367452800, "0.269", "-4.564", "4.833"));
		verify(collector).emit(new Values("mypair", 1367539200, "2.180", "-3.216", "5.396"));
		verify(collector).emit(new Values("mypair", 1367798400, "4.509", "-1.671", "6.179"));
		verify(collector).emit(new Values("mypair", 1367884800, "6.118", "-0.113", "6.231"));
		verify(collector).emit(new Values("mypair", 1367971200, "7.722", "1.454", "6.268"));
		verify(collector).emit(new Values("mypair", 1368057600, "8.327", "2.829", "5.499"));
		verify(collector).emit(new Values("mypair", 1368144000, "8.403", "3.944", "4.460"));
		verify(collector).emit(new Values("mypair", 1368403200, "8.508", "4.857", "3.652"));
		verify(collector).emit(new Values("mypair", 1368489600, "7.626", "5.410", "2.215"));
		verify(collector).emit(new Values("mypair", 1368576000, "5.650", "5.458", "0.192"));
		verify(collector).emit(new Values("mypair", 1368662400, "4.495", "5.266", "-0.771"));
		verify(collector).emit(new Values("mypair", 1368748800, "3.433", "4.899", "-1.466"));
		verify(collector).emit(new Values("mypair", 1369008000, "3.333", "4.586", "-1.252"));
		verify(collector).emit(new Values("mypair", 1369094400, "2.957", "4.260", "-1.303"));
		verify(collector).emit(new Values("mypair", 1369180800, "2.763", "3.961", "-1.198"));

	}

}
