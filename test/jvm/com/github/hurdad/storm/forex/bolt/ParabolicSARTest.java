package com.github.hurdad.storm.forex.bolt;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * Reference : http://stockcharts.com/help/doku.php?id=chart_school:technical_indicators:parabolic_sar
 */
public class ParabolicSARTest {

	class TupleType {
		public String open;
		public String high;
		public String low;
		public String close;
		public Integer ts;

		public TupleType(Integer ts, String open, String high, String low, String close) {
			this.open = open;
			this.high = high;
			this.low = low;
			this.close = close;
			this.ts = ts;
		}
	}

	@Test
	public void shouldEmitParabolicSARValues() {

		// given
		ParabolicSARBolt bolt = new ParabolicSARBolt(new BigDecimal("0.02"), new BigDecimal("0.20"));
		Map conf = mock(Map.class);
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(conf, context, collector);

		List<TupleType> data = new ArrayList<TupleType>();
		data.add(new TupleType(432259200,"10.3000001907","10.3299999237","10.1099996567","10.1599998474"));
		data.add(new TupleType(432345600,"9.9499998093","9.9799995422","9.7600002289","9.8000001907"));
		data.add(new TupleType(432432000,"9.75","9.8299999237","9.6400003433","9.7600002289"));
		data.add(new TupleType(432518400,"9.5500001907","9.6000003815","9.3500003815","9.4099998474"));
		data.add(new TupleType(432777600,"9.5500001907","9.8500003815","9.5100002289","9.8500003815"));
		data.add(new TupleType(432864000,"9.7700004578","9.8000001907","9.25","9.3299999237"));
		data.add(new TupleType(432950400,"9.0900001526","9.1899995804","9","9.0600004196"));
		data.add(new TupleType(433036800,"9.1599998474","9.4099998474","9.1599998474","9.3999996185"));
		data.add(new TupleType(433123200,"9.3999996185","9.470000267","9.0799999237","9.220000267"));
		data.add(new TupleType(433382400,"8.9499998093","9.3199996948","8.9499998093","9.3100004196"));
		data.add(new TupleType(433468800,"9.6400003433","9.9399995804","9.5500001907","9.7600002289"));
		data.add(new TupleType(433555200,"9.8299999237","10.2299995422","9.8299999237","10"));
		data.add(new TupleType(433641600,"10","10.1099996567","9.5600004196","9.5900001526"));
		data.add(new TupleType(433728000,"9.4600000381","10.5","9.4600000381","10.3999996185"));
		data.add(new TupleType(433987200,"10.8800001144","11.25","10.8199996948","11.2299995422"));
		data.add(new TupleType(434073600,"11.3000001907","11.5699996948","11.3000001907","11.4399995804"));
		data.add(new TupleType(434160000,"11.4899997711","11.5500001907","11.2100000381","11.4399995804"));
		data.add(new TupleType(434246400,"11.4200000763","11.8000001907","11.2899999619","11.779999733"));
		data.add(new TupleType(434332800,"11.779999733","11.8999996185","11.6700000763","11.8800001144"));
		data.add(new TupleType(434592000,"11.8500003815","11.9399995804","11.6199998856","11.6700000763"));
		data.add(new TupleType(434678400,"11.5","11.5900001526","11.3199996948","11.3299999237"));
		data.add(new TupleType(434764800,"11.220000267","11.4300003052","11.0500001907","11.0500001907"));
		data.add(new TupleType(434851200,"10.9499998093","11.2299995422","10.8699998856","11.0900001526"));
		data.add(new TupleType(434937600,"11.220000267","11.3699998856","11.1099996567","11.3500003815"));
		data.add(new TupleType(435196800,"11.1999998093","11.3400001526","11.1199998856","11.2700004578"));
		data.add(new TupleType(435283200,"11.0799999237","11.2700004578","10.9600000381","11"));
		data.add(new TupleType(435369600,"10.8599996567","10.9399995804","10.75","10.7600002289"));
		data.add(new TupleType(435456000,"10.6800003052","10.7600002289","10.529999733","10.5399999619"));
		data.add(new TupleType(435542400,"10.6199998856","10.6899995804","10.5500001907","10.6800003052"));
		data.add(new TupleType(435801600,"10.6599998474","10.779999733","10.0500001907","10.0900001526"));
		data.add(new TupleType(435888000,"9.9499998093","10.0200004578","9.7700004578","9.8900003433"));
		data.add(new TupleType(435974400,"9.9499998093","10.0600004196","9.8199996948","10.0399999619"));
		data.add(new TupleType(436060800,"9.75","9.8000001907","9.4799995422","9.6300001144"));
		data.add(new TupleType(436147200,"9.6800003052","9.75","9.6400003433","9.6599998474"));
		data.add(new TupleType(436406400,"9.4200000763","9.5","9.3299999237","9.3599996567"));
		data.add(new TupleType(436492800,"9.279999733","9.3900003433","9.1300001144","9.3699998856"));
		data.add(new TupleType(436579200,"9.3999996185","9.5399999619","9.0600004196","9.1000003815"));
		data.add(new TupleType(436665600,"8.970000267","9.4499998093","8.970000267","9.4300003052"));
		data.add(new TupleType(436752000,"9.3699998856","9.6499996185","9.2700004578","9.5200004578"));
		data.add(new TupleType(437011200,"9.779999733","9.9600000381","9.7600002289","9.8100004196"));
		data.add(new TupleType(437097600,"9.9600000381","10.0200004578","9.8699998856","9.9099998474"));
		data.add(new TupleType(437184000,"9.6499996185","9.8199996948","9.6199998856","9.7600002289"));
		data.add(new TupleType(437270400,"9.6599998474","10.0799999237","9.6599998474","9.9600000381"));
		data.add(new TupleType(437356800,"9.9799995422","10.0399999619","9.25","9.2600002289"));
		data.add(new TupleType(437616000,"9.1300001144","9.3999996185","9.1300001144","9.3999996185"));
		data.add(new TupleType(437702400,"9.3000001907","9.470000267","9.1999998093","9.220000267"));
		data.add(new TupleType(437788800,"9.1899995804","9.3400001526","9.1199998856","9.1999998093"));
		data.add(new TupleType(437875200,"9.0500001907","9.3999996185","8.8199996948","9.3699998856"));
		data.add(new TupleType(437961600,"9.3199996948","9.4499998093","9.279999733","9.3500003815"));
		data.add(new TupleType(438220800,"9.2700004578","10.1000003815","9.2700004578","10.0799999237"));

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
		verify(collector).emit(new Values("mypair",432777600, "9.35000"));
		verify(collector).emit(new Values("mypair",432864000, "9.35000"));
		verify(collector).emit(new Values("mypair",432950400, "10.33000"));
		verify(collector).emit(new Values("mypair",433036800, "10.27680"));
		verify(collector).emit(new Values("mypair",433123200, "10.22573"));
		verify(collector).emit(new Values("mypair",433382400, "10.17670"));
		verify(collector).emit(new Values("mypair",433468800, "10.10310"));
		verify(collector).emit(new Values("mypair",433555200, "10.03391"));
		verify(collector).emit(new Values("mypair",433641600, "8.95000"));
		verify(collector).emit(new Values("mypair",433728000, "8.99640"));
		verify(collector).emit(new Values("mypair",433987200, "9.08662"));
		verify(collector).emit(new Values("mypair",434073600, "9.25969"));
		verify(collector).emit(new Values("mypair",434160000, "9.49072"));
		verify(collector).emit(new Values("mypair",434246400, "9.69865"));
		verify(collector).emit(new Values("mypair",434332800, "9.95081"));
		verify(collector).emit(new Values("mypair",434592000, "10.22370"));
		verify(collector).emit(new Values("mypair",434678400, "10.49830"));
		verify(collector).emit(new Values("mypair",434764800, "10.72898"));
		verify(collector).emit(new Values("mypair",434851200, "10.92274"));
		verify(collector).emit(new Values("mypair",434937600, "11.94000"));
		verify(collector).emit(new Values("mypair",435196800, "11.90680"));
		verify(collector).emit(new Values("mypair",435283200, "11.87493"));
		verify(collector).emit(new Values("mypair",435369600, "11.82003"));
		verify(collector).emit(new Values("mypair",435456000, "11.73443"));
		verify(collector).emit(new Values("mypair",435542400, "11.61399"));
		verify(collector).emit(new Values("mypair",435801600, "11.50559"));
		verify(collector).emit(new Values("mypair",435888000, "11.33092"));
		verify(collector).emit(new Values("mypair",435974400, "11.11239"));
		verify(collector).emit(new Values("mypair",436060800, "10.92445"));
		verify(collector).emit(new Values("mypair",436147200, "10.69334"));
		verify(collector).emit(new Values("mypair",436406400, "10.49921"));
		verify(collector).emit(new Values("mypair",436492800, "10.28875"));
		verify(collector).emit(new Values("mypair",436579200, "10.05700"));
		verify(collector).emit(new Values("mypair",436665600, "9.85760"));
		verify(collector).emit(new Values("mypair",436752000, "9.68008"));
		verify(collector).emit(new Values("mypair",437011200, "9.65000"));
		verify(collector).emit(new Values("mypair",437097600, "8.97000"));
		verify(collector).emit(new Values("mypair",437184000, "9.01200"));
		verify(collector).emit(new Values("mypair",437270400, "9.05232"));
		verify(collector).emit(new Values("mypair",437356800, "9.11398"));
		verify(collector).emit(new Values("mypair",437616000, "9.17194"));
		verify(collector).emit(new Values("mypair",437702400, "10.08000"));
		verify(collector).emit(new Values("mypair",437788800, "10.04480"));
		verify(collector).emit(new Values("mypair",437875200, "9.98931"));
		verify(collector).emit(new Values("mypair",437961600, "9.89577"));
		verify(collector).emit(new Values("mypair",438220800, "9.80971"));	

	}
}
