package com.github.hurdad.storm.forex.spout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TrueFXSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	Integer wait;
	HttpURLConnection connection = null;
	BufferedReader rd = null;
	String line = null;
	URL serverAddress = null;

	public TrueFXSpout(Integer wait_ms) {
		wait = wait_ms;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void nextTuple() {

		String url = "http://webrates.truefx.com/rates/connect.html";
		String data = "";

		try {
			serverAddress = new URL(url);
			connection = null;
			connection = (HttpURLConnection) serverAddress.openConnection();
			connection.setRequestMethod("GET");
		//	connection.setReadTimeout(500);
			connection.connect();
			rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			data = rd.readLine();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (ProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			connection.disconnect();
			rd = null;
			connection = null;
		}
		
		if(data.equals(""))
			return;

		Integer pairCount = 0;
		String valueStr = data;
		Integer index = valueStr.indexOf("/");

		while (index > -1) {

			pairCount++;
			valueStr = valueStr.substring(index + 1);
			index = valueStr.indexOf("/");

		}
		if (pairCount > 0) {
			valueStr = data;
			
			List<String> pairs = parseIt(valueStr, 7, pairCount);

			valueStr = valueStr.substring(pairCount * 7);

			List<String> bidBigNumber = parseIt(valueStr, 4, pairCount);
			valueStr = valueStr.substring(pairCount * 4);

			List<String> bidPoints = parseIt(valueStr, 3, pairCount);
			valueStr = valueStr.substring(pairCount * 3);

			List<String> offerBigNumber = parseIt(valueStr, 4, pairCount);
			valueStr = valueStr.substring(pairCount * 4);

			List<String> offerPoints = parseIt(valueStr, 3, pairCount);
			valueStr = valueStr.substring(pairCount * 3);

			List<String> highs = parseIt(valueStr, 7, pairCount);
			valueStr = valueStr.substring(pairCount * 7);
			List<String> lows = parseIt(valueStr, 7, pairCount);
			valueStr = valueStr.substring(pairCount * 7);

			List<String> msTime = parseIt(valueStr, 13, pairCount);
			valueStr = valueStr.substring(pairCount * 13);

			// loop pairs
			for (Integer i = 0; i < pairs.size(); i++) {

				String pair = pairs.get(i);
				String bid = bidBigNumber.get(i).replace("#", "") +  bidPoints.get(i);
				String offer = offerBigNumber.get(i).replace("#", "") + offerPoints.get(i);
				String time = msTime.get(i);
				
				//emit
				_collector.emit(new Values(pair, Double.parseDouble(bid), Double.parseDouble(offer), Long.parseLong(time)));
			}
		}
	
		Utils.sleep(wait);// wait
	}

	private List<String> parseIt(String valueStr, Integer tokenLength, Integer tokenCount) {

		Integer start = 0;
		Integer end = start + tokenLength;
		List<String> tokens = new ArrayList<String>();
		for (Integer index = 0; index < tokenCount; index++) {
			tokens.add(valueStr.substring(start, start + tokenLength));
			start = end;
			end = start + tokenLength;
		}
		return tokens;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pair", "bid", "offer", "timestamp"));
	}

}
