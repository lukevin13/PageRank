package edu.upenn.cis455.mapreduce.job;

import java.util.ArrayList;
import java.util.List;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class PageRankJob implements Job {

	double alpha = 0.85;

	// INPUT = key : [page URL:rank], value : comma separated list of out links
	@Override
	public void map(String key, String value, Context context) {
		String page = getPage(key);
		double rank = getRank(key);
		List<String> links = getLinks(value);
		for (String link : links) {
			context.write(link, "" + rank/links.size());
		}
		context.write(page, "->" + value);
	}

	// INPUT = key : [page URL], values : list of [inlinks:pagerank], out links
	@Override
	public void reduce(String key, String[] values, Context context) {
		String links = "";
		double rankK = 0;
		for (int i = 0; i < values.length - 1; i++) {
			if (values[i] != null && !values[i].isEmpty()) {
				if (values[i].startsWith("->")) {
					links = values[i].substring(2).trim();
				} else {
					rankK += Double.parseDouble(values[i]);
				}
			}
		}
		rankK = (1-this.alpha) + rankK * this.alpha;
		context.write(key + ":" + rankK, links);

	}

	// Forms List<String> from a comma separated string
	private List<String> getLinks(String list) {
		List<String> links = new ArrayList<String>();

		String[] parsedList = list.split(",");
		for (String link : parsedList) {
			links.add(link);
		}
		return links;
	}

	// Gets the rank of a URL from a key
	private double getRank(String key) {
		String[] parsed = key.split(":",2);
		return Double.parseDouble(parsed[1]);
	}

	// Gets the page URL from a key
	private String getPage(String key) {
		String[] parsed = key.split(":",2);
		return parsed[0];
	}

}
