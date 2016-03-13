package com.yan2.analytics.whatsapp_analytics;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * Hello world!
 *
 */
public class App {

	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
			"yyyyMMM' 'd' 'H:m", new Locale("ES"));

	public static final SimpleDateFormat HOUR_OF_DAY = new SimpleDateFormat("H");

	public static final SimpleDateFormat DAY_OF_WEEK = new SimpleDateFormat("u");

	public static final Pattern LOG_PATTERN = Pattern
			.compile("^(.*) - (.*): (.*)$");

	static {
		DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("America/Mexico"));
		HOUR_OF_DAY.setTimeZone(TimeZone.getTimeZone("America/Mexico"));
		DAY_OF_WEEK.setTimeZone(TimeZone.getTimeZone("America/Mexico"));
	}

	static final String ELASTIC_SERVER = "192.168.99.100";
	
	public static void main(String[] args) throws IOException {

		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "elasticsearch").build();

		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(ELASTIC_SERVER, 19300));

		System.out.println("Bulk load");
		BulkRequestBuilder bulkRequest = client.prepareBulk();

		File file = new File("Chat.txt");
		System.out.println("Reading file " + file.getAbsolutePath());

		BufferedReader br = new BufferedReader(new FileReader(file));

		String line = null;
		while (true) {
			line = br.readLine();

			if (line == null)
				break;

			System.out.println(line);

			Matcher m = LOG_PATTERN.matcher(line);

			if (m.matches()) {

				String dateStr = m.group(1);
				String user = m.group(2);
				String message = m.group(3);

				try {
					Date date = DATE_FORMAT.parse("2015" + dateStr);

					XContentBuilder json = jsonBuilder()
							.startObject()
							.field("post_date", date)
							.field("user", user)
							.field("message", message)
							.field("hour_of_day",
									Integer.parseInt(HOUR_OF_DAY.format(date)))
							.field("day_of_week",
									Integer.parseInt(DAY_OF_WEEK.format(date)))
							.endObject();

					System.out.println(json.string());

					bulkRequest.add(client.prepareIndex("chinitos", "whatsapp")
							.setSource(json));

				} catch (ParseException e) {

				}

			} else {
				System.out
						.println("IGNORING LINE BECAUSE IT DOESN'T MATCHES THE PATTERN"
								+ line);
			}

		}

		// System.out.println(client.prepareSearch().execute().actionGet());
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			// process failures by iterating through each bulk response item
			System.out.println("Failures!!!");
		}

		br.close();
		client.close();
	}
}
