package com.timgroup.statsd;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Locale;

/** */
public class StatsEvent {
  /**
   * Because NumberFormat is not thread-safe we cannot share instances across threads. Use a
   * ThreadLocal to create one pre thread as this seems to offer a significant performance
   * improvement over creating one per-thread: http://stackoverflow.com/a/1285297/2648
   * https://github.com/indeedeng/java-dogstatsd-client/issues/4
   */
  private static final ThreadLocal<NumberFormat> NUMBER_FORMATTERS =
      new ThreadLocal<NumberFormat>() {
        @Override
        protected NumberFormat initialValue() {

          // Always create the formatter for the US locale in order to avoid this bug:
          // https://github.com/indeedeng/java-dogstatsd-client/issues/3
          final NumberFormat numberFormatter = NumberFormat.getInstance(Locale.US);
          numberFormatter.setGroupingUsed(false);
          numberFormatter.setMaximumFractionDigits(6);

          // we need to specify a value for Double.NaN that is recognized by dogStatsD
          if (numberFormatter instanceof DecimalFormat) { // better safe than a runtime error
            final DecimalFormat decimalFormat = (DecimalFormat) numberFormatter;
            final DecimalFormatSymbols symbols = decimalFormat.getDecimalFormatSymbols();
            symbols.setNaN("NaN");
            decimalFormat.setDecimalFormatSymbols(symbols);
          }

          return numberFormatter;
        }
      };

  private final String prefix;
  private final String constantTagsRendered;
  Type type;
  String aspect;
  String stringValue;
  long delta;
  long timeInMs;
  double sampleRate;
  long longValue;
  double value;
  String[] tags;
  Event event;
  ServiceCheck serviceCheck;

  public StatsEvent(String prefix, String constantTagsRendered) {
    this.prefix = prefix;
    this.constantTagsRendered = constantTagsRendered;
  }

  /** Generate a suffix conveying the given tag list to the client */
  static String tagString(final String[] tags, final String tagPrefix) {
    final StringBuilder sb;
    if (tagPrefix != null) {
      if ((tags == null) || (tags.length == 0)) {
        return tagPrefix;
      }
      sb = new StringBuilder(tagPrefix);
      sb.append(",");
    } else {
      if ((tags == null) || (tags.length == 0)) {
        return "";
      }
      sb = new StringBuilder("|#");
    }

    for (int n = tags.length - 1; n >= 0; n--) {
      sb.append(tags[n]);
      if (n > 0) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  void reset() {
    stringValue = null;
    aspect = null;
    tags = null;
    event = null;
    serviceCheck = null;
  }

  String count1() {

    return String.format("%s%s:%d|c%s", prefix, aspect, delta, tagString(tags));
  }

  String count2() {

    return String.format("%s%s:%d|c|@%f%s", prefix, aspect, delta, sampleRate, tagString(tags));
  }

  String gauge1() {
    return String.format(
        "%s%s:%s|g%s", prefix, aspect, NUMBER_FORMATTERS.get().format(value), tagString(tags));
  }

  String gauge2() {
    
    return String.format(
        "%s%s:%s|g|@%f%s",
        prefix, aspect, NUMBER_FORMATTERS.get().format(value), sampleRate, tagString(tags));
  }

  String gauge3() {
    return String.format("%s%s:%d|g%s", prefix, aspect, longValue, tagString(tags));
  }

  String gauge4() {
    return String.format("%s%s:%d|g|@%f%s", prefix, aspect, longValue, sampleRate, tagString(tags));
  }

  String executionTime1() {
    return String.format("%s%s:%d|ms%s", prefix, aspect, timeInMs, tagString(tags));
  }

  String executionTime2() {
    return String.format("%s%s:%d|ms|@%f%s", prefix, aspect, timeInMs, sampleRate, tagString(tags));
  }

  String histogram1() {
    return String.format(
        "%s%s:%s|h%s", prefix, aspect, NUMBER_FORMATTERS.get().format(value), tagString(tags));
  }

  String histogram2() {
    return String.format(
        "%s%s:%s|h|@%f%s",
        prefix, aspect, NUMBER_FORMATTERS.get().format(value), sampleRate, tagString(tags));
  }

  String histogram3() {
    return String.format("%s%s:%d|h%s", prefix, aspect, longValue, tagString(tags));
  }

  String histogram4() {
    return String.format("%s%s:%d|h|@%f%s", prefix, aspect, longValue, sampleRate, tagString(tags));
  }

  String event1() {
    final String title = escapeEventString(prefix + event.getTitle());
    final String text = escapeEventString(event.getText());
    return String.format(
        "_e{%d,%d}:%s|%s%s%s",
        title.length(), text.length(), title, text, eventMap(event), tagString(tags));
  }

  String serviceCheck() {
    return toStatsDString(serviceCheck);
  }

  String setValue() {
    return String.format("%s%s:%s|s%s", prefix, aspect, stringValue, tagString(tags));
  }

  private String escapeEventString(final String title) {
    return title.replace("\n", "\\n");
  }

  /** Generate a suffix conveying the given tag list to the client */
  String tagString(final String[] tags) {
    return tagString(tags, constantTagsRendered);
  }

  private String eventMap(final Event event) {
    final StringBuilder res = new StringBuilder("");

    final long millisSinceEpoch = event.getMillisSinceEpoch();
    if (millisSinceEpoch != -1) {
      res.append("|d:").append(millisSinceEpoch / 1000);
    }

    final String hostname = event.getHostname();
    if (hostname != null) {
      res.append("|h:").append(hostname);
    }

    final String aggregationKey = event.getAggregationKey();
    if (aggregationKey != null) {
      res.append("|k:").append(aggregationKey);
    }

    final String priority = event.getPriority();
    if (priority != null) {
      res.append("|p:").append(priority);
    }

    final String alertType = event.getAlertType();
    if (alertType != null) {
      res.append("|t:").append(alertType);
    }

    return res.toString();
  }

  private String toStatsDString(final ServiceCheck sc) {
    // see http://docs.datadoghq.com/guides/dogstatsd/#service-checks
    final StringBuilder sb = new StringBuilder();
    sb.append(String.format("_sc|%s|%d", sc.getName(), sc.getStatus()));
    if (sc.getTimestamp() > 0) {
      sb.append(String.format("|d:%d", sc.getTimestamp()));
    }
    if (sc.getHostname() != null) {
      sb.append(String.format("|h:%s", sc.getHostname()));
    }
    sb.append(tagString(sc.getTags()));
    if (sc.getMessage() != null) {
      sb.append(String.format("|m:%s", sc.getEscapedMessage()));
    }
    return sb.toString();
  }

  enum Type {
    count1,
    count2,
    gauge1,
    gauge2,
    gauge4,
    executionTime1,
    executionTime2,
    histogram1,
    histogram2,
    histogram3,
    histogram4,
    event1,
    serviceCheck,
    setValue,
    gauge3
  }
}
