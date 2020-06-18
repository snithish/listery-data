package io.listery;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class Utils {
  private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

  static String todayString() {
    LocalDate today = LocalDate.now();
    return today.format(formatter);
  }

  static String yesterdayString(String todayString) throws ParseException {
    LocalDate today = LocalDate.parse(todayString, formatter);
    LocalDate yesterday = today.minusDays(1);
    return yesterday.format(formatter);
  }
}
