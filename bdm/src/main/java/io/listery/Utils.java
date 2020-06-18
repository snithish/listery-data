package io.listery;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {
  private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

  static String todayString() {
    return simpleDateFormat.format(new Date());
  }
}
