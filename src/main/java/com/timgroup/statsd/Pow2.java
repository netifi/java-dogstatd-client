package com.timgroup.statsd;

final class Pow2 {
  private static final int MAX_POW2 = 1 << 30;

  /**
   * @param value from which next positive power of two will be found.
   * @return the next positive power of 2, this value if it is a power of 2. Negative values are
   *     mapped to 1.
   * @throws IllegalArgumentException is value is more than MAX_POW2 or less than 0
   */
  static int roundToPowerOfTwo(final int value) {
    if (value > MAX_POW2) {
      throw new IllegalArgumentException(
          "There is no larger power of 2 int for value:" + value + " since it exceeds 2^31.");
    }
    if (value < 0) {
      throw new IllegalArgumentException("Given value:" + value + ". Expecting value >= 0.");
    }
    final int nextPow2 = 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    return nextPow2;
  }
}
