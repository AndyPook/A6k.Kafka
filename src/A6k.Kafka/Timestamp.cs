using System;

namespace A6k.Kafka
{
    public static class Timestamp
    {
        /// <summary>
        ///     Unix epoch as a UTC DateTime. Unix time is defined as 
        ///     the number of seconds past this UTC time, excluding 
        ///     leap seconds.
        /// </summary>
        public static readonly DateTime UnixTimeEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private const long UnixTimeEpochMilliseconds = 62135596800000; // = UnixTimeEpoch.TotalMiliseconds

        /// <summary>
        ///     Convert a DateTime instance to a milliseconds unix timestamp.
        ///     Note: <paramref name="dateTime"/> is first converted to UTC 
        ///     if it is not already.
        /// </summary>
        /// <param name="dateTime">
        ///     The DateTime value to convert.
        /// </param>
        /// <returns>
        ///     The milliseconds unix timestamp corresponding to <paramref name="dateTime"/>
        ///     rounded down to the previous millisecond.
        /// </returns>
        public static long DateTimeToUnixTimestampMs(this DateTime dateTime)
            => dateTime.ToUniversalTime().Ticks / TimeSpan.TicksPerMillisecond - UnixTimeEpochMilliseconds;

        /// <summary>
        ///     Convert a milliseconds unix timestamp to a DateTime value.
        /// </summary>
        /// <param name="unixMillisecondsTimestamp">
        ///     The milliseconds unix timestamp to convert.
        /// </param>
        /// <returns>
        ///     The DateTime value associated with <paramref name="unixMillisecondsTimestamp"/> with Utc Kind.
        /// </returns>
        public static DateTime UnixTimestampMsToDateTime(long unixMillisecondsTimestamp)
            => UnixTimeEpoch + TimeSpan.FromMilliseconds(unixMillisecondsTimestamp);
    }
}
