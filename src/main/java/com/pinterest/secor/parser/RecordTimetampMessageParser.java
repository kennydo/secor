package java.com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.parser.TimestampedMessageParser;

/**
 * RecordTimetampMessageParser extracts the timestamp from the Kafka record's metadata.
 *
 * @author Kenny Do (kennyhdo@gmail.com)
 */

public class RecordTimetampMessageParser extends TimestampedMessageParser {
    public RecordTimetampMessageParser(SecorConfig config) {
        super(config);
    }

    @Override
    public long extractTimestampMillis(Message message) throws Exception {
        return message.getTimestamp();
    }
}
