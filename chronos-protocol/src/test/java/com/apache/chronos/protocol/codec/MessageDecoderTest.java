package com.apache.chronos.protocol.codec;

import com.apache.chronos.protocol.message.Ping;
import com.apache.chronos.protocol.message.Pong;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MessageDecoderTest {

  private static final Logger log = LogManager.getLogger(MessageDecoderTest.class);
  private MessageEncoder encoder = new MessageEncoder();
  private MessageDecoder decoder = new MessageDecoder();

  @Test
  public void testPing() throws Exception {
    Ping ping = Ping.create();
    ping.setNodeId(UUID.randomUUID().toString());
    ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
    encoder.encode(null, ping, out);

    log.info(ByteBufUtil.hexDump(out));

    List<Object> outList = new ArrayList<>();
    decoder.decode(null, out, outList);

    Assertions.assertEquals(ping.getNodeId(), ((Ping) outList.get(0)).getNodeId());
  }

  @Test
  public void testPong() throws Exception {
    Pong ping = Pong.create();
    ping.setNodeId(UUID.randomUUID().toString());
    ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
    encoder.encode(null, ping, out);

    log.info(ByteBufUtil.hexDump(out));

    List<Object> outList = new ArrayList<>();
    decoder.decode(null, out, outList);

    Assertions.assertEquals(ping.getNodeId(), ((Pong) outList.get(0)).getNodeId());
  }
}