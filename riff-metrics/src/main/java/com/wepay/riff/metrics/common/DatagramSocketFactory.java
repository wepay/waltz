package com.wepay.riff.metrics.common;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;

public class DatagramSocketFactory {
  public DatagramSocket createSocket() throws SocketException {
    return new DatagramSocket();
  }

  public DatagramPacket createPacket(final byte[] bytes, final int length, final InetSocketAddress address)
      throws SocketException {
    return new DatagramPacket(bytes, length, address);
  }
}
