package de.uni_stuttgart.ipvs.ids.communication;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Vector;

/**
 * Part b) Extend the method receiveMessages to return all DatagramPackets that
 * were received during the given timeout.
 * 
 * Also implement unpack() to conveniently convert a Collection of
 * DatagramPackets containing ValueResponseMessages to a collection of
 * MessageWithSource objects.
 * 
 */
public class NonBlockingReceiver {

	protected DatagramSocket socket;

	public NonBlockingReceiver(DatagramSocket socket) {
		this.socket = socket;
	}

	public Vector<DatagramPacket> receiveMessages(int timeoutMillis, int expectedMessages)
			throws IOException {
		
		Vector<DatagramPacket> packets = new Vector<DatagramPacket>();
		
		//change timeout
		try {
			while(expectedMessages > 0){
				//set shorter timeout every time
				socket.setSoTimeout(timeoutMillis);
		
				byte[] buffer = new byte[5000];
				DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
				//measure timeout
				long start = System.currentTimeMillis();
				socket.receive(packet);
				long stop = System.currentTimeMillis();
				timeoutMillis -= (stop-start);
				packets.add(packet);
				expectedMessages--;
			}
			
		} catch (SocketTimeoutException e) {
			// TODO: handle exception
		}
		
		return packets;
	}

	public static <T> Collection<MessageWithSource<T>> unpack(
			Collection<DatagramPacket> packetCollection) throws IOException,
			ClassNotFoundException {
		
		LinkedList<MessageWithSource<T>> messages = new LinkedList<MessageWithSource<T>>();
		
		for(DatagramPacket packet: packetCollection){
			ByteArrayInputStream bais = new ByteArrayInputStream(packet.getData());
			ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(bais));
			
			//build message with source
			MessageWithSource<T> message = new MessageWithSource<T>(packet.getSocketAddress(),(T) ois.readObject()); 
			
			messages.add(message);
		}
		return messages;
	}
	
}
