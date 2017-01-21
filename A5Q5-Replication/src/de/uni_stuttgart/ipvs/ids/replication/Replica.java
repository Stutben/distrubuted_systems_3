package de.uni_stuttgart.ipvs.ids.replication;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.LinkedList;

import de.uni_stuttgart.ipvs.ids.communication.ReadRequestMessage;
import de.uni_stuttgart.ipvs.ids.communication.ReleaseReadLock;
import de.uni_stuttgart.ipvs.ids.communication.ReleaseWriteLock;
import de.uni_stuttgart.ipvs.ids.communication.RequestReadVote;
import de.uni_stuttgart.ipvs.ids.communication.RequestWriteVote;
import de.uni_stuttgart.ipvs.ids.communication.ValueResponseMessage;
import de.uni_stuttgart.ipvs.ids.communication.Vote;
import de.uni_stuttgart.ipvs.ids.communication.WriteRequestMessage;

public class Replica<T> extends Thread {

	public enum LockType {
		UNLOCKED, READLOCK, WRITELOCK
	};

	private int id;

	private double availability;
	private VersionedValue<T> value;

	protected DatagramSocket socket = null;

	protected LockType lock;

	/**
	 * This address holds the addres of the client holding the lock. This
	 * variable should be set to NULL every time the lock is set to UNLOCKED.
	 */
	protected SocketAddress lockHolder;

	public Replica(int id, int listenPort, double availability, T initialValue) throws SocketException {
		super("Replica:" + listenPort);
		this.id = id;
		SocketAddress socketAddress = new InetSocketAddress("127.0.0.1", listenPort);
		this.socket = new DatagramSocket(socketAddress);
		this.availability = availability;
		this.value = new VersionedValue<T>(0, initialValue);
		this.lock = LockType.UNLOCKED;
	}

	/**
	 * Part a) Implement this run method to receive and process request
	 * messages. To simulate a replica that is sometimes unavailable, it should
	 * randomly discard requests as long as it is not locked. The probability
	 * for discarding a request is (1 - availability).
	 * 
	 * For each request received, it must also be checked whether the request is
	 * valid. For example: - Does the requesting client hold the correct lock? -
	 * Is the replica unlocked when a new lock is requested?
	 */
	public void run() {

		while (true) {
			try {
				// receive a message
				byte[] receiveBuffer = new byte[5000];
				DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
				socket.receive(receivePacket);
				Object o = getObjectFromMessage(receivePacket);

				SocketAddress sender = receivePacket.getSocketAddress();

				// availability
				double failurePosibility = 0;
				if (lock == LockType.UNLOCKED) {
					failurePosibility = 1 - availability;
				}

				if (Math.random() >= failurePosibility) {

					// logic
					if (o instanceof RequestReadVote) {
						// no write lock
						if (lock != LockType.WRITELOCK) {
							lock = LockType.READLOCK;
							lockHolder = sender;
							sendVote(sender, Vote.State.YES, value.getVersion());
							// write lock
						} else {
							sendVote(sender, Vote.State.NO, -1);
						}
					} else if (o instanceof RequestWriteVote) {
						// no read or write lock
						if (lock == LockType.UNLOCKED) {
							lock = LockType.WRITELOCK;
							lockHolder = sender;
							sendVote(sender, Vote.State.YES, value.getVersion());
							// locked
						} else {
							sendVote(sender, Vote.State.NO, -1);
						}
					} else if (o instanceof ReadRequestMessage) {
						// check if sender is lockholder
						if ( ((InetSocketAddress) lockHolder).getPort() == ((InetSocketAddress) sender).getPort() && lock == LockType.READLOCK) {
							// prepare ValueResponseMessage
							ValueResponseMessage<VersionedValue<T>> message = new ValueResponseMessage<VersionedValue<T>>(
									value);

							// send message
							sendObject(sender, message);
							// sender is not lockholder
						} else {
							sendVote(sender, Vote.State.NO, -1);
						}
					} else if (o instanceof WriteRequestMessage) {
						// check if sender is lockholder
						if (((InetSocketAddress) lockHolder).getPort() == ((InetSocketAddress) sender).getPort() && lock == LockType.WRITELOCK) {
							// write value
							value = new VersionedValue(((WriteRequestMessage) o).getVersion(), ((WriteRequestMessage) o).getValue());

							// send ack
							sendVote(sender, Vote.State.YES, -1);
							// sender is not lockholder
						} else {
							sendVote(sender, Vote.State.NO, -1);
						}
					} else if (o instanceof ReleaseReadLock) {
						// check if sender is lockholder
						if (((InetSocketAddress) lockHolder).getPort() == ((InetSocketAddress) sender).getPort() && lock == LockType.READLOCK) {
							// release lock
							lockHolder = null;
							lock = LockType.UNLOCKED;

							// send ack
							sendVote(sender, Vote.State.YES, -1);
							// sender is not lockholder
						} else {
							// send nack
							sendVote(sender, Vote.State.NO, -1);
						}
					} else if (o instanceof ReleaseWriteLock) {
						// check if sender is lockholder
						if (((InetSocketAddress) lockHolder).getPort() == ((InetSocketAddress) sender).getPort() && lock == LockType.WRITELOCK) {
							// release lock
							lockHolder = null;
							lock = LockType.UNLOCKED;

							// send ack
							sendVote(sender, Vote.State.YES, -1);
							// sender is not lockholder
						} else {
							// send nack
							sendVote(sender, Vote.State.NO, -1);
						}
					}
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	/**
	 * This is a helper method. You can implement it if you want to use it or
	 * just ignore it. Its purpose is to send a Vote (YES/NO depending on the
	 * state) to the given address.
	 */
	protected void sendVote(SocketAddress address, Vote.State state, int version) throws IOException {

		// prepare vote
		Vote vote = new Vote(state, version);

		// send vote
		sendObject(address, vote);
	}

	/**
	 * sends an object to the given address
	 * 
	 * @param address
	 * @param o
	 * @throws IOException
	 */
	protected void sendObject(SocketAddress address, Object o) throws IOException {

		ByteArrayOutputStream baos = new ByteArrayOutputStream(5000);
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.flush();
		oos.writeObject(o);
		oos.flush();

		byte[] sendBuffer = baos.toByteArray();
		DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, address);
		socket.send(sendPacket);

	}

	/**
	 * This is a helper method. You can implement it if you want to use it or
	 * just ignore it. Its purpose is to extract the object stored in a
	 * DatagramPacket.
	 */
	protected Object getObjectFromMessage(DatagramPacket packet) throws IOException, ClassNotFoundException {

		ByteArrayInputStream bais = new ByteArrayInputStream(packet.getData());
		ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(bais));

		return ois.readObject();
	}

	public int getID() {
		return id;
	}

	public SocketAddress getSocketAddress() {
		return socket.getLocalSocketAddress();
	}

}
