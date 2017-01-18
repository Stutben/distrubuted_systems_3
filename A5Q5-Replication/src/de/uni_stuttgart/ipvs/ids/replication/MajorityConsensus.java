package de.uni_stuttgart.ipvs.ids.replication;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;

import de.uni_stuttgart.ipvs.ids.communication.MessageWithSource;
import de.uni_stuttgart.ipvs.ids.communication.NonBlockingReceiver;
import de.uni_stuttgart.ipvs.ids.communication.ReadRequestMessage;
import de.uni_stuttgart.ipvs.ids.communication.ReleaseReadLock;
import de.uni_stuttgart.ipvs.ids.communication.ReleaseWriteLock;
import de.uni_stuttgart.ipvs.ids.communication.RequestReadVote;
import de.uni_stuttgart.ipvs.ids.communication.RequestWriteVote;
import de.uni_stuttgart.ipvs.ids.communication.ValueResponseMessage;
import de.uni_stuttgart.ipvs.ids.communication.Vote;
import de.uni_stuttgart.ipvs.ids.communication.Vote.State;
import de.uni_stuttgart.ipvs.ids.communication.WriteRequestMessage;

public class MajorityConsensus<T> {

	protected Collection<SocketAddress> replicas;

	protected DatagramSocket socket;
	protected NonBlockingReceiver nbio;

	final static int TIMEOUT = 1000;

	public MajorityConsensus(Collection<SocketAddress> replicas)
			throws SocketException {
		this.replicas = replicas;
		SocketAddress address = new InetSocketAddress("127.0.0.1", 4999);
		this.socket = new DatagramSocket(address);
		this.nbio = new NonBlockingReceiver(socket);
	}

	/**
	 * Part c) Implement this method.
	 */
	protected Collection<MessageWithSource<Vote>> requestReadVote() throws QuorumNotReachedException, IOException, ClassNotFoundException {
		// TODO: Implement me!
		Iterator<SocketAddress> it = replicas.iterator();

		ByteArrayOutputStream baos = new ByteArrayOutputStream(5000);

		ObjectOutputStream oos = new ObjectOutputStream(baos);

		RequestReadVote requestReadVote = new RequestReadVote();

		while(it.hasNext()){

			SocketAddress address = it.next();

			oos.flush();

			oos.writeObject(requestReadVote);

			oos.flush();

			byte[] sendBuffer = baos.toByteArray();

			DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, address);

			socket.send(sendPacket);

		}
		return nbio.unpack(nbio.receiveMessages(1000, replicas.size()));
	}
	
	/**
	 * Part c) Implement this method.
	 */
	protected void releaseReadLock(Collection<SocketAddress> lockedReplicas) throws IOException {
		
		ReleaseReadLock release_RL = new ReleaseReadLock();
		Iterator<SocketAddress> it = lockedReplicas.iterator();
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream(5000);
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		
		while(lockedReplicas.isEmpty() == false){
		
			while (it.hasNext() == true){
				
				SocketAddress socketaddr =  it.next();
				
				oos.flush();
				oos.writeObject(release_RL);
				oos.flush();
	
				byte[] sendBuffer = baos.toByteArray();
				DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, socketaddr);
							
				try{
					socket.send(sendPacket);
				}
				catch(Exception e) {
				}
			}
			
			Vector<DatagramPacket> packets = nbio.receiveMessages(200, lockedReplicas.size());
			Collection<MessageWithSource<Vote>> messages = null;
			
			try {
				messages = NonBlockingReceiver.unpack(packets);
			}
			catch(Exception e)
			{}
			
			Collection<SocketAddress> responders = MessageWithSource.getSources(messages);
			
			lockedReplicas.removeAll(responders);
				
		}
	}
	
	/**
	 * Part d) Implement this method.
	 */
	protected Collection<MessageWithSource<Vote>> requestWriteVote() throws QuorumNotReachedException {
		// TODO: Implement me!
	}
	
	/**
	 * Part d) Implement this method.
	 */
	protected void releaseWriteLock(Collection<SocketAddress> lockedReplicas) throws IOException{
		ReleaseWriteLock release_WL = new ReleaseWriteLock();
		Iterator<SocketAddress> it = lockedReplicas.iterator();
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream(5000);
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		
		while(lockedReplicas.isEmpty() == false){
		
			while (it.hasNext() == true){
				
				SocketAddress socketaddr =  it.next();
				
				oos.flush();
				oos.writeObject(release_WL);
				oos.flush();
	
				byte[] sendBuffer = baos.toByteArray();
				DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, socketaddr);
							
				try{
					socket.send(sendPacket);
				}
				catch(Exception e) {
				}
			}
			
			Vector<DatagramPacket> packets = nbio.receiveMessages(200, lockedReplicas.size());
			Collection<MessageWithSource<Vote>> messages = null;
			
			try {
				messages = NonBlockingReceiver.unpack(packets);
			}
			catch(Exception e)
			{}
			
			Collection<SocketAddress> responders = MessageWithSource.getSources(messages);
			
			lockedReplicas.removeAll(responders);
				
		}	
	}
	
	
	/**
	 * Part c) Implement this method.
	 */
	protected T readReplica(SocketAddress replica) {
		//send ReadRequestMessage
		
		Collection<MessageWithSource<ValueResponseMessage<T>>> response = null;			
		try{
			ByteArrayOutputStream baos = new ByteArrayOutputStream(5000);
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			
			oos.flush();
			oos.writeObject(new ReadRequestMessage());
			oos.flush();

			//receive answer
			byte[] sendBuffer = baos.toByteArray();
			DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, replica);
			socket.send(sendPacket);
			Vector<DatagramPacket> packets = nbio.receiveMessages(200, 1);	
			response = NonBlockingReceiver.unpack(packets);	
		}
		catch(Exception e) {
		
		}
		
		
		Iterator<MessageWithSource<ValueResponseMessage<T>>> it = response.iterator();
		
		return it.next().getMessage().getValue();
	}
	
	/**
	 * Part d) Implement this method.
	 */
	protected void writeReplicas(Collection<SocketAddress> lockedReplicas, VersionedValue<T> newValue) {
		// TODO: Implement me!
	}
	
	/**
	 * Part c) Implement this method (and checkQuorum(), see below) to read the
	 * replicated value using the majority consensus protocol.
	 */
	public VersionedValue<T> get() throws QuorumNotReachedException {
		// TODO: Implement me
	}

	/**
	 * Part d) Implement this method to set the
	 * replicated value using the majority consensus protocol.
	 */
	public void set(T value) throws QuorumNotReachedException {
		// TODO: Implement me!
	}

	/**
	 * Part c) Implement this method to check whether a sufficient number of
	 * replies were received. If a sufficient number was received, this method
	 * should return the {@link MessageWithSource}s of the locked {@link Replica}s.
	 * Otherwise, a QuorumNotReachedException must be thrown.
	 * @throws QuorumNotReachedException 
	 */
	protected Collection<MessageWithSource<Vote>> checkQuorum(
			Collection<MessageWithSource<Vote>> replies) throws QuorumNotReachedException {
	Iterator<MessageWithSource<Vote>> it = replies.iterator();
		
		int writequorum = replicas.size() / 2 + 1;
		int readquorum = replicas.size() / 2;
		int readlocks = 0;
		int writelocks = 0;
		
		while (it.hasNext()){
			
			MessageWithSource<Vote> currentMessage = it.next();
			if (currentMessage.getMessage().getState() == Vote.State.YES ){
				
				if(currentMessage.getMessage().getVersion() == -1){
					
					writelocks ++;
				}
				else{
				
					readlocks ++;
				}
			}
			else{
				
				replies.remove(currentMessage);
			}
				
		}
		
		if(writelocks < writequorum) 
		
			throw new QuorumNotReachedException(writequorum, MessageWithSource.getSources(replies));

		else if(readlocks < readquorum){
			
			throw new QuorumNotReachedException(readquorum, MessageWithSource.getSources(replies)); 
		}
		
		return replies;
	}
}
