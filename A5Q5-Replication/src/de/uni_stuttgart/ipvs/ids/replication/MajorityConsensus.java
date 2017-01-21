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

		Iterator<SocketAddress> it = replicas.iterator();

		ByteArrayOutputStream baos = new ByteArrayOutputStream(5000);

		ObjectOutputStream oos = new ObjectOutputStream(baos);

		RequestReadVote requestReadVote = new RequestReadVote();

		// iterate over all replicas and send a requestReadVote to each replica
		while(it.hasNext()){

			SocketAddress address = it.next();

			oos.flush();

			oos.writeObject(requestReadVote);

			oos.flush();

			byte[] sendBuffer = baos.toByteArray();

			DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, address);

			socket.send(sendPacket);
		}
		// return all "YES" Messages if required Quorum is reached, else throw QuorumNotReachedException
		
		Collection<MessageWithSource<Vote>> messages = NonBlockingReceiver.unpack(nbio.receiveMessages(TIMEOUT, replicas.size()));
		return checkQuorum(messages);
	}
	
	/**
	 * Part c) Implement this method.
	 */
	protected void releaseReadLock(Collection<SocketAddress> lockedReplicas) throws IOException {
		
		ReleaseReadLock release_RL = new ReleaseReadLock();
		Iterator<SocketAddress> it = lockedReplicas.iterator();
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream(5000);
		ObjectOutputStream oos = new ObjectOutputStream(baos);

		//iterate over all (read-)locked replicas and send a ReleaseReadlock message to each of the locked replicas
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
			
			Vector<DatagramPacket> packets = nbio.receiveMessages(TIMEOUT, lockedReplicas.size());
			Collection<MessageWithSource<Vote>> messages = null;
			
			try {
				messages = NonBlockingReceiver.unpack(packets);
			}
			catch(Exception e)
			{}
			
			Collection<SocketAddress> responders = MessageWithSource.getSources(messages);

			//remove all of the unlocked replicas from the list of locked replicas
			lockedReplicas.removeAll(responders);
				
		}
	}
	
	/**
	 * Part d) Implement this method.
	 */
	protected Collection<MessageWithSource<Vote>> requestWriteVote() throws QuorumNotReachedException, IOException, ClassNotFoundException {

		Iterator<SocketAddress> it = replicas.iterator();

		ByteArrayOutputStream baos = new ByteArrayOutputStream(5000);

		ObjectOutputStream oos = new ObjectOutputStream(baos);

		RequestWriteVote requestWriteVote = new RequestWriteVote();

		// iterate over all replicas and send a requestWriteVote to each replica
		while(it.hasNext()){

			SocketAddress address = it.next();

			oos.flush();

			oos.writeObject(requestWriteVote);

			oos.flush();

			byte[] sendBuffer = baos.toByteArray();

			DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, address);

			socket.send(sendPacket);
		}
		// return all "YES" Messages if required Quorum is reached, else throw QuorumNotReachedException
		Collection<MessageWithSource<Vote>> messages = NonBlockingReceiver.unpack(nbio.receiveMessages(TIMEOUT, replicas.size()));
		return checkQuorum(messages);
	}
	
	/**
	 * Part d) Implement this method.
	 */
	protected void releaseWriteLock(Collection<SocketAddress> lockedReplicas) throws IOException{
		ReleaseWriteLock release_WL = new ReleaseWriteLock();
		Iterator<SocketAddress> it = lockedReplicas.iterator();
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream(5000);
		ObjectOutputStream oos = new ObjectOutputStream(baos);

		//iterate over all (write-)locked replicas and send a ReleaseWritelock message to each of the locked replicas
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
			
			Vector<DatagramPacket> packets = nbio.receiveMessages(TIMEOUT, lockedReplicas.size());
			Collection<MessageWithSource<Vote>> messages = null;
			
			try {
				messages = NonBlockingReceiver.unpack(packets);
			}
			catch(Exception e)
			{}
			
			Collection<SocketAddress> responders = MessageWithSource.getSources(messages);

			//remove all of the unlocked replicas from the list of locked replicas
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
			Vector<DatagramPacket> packets = nbio.receiveMessages(TIMEOUT, 1);
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
	protected void writeReplicas(Collection<SocketAddress> lockedReplicas, VersionedValue<T> newValue) throws IOException {
		// TODO: Implement me!

		WriteRequestMessage writeRequestMessage = new WriteRequestMessage(newValue.getVersion(), newValue.getValue());
		ByteArrayOutputStream baos = new ByteArrayOutputStream(5000);
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		Iterator<SocketAddress> it = lockedReplicas.iterator();

		while(it.hasNext()){
			SocketAddress socketaddr =  it.next();

			oos.flush();
			oos.writeObject(writeRequestMessage);
			oos.flush();

			byte[] sendBuffer = baos.toByteArray();
			DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, socketaddr);

			try{
				socket.send(sendPacket);
				nbio.receiveMessages(TIMEOUT, lockedReplicas.size());
			}
			catch(Exception e) {
			}
		}
	}
	
	/**
	 * Part c) Implement this method (and checkQuorum(), see below) to read the
	 * replicated value using the majority consensus protocol.
	 */
	public VersionedValue<T> get() throws QuorumNotReachedException, IOException, ClassNotFoundException {
		
		Collection<MessageWithSource<Vote>> lockedreplies = null;
		//get all replicas that answered with YES
		try{
			lockedreplies = this.requestReadVote();

		}
		catch(QuorumNotReachedException e){
			
			this.releaseReadLock(e.getAchieved());
			throw e;			
		}
		
		
		Iterator<MessageWithSource<Vote>> it = lockedreplies.iterator();
		
		int serialmax = 0;
		SocketAddress maxSocket = null;

		//get the replica with the most up-to-date value using the serialnumber
		while (it.hasNext()){
			
			MessageWithSource<Vote> currentmessage = it.next();
			int currentserial = currentmessage.getMessage().getVersion();
			if(currentserial >= serialmax){
				
				serialmax = currentserial;
				maxSocket = currentmessage.getSource();
			}
		}
		// get value from replica
		VersionedValue<T> result = (VersionedValue<T>)this.readReplica(maxSocket);

		//release readlocks
		this.releaseReadLock(MessageWithSource.getSources(lockedreplies));
		
		return result; 
	}

	/**
	 * Part d) Implement this method to set the
	 * replicated value using the majority consensus protocol.
	 */
	public void set(T value) throws QuorumNotReachedException, IOException, ClassNotFoundException {
		// TODO: Implement me!

		Collection<MessageWithSource<Vote>> lockedreplicas = null;
		//get all replicas that answered with YES
		try{
			lockedreplicas = this.requestWriteVote();

		}
		catch(QuorumNotReachedException e){

			this.releaseWriteLock(e.getAchieved());
			throw e;
		}


		Iterator<MessageWithSource<Vote>> it = lockedreplicas.iterator();

		int serialmax = 0;

		//get the replica with the most up-to-date value using the serialnumber
		while (it.hasNext()){

			MessageWithSource<Vote> currentmessage = it.next();
			int currentserial = currentmessage.getMessage().getVersion();
			if(currentserial > serialmax){

				serialmax = currentserial;
			}
		}
		// create new value to be written
		VersionedValue<T> valueToWrite = new VersionedValue(serialmax+1, value);

		//write value
		this.writeReplicas(MessageWithSource.getSources(lockedreplicas), valueToWrite);
		
		//release locks
		this.releaseWriteLock(MessageWithSource.getSources(lockedreplicas));
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

		//iterate over replies and count "YES" and "NO" messages
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

		//check if quorum to read/write is fulfilled
		if(writelocks < writequorum && readlocks < readquorum) 
		
			throw new QuorumNotReachedException(writequorum, MessageWithSource.getSources(replies));
		
		return replies;
	}
}
