package no.hvl.dat110.mutexprocess;

import java.io.File;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import no.hvl.dat110.interfaces.ProcessInterface;
import no.hvl.dat110.util.Util;

public class MutexProcess extends UnicastRemoteObject implements ProcessInterface {

	private static final long serialVersionUID = 1L;

	private int processId;
	private String procStubname;
	private int counter;

	private List<Message> queueACK; // queue for acknowledged messages

	private File localfile; // a shared resource; each replica has own local copy
	private String filename = "file1.txt";
	private int version = 0;
	private List<String> replicas; // list of other processes including self known to this process
	private boolean CS_BUSY = false; // Lock: indicate that critical section is being accessed (e.g. accessing a
										// shared resource)
	private boolean WANTS_TO_ENTER_CS = false; // Lock: indicate process wants to enter CS
	private int N; // number of replicas storing a replicated resource - Not all processes
	private int quorum; // quorum needed to be granted access

	protected MutexProcess(int procId, String stubName) throws RemoteException {
		super();
		this.processId = procId;
		this.procStubname = stubName;
		counter = 0;

		queueACK = new ArrayList<Message>();
		queueACK = Collections.synchronizedList(queueACK); // make sure to synchronize this list

		replicas = Util.getProcessReplicas();
		N = replicas.size();
		quorum = N / 2 + 1;

		createFile(); // create a local file - this is our shared resource (item)
	}

	private void createFile() {
		String path = new File(".").getAbsolutePath().replace(".", "");
		System.out.println(path);
		path = path + "/" + procStubname + "/";
		File fpath = new File(path);
		if (!fpath.exists()) {
			boolean suc = fpath.mkdir();
			try {
				if (suc) {
					File file = new File(fpath + "/" + filename);
					file.createNewFile();

				}
			} catch (IOException e) {

				e.printStackTrace();
			}
		}
		this.setFilename(fpath.getAbsolutePath() + "/" + filename);
	}

	public void incrementclock() throws RemoteException {
		counter++;
	}

	public void acquireLock() throws RemoteException {
		// logical clock update and set CS variable

		CS_BUSY = true;
		incrementclock();
	}

	public void releaseLocks() throws RemoteException {

		// release your lock variables and logical clock update

		CS_BUSY = false;
		incrementclock();
		// update clock
	}

	public boolean requestWriteOperation(Message message) throws RemoteException {
		incrementclock(); // increment clock
		message.setClock(counter); // set the timestamp of message
		message.setProcessID(processId); // set the process ID
		message.setOptype(OperationType.WRITE);

		return multicastMessage(message, N - 1);

		// multicast read request to start the voting to N/2 + 1 replicas (majority) -
		// optimal. You could as well send to all the replicas that have the file

		// FIXME maybe. Skal denne bli godkjent bare her? Eller ved alle? Altså denne
		// holder jo på alle andre registries sine navn, men har ikke kobling.

		// change to the election result
	}

	public boolean requestReadOperation(Message message) throws RemoteException {
		incrementclock(); // increment clock
		message.setClock(counter); // set the timestamp of message
		message.setProcessID(processId); // set the process ID
		message.setOptype(OperationType.READ);

		WANTS_TO_ENTER_CS = true;

		// multicast read request to start the voting to N/2 + 1 replicas (majority) -
		// optimal. You could as well send to all the replicas that have the file

		return multicastMessage(message, N - 1); // change to the election result
	}

	// multicast message to N/2 + 1 processes (random processes)
	private boolean multicastMessage(Message message, int n) throws AccessException, RemoteException {

		replicas.remove(this.procStubname); // remove this process from the list

		// randomize - shuffle list each time - to get random processes each time
		Collections.shuffle(replicas);

		// multicast message to N/2 + 1 processes (random processes) - block until
		// feedback is received

		CS_BUSY = true;

		for (String rep : replicas) {
			try {
				ProcessInterface pi = Util.registryHandle(rep);
				// Hentet ut prosessen.. Hva skal den brukes til?

				queueACK.add(pi.onMessageReceived(message));
			} catch (NotBoundException e) {
				e.printStackTrace();
			}
		}

		CS_BUSY = false;

		// do something with the acknowledgement you received from the voters - Idea:
		// use the queueACK to collect GRANT/DENY messages and make sure queueACK is
		// synchronized!!!

		// Å ja, bare ha en test greie... Men hvordan brukes queueACK. add en
		// onmessagerecieved(message)

		// compute election result - Idea call majorityAcknowledged()
		replicas = Util.getProcessReplicas();
		return majorityAcknowledged(); // change to the election result

	}

	@Override
	public Message onMessageReceived(Message message) throws RemoteException {

		// increment the local clock
		incrementclock();
		// Hint: for all 3 cases, use Message to send GRANT or DENY. e.g.
		// message.setAcknowledgement(true) = GRANT

		/**
		 * case 1: Receiver is not accessing shared resource and does not want to:
		 * GRANT, acquirelock and reply
		 */
		if (!CS_BUSY && !WANTS_TO_ENTER_CS) {
			message.setAcknowledged(true);
			acquireLock();

		} else if (CS_BUSY) {
			/**
			 * case 2: Receiver already has access to the resource: DENY and reply
			 */
			message.setAcknowledged(false);
		} else if (WANTS_TO_ENTER_CS) {
			/**
			 * case 3: Receiver wants to access resource but is yet to (compare own
			 * multicast message to received message the message with lower timestamp wins)
			 * - GRANT if received is lower, acquirelock and reply
			 */
			if (counter < message.getClock()) {
				message.setAcknowledged(true);
				acquireLock();
			} else {
				message.setAcknowledged(false);
			}
		}

		return message;
	}

	public boolean majorityAcknowledged() throws RemoteException {

		// count the number of yes (i.e. where message.isAcknowledged = true)
		// check if it is the majority or not
		// return the decision (true or false)

		int totalAcknowledged = 0;

		for (Message message : queueACK) {
			if (message.isAcknowledged())
				totalAcknowledged++;
		}

		return (totalAcknowledged >= quorum);

	}

	@Override
	public void onReceivedVotersDecision(Message message) throws RemoteException {

		// release CS lock if voter initiator says he was denied access bcos he lacks
		// majority votes
		//Tell everyone of the boolean desitiion
		
		if (!message.isAcknowledged()) {
			// Release
			CS_BUSY = false;
		} else {
			// otherwise lock is kept - says hey im about to use this critical section.
			CS_BUSY = true;
		}
	}

	@Override
	public void onReceivedUpdateOperation(Message message) throws RemoteException {

		// check the operation type: we expect a WRITE operation to do this.
		// perform operation by using the Operations class
		// Release locks after this operation

		if (message.getOptype() == OperationType.WRITE) {

		}

	}

	@Override
	public void multicastUpdateOrReadReleaseLockOperation(Message message) throws RemoteException {

		// check the operation type:
		// if this is a write operation, multicast the update to the rest of the
		if (message.getOptype() == OperationType.WRITE) {

		} else {
			// READ
		}
		// replicas (voters)
		// otherwise if this is a READ operation multicast releaselocks to the replicas
		// (voters)
	}

	@Override
	public void multicastVotersDecision(Message message) throws RemoteException {
		// multicast voters decision to the rest of the replicas

		replicas.remove(this.procStubname); // remove this process from the list

		for (String rep : replicas) {
			try {
				Util.registryHandle(rep).onReceivedVotersDecision(message);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	@Override
	public int getProcessID() throws RemoteException {

		return processId;
	}

	@Override
	public int getVersion() throws RemoteException {

		return version;
	}

	public File getLocalfile() {
		return localfile;
	}

	public void setLocalfile(File localfile) {
		this.localfile = localfile;
	}

	@Override
	public void setVersion(int version) throws RemoteException {

		this.version = version;
	}

	public String getFilename() throws RemoteException {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

}
