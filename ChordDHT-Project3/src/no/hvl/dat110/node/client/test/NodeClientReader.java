package no.hvl.dat110.node.client.test;

import java.util.Random;

import no.hvl.dat110.rpc.StaticTracker;

public class NodeClientReader extends Thread {

	private boolean succeed = false;

	private String filename;

	public NodeClientReader(String filename) {
		this.filename = filename;
	}

	public void run() {
		sendRequest();
	}

	private void sendRequest() {

		// Lookup(key) - Use this class as a client that is requesting for a new file
		// and needs the identifier and IP of the node where the file is located
		// assume you have a list of nodes in the tracker class and select one randomly.
		// We can use the Tracker class for this purpose
		Random r = new Random();
		//FIXME might be ACTIVENODES.length
		int limit = r.nextInt(StaticTracker.N);
		String ip = StaticTracker.ACTIVENODES[limit];
		// connect to an active chord node - can use the process defined in
		// StaticTracker

		// Compute the hash of the node's IP address

		// use the hash to retrieve the ChordNodeInterface remote object from the
		// registry

		// do: FileManager fm = new FileManager(ChordNodeInterface, StaticTracker.N);

		// do: boolean succeed = fm.requestToReadFileFromAnyActiveNode(filename);

	}

	public boolean isSucceed() {
		return succeed;
	}

}
