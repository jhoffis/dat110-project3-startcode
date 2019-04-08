package no.hvl.dat110.node.client.test;

import java.math.BigInteger;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.Random;

import no.hvl.dat110.file.FileManager;
import no.hvl.dat110.rpc.StaticTracker;
import no.hvl.dat110.rpc.interfaces.ChordNodeInterface;
import no.hvl.dat110.util.Hash;
import no.hvl.dat110.util.Util;

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
		// connect to an active chord node - can use the process defined in
		// StaticTracker
				
		Registry reg = Util.tryIPs();
		try 
		{
			if(reg != null)
			{
				// Compute the hash of the node's IP address
				BigInteger hashActiveNode = Hash.hashOf(Util.activeIP);
				// use the hash to retrieve the ChordNodeInterface remote object from the registry
				ChordNodeInterface chordNode = (ChordNodeInterface) reg.lookup(hashActiveNode.toString());
				
				if(chordNode != null)
				{
					// do: FileManager fm = new FileManager(ChordNodeInterface, StaticTracker.N);
					FileManager fm = new FileManager(chordNode, StaticTracker.N);
					// do: boolean succeed = fm.requestToReadFileFromAnyActiveNode(filename);
					fm.requestToReadFileFromAnyActiveNode(filename);			
				}
					
				
			}
			
		}
		catch(RemoteException | NotBoundException e) 
		{
			e.printStackTrace();
		}		

	}

	public boolean isSucceed() {
		return succeed;
	}

}
