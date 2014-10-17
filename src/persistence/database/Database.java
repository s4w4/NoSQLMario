package persistence.database;

import persistence.importhandler.impl.DBConfig;

public interface Database {
		
	// Erstellt eine neue Datenbank, wenn die DB nicht existiert
	// und (erzeugt)/(ersetzt die alte) Tabelle
	public boolean createDatabase();
	
	// Gibt eine Liste von Rewards zurueck
	public double[] select(int stateId);
	
	// update lokal
	boolean update(int state, int action, double rewards); 	

	// Speichert neuen Wissenbasis in db
	public boolean saveAll();
	

}
