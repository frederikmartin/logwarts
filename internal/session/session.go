package session

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var (
	sessionDB   *sql.DB
	sessionLock sync.Mutex
)

type Session struct {
	ID        int
	CreatedAt time.Time
	UpdatedAt time.Time
	Name      string
	State     string
}

func Init() error {
	var err error
	dbPath := filepath.Join(os.TempDir(), "logwarts_sessions.db")
	sessionDB, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("Failed to open session database: %v", err)
	}

	if sessionDB == nil {
		return fmt.Errorf("Failed to initialize session database: sessionDB is nil")
	}

	createTableQuery := `
	CREATE TABLE IF NOT EXISTS sessions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		name TEXT UNIQUE NOT NULL,
		state TEXT NOT NULL
	);
	CREATE TRIGGER IF NOT EXISTS update_sessions_updated_at
		AFTER UPDATE ON sessions
		FOR EACH ROW
		BEGIN
			UPDATE sessions SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
	END;`
	_, err = sessionDB.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("Failed to create sessions table: %v", err)
	}

	return nil
}

func CreateSession(name string) error {
	sessionLock.Lock()
	defer sessionLock.Unlock()

	if sessionDB == nil {
		return fmt.Errorf("sessionDB is not initialized. Please call Initialize() first")
	}

	inactivateSessionsQuery := `UPDATE sessions SET state = 'inactive' WHERE state = 'active'`
	_, err := sessionDB.Exec(inactivateSessionsQuery)
	if err != nil {
		return fmt.Errorf("Failed to inactivate sessions: %v", err)
	}

	sessionName, err := SanitizeSessionName(name)
	if err != nil {
		return fmt.Errorf("Invalid session name: %v", err)
	}
	insertQuery := `INSERT INTO sessions (name, state) VALUES (?, 'active')`
	_, err = sessionDB.Exec(insertQuery, sessionName)
	if err != nil {
		return fmt.Errorf("Failed to create session: %v", err)
	}
	fmt.Printf("Session '%s' created successfully\n", sessionName)
	return nil
}

func AttachSession(name string) error {
	sessionLock.Lock()
	defer sessionLock.Unlock()

	if sessionDB == nil {
		return fmt.Errorf("sessionDB is not initialized. Please call Initialize() first")
	}

	selectQuery := `SELECT id, state FROM sessions WHERE name = ?`
	row := sessionDB.QueryRow(selectQuery, name)
	var sessionToAttach Session
	err := row.Scan(&sessionToAttach.ID, &sessionToAttach.State)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("Session with name '%s' not found: %v", name, err)
		}
		return fmt.Errorf("Error scanning session state: %v", err)
	}

	inactivateSessionsQuery := `UPDATE sessions SET state = 'inactive' WHERE state = 'active'`
	_, err = sessionDB.Exec(inactivateSessionsQuery)
	if err != nil {
		return fmt.Errorf("Failed to inactivate sessions: %v", err)
	}

	activateSessionQuery := `UPDATE sessions SET state = 'active' WHERE id = ?`
	_, err = sessionDB.Exec(activateSessionQuery, sessionToAttach.ID)
	if err != nil {
		return fmt.Errorf("Failed to activate session '%s': %v", name, err)
	}

	fmt.Printf("Attached to session: %s\n", name)
	return nil
}

func GetActiveSession() (*Session, error) {
	sessionLock.Lock()
	defer sessionLock.Unlock()

	if sessionDB == nil {
		return nil, fmt.Errorf("sessionDB is not initialized. Please call Initialize() first")
	}

	selectQuery := `SELECT id, created_at, updated_at, name, state FROM sessions WHERE state = 'active'`
	var session Session
	row := sessionDB.QueryRow(selectQuery)
	if err := row.Scan(&session.ID, &session.CreatedAt, &session.UpdatedAt, &session.Name, &session.State); err != nil {
		return nil, fmt.Errorf("No active session found")
	}
	return &session, nil
}

func ListSessions() ([]Session, error) {
	sessionLock.Lock()
	defer sessionLock.Unlock()

	if sessionDB == nil {
		return nil, fmt.Errorf("sessionDB is not initialized. Please call Initialize() first")
	}

	var sessions []Session
	query := `SELECT id, created_at, updated_at, name, state FROM sessions`
	rows, err := sessionDB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("Failed to list sessions: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var session Session
		if err := rows.Scan(&session.ID, &session.CreatedAt, &session.UpdatedAt, &session.Name, &session.State); err != nil {
			return nil, fmt.Errorf("Failed to read session data: %v", err)
		}
		sessions = append(sessions, session)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("Error during rows iteration: %v", err)
	}

	return sessions, nil
}

func KillSession() error {
	sessionLock.Lock()
	defer sessionLock.Unlock()

	if sessionDB == nil {
		return fmt.Errorf("sessionDB is not initialized. Please call Initialize() first")
	}

	query := `DELETE FROM sessions WHERE state = 'active'`
	_, err := sessionDB.Exec(query)
	if err != nil {
		return fmt.Errorf("Failed to kill session: %v", err)
	}

	return nil
}

func Close() error {
	if sessionDB != nil {
		return sessionDB.Close()
	}
	return nil
}
