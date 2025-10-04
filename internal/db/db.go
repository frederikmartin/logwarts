package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/frederikmartin/logwarts/internal/session"
	_ "github.com/marcboeker/go-duckdb"
)

func Connect(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to duckdb: %v", err)
	}
	err = configure(db, runtime.NumCPU())
	if err != nil {
		return nil, fmt.Errorf("Failed to config duckdb: %v", err)
	}
	return db, nil
}

func configure(db *sql.DB, threads int) error {
	query := fmt.Sprintf("SET threads=%d;", threads)
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("Failed to set threads: %v", err)
	}
	return nil
}

func InitializeLogTable(db *sql.DB) error {
	activeSessions, err := session.GetActiveSession()
	if err != nil {
		return fmt.Errorf("Failed to get active session for import: %v", err)
	}
	tableName := fmt.Sprintf("alb_logs_%s", activeSessions.Name)

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			type VARCHAR,
			time TIMESTAMP,
			elb VARCHAR,
			client VARCHAR,
			target VARCHAR,
			request_processing_time FLOAT,
			target_processing_time FLOAT,
			response_processing_time FLOAT,
			elb_status_code INTEGER,
			target_status_code VARCHAR,
			received_bytes BIGINT,
			sent_bytes BIGINT,
			request VARCHAR,
			user_agent VARCHAR,
			ssl_cipher VARCHAR,
			ssl_protocol VARCHAR,
			target_group_arn VARCHAR,
			trace_id VARCHAR,
			domain_name VARCHAR,
			chosen_cert_arn VARCHAR,
			matched_rule_priority VARCHAR,
			request_creation_time TIMESTAMP,
			actions_executed VARCHAR,
			redirect_url VARCHAR,
			error_reason VARCHAR,
			target_port_list VARCHAR,
			target_status_code_list VARCHAR,
			classification VARCHAR,
			classification_reason VARCHAR,
			conn_trace_id VARCHAR,
			unkown_field_1 VARCHAR,
			unkown_field_2 VARCHAR,
			unkown_field_3 VARCHAR,
		);
	`, tableName)
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("Failed to create log table: %v", err)
	}

	return nil
}

func ImportLogFile(db *sql.DB, logFilePath string) error {
	activeSessions, err := session.GetActiveSession()
	if err != nil {
		return fmt.Errorf("Failed to get active session for import: %v", err)
	}
	tableName := fmt.Sprintf("alb_logs_%s", activeSessions.Name)

	query := fmt.Sprintf(`
		COPY %s FROM '%s' (DELIMITER ' ', HEADER FALSE, QUOTE '"', ESCAPE '"', NULL '-');
	`, tableName, logFilePath)
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("Failed to import log file: %v", err)
	}

	return nil
}

func ImportDirectoryLogs(db *sql.DB, dirPath string, progressCallback func(current, total int)) error {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("Failed to read directory '%s': %v", dirPath, err)
	}

	var logFiles []os.DirEntry
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".csv") {
			logFiles = append(logFiles, file)
		}
	}

	total := len(logFiles)
	for i, file := range logFiles {
		filePath := filepath.Join(dirPath, file.Name())
		err := ImportLogFile(db, filePath)
		if err != nil {
			fmt.Printf("Failed to import file '%s': %v\n", filePath, err)
		}
		if progressCallback != nil {
			progressCallback(i+1, total)
		}
	}
	return nil
}

func ExecuteQuery(db *sql.DB, query string) (*sql.Rows, error) {
	return db.Query(query)
}

func DeleteLogs(db *sql.DB) error {
	activeSessions, err := session.GetActiveSession()
	if err != nil {
		return fmt.Errorf("Failed to get active session for import: %v", err)
	}
	tableName := fmt.Sprintf("alb_logs_%s", activeSessions.Name)

	query := fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName)

	_, err = db.Exec(query)
	return err
}

func GetFilteredStats(db *sql.DB, filter string) (*sql.Rows, error) {
	activeSessions, err := session.GetActiveSession()
	if err != nil {
		return nil, fmt.Errorf("Failed to get active session for import: %v", err)
	}
	tableName := fmt.Sprintf("alb_logs_%s", activeSessions.Name)

	query := fmt.Sprintf(`
	SELECT
            DATE_TRUNC('minute', time) AS minute,
            COUNT(*) AS requests,
            MIN(target_processing_time) AS min_response_time,
            MAX(target_processing_time) AS max_response_time,
            AVG(target_processing_time) AS avg_response_time
        FROM
            %s
	WHERE REGEXP_MATCHES(request, '%s')
	GROUP BY
            minute
        ORDER BY
            minute;
	`, tableName, filter)

	return db.Query(query)
}
