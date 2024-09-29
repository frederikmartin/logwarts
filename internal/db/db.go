package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/frederikmartin/logwarts/internal/session"
	_ "github.com/marcboeker/go-duckdb"
)

func Connect(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to duckdb: %v", err)
	}
	return db, nil
}

func ImportLogFile(db *sql.DB, logFilePath string) error {
	activeSessions, err := session.GetActiveSession()
	if err != nil {
		return fmt.Errorf("Failed to get active session for import: %v", err)
	}
	tableName := fmt.Sprintf("alb_logs_%s", activeSessions.Name)

	query := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s AS SELECT
		type,
		time,
		elb,
		SPLIT_PART("client:port", ':', 1) AS client_ip,
		CAST(SPLIT_PART("client:port", ':', -1) AS INTEGER) AS client_port,
		CASE WHEN "target:port" = '-' THEN NULL ELSE SPLIT_PART("target:port", ':', 1) END AS target_ip,
		CASE WHEN "target:port" = '-' THEN NULL ELSE CAST(SPLIT_PART("target:port", ':', -1) AS INTEGER) END AS target_port,
		COALESCE(request_processing_time, -1) AS request_processing_time,
		COALESCE(target_processing_time, -1) AS target_processing_time,
		COALESCE(response_processing_time, -1) AS response_processing_time,
		COALESCE(elb_status_code, NULL) AS elb_status_code,
		COALESCE(target_status_code, NULL) AS target_status_code,
		COALESCE(received_bytes, 0) AS received_bytes,
		COALESCE(sent_bytes, 0) AS sent_bytes,
		request,
		user_agent,
		COALESCE(ssl_cipher, NULL) AS ssl_cipher,
		COALESCE(ssl_protocol, NULL) AS ssl_protocol,
		COALESCE(target_group_arn, NULL) AS target_group_arn,
		COALESCE(trace_id, NULL) AS trace_id,
		COALESCE(domain_name, NULL) AS domain_name,
		COALESCE(chosen_cert_arn, NULL) AS chosen_cert_arn,
		COALESCE(matched_rule_priority, NULL) AS matched_rule_priority,
		request_creation_time,
		COALESCE(actions_executed, NULL) AS actions_executed,
		COALESCE(redirect_url, NULL) AS redirect_url,
		COALESCE(error_reason, NULL) AS error_reason,
		COALESCE("target:port_list", NULL) AS target_port_list,
		COALESCE(target_status_code_list, NULL) AS target_status_code_list,
		COALESCE(classification, NULL) AS classification,
		COALESCE(classification_reason, NULL) AS classification_reason,
		COALESCE(conn_trace_id, NULL) AS conn_trace_id
	FROM read_csv_auto(
		'%s', 
		delim=' ',
		types={
			'type': 'VARCHAR',
			'time': 'TIMESTAMP',
			'elb': 'VARCHAR',
			'client:port': 'VARCHAR',
			'target:port': 'VARCHAR',
			'request_processing_time': 'FLOAT',
			'target_processing_time': 'FLOAT',
			'response_processing_time': 'FLOAT',
			'elb_status_code': 'INTEGER',
			'target_status_code': 'VARCHAR',
			'received_bytes': 'BIGINT',
			'sent_bytes': 'BIGINT',
			'request': 'VARCHAR',
			'user_agent': 'VARCHAR',
			'ssl_cipher': 'VARCHAR',
			'ssl_protocol': 'VARCHAR',
			'target_group_arn': 'VARCHAR',
			'trace_id': 'VARCHAR',
			'domain_name': 'VARCHAR',
			'chosen_cert_arn': 'VARCHAR',
			'matched_rule_priority': 'VARCHAR',
			'request_creation_time': 'TIMESTAMP',
			'actions_executed': 'VARCHAR',
			'redirect_url': 'VARCHAR',
			'error_reason': 'VARCHAR',
			'target:port_list': 'VARCHAR',
			'target_status_code_list': 'VARCHAR',
			'classification': 'VARCHAR',
			'classification_reason': 'VARCHAR',
			'conn_trace_id': 'VARCHAR',
		},
		names=[
			'type',
			'time',
			'elb',
			'client:port',
			'target:port',
			'request_processing_time',
			'target_processing_time',
			'response_processing_time',
			'elb_status_code',
			'target_status_code',
			'received_bytes',
			'sent_bytes',
			'request',
			'user_agent',
			'ssl_cipher',
			'ssl_protocol',
			'target_group_arn',
			'trace_id',
			'domain_name',
			'chosen_cert_arn',
			'matched_rule_priority',
			'request_creation_time',
			'actions_executed',
			'redirect_url',
			'error_reason',
			'target:port_list',
			'target_status_code_list',
			'classification',
			'classification_reason',
			'conn_trace_id',
		]
	)`, tableName, logFilePath)
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("Failed to import log file: %v", err)
	}
	fmt.Printf("Imported log file: %s\n", logFilePath)
	return nil
}

func ImportDirectoryLogs(db *sql.DB, dirPath string) error {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("Failed to read directory '%s': %v", dirPath, err)
	}

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".csv") {
			filePath := filepath.Join(dirPath, file.Name())
			err := ImportLogFile(db, filePath)
			if err != nil {
				fmt.Printf("Failed to import file '%s': %v\n", filePath, err)
				continue
			}
		}
	}
	return nil
}

func ExecuteQuery(db *sql.DB, query string) (*sql.Rows, error) {
	return db.Query(query)
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
