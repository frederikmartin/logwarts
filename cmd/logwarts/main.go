package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/frederikmartin/logwarts/internal/db"
	"github.com/frederikmartin/logwarts/internal/output"
	"github.com/frederikmartin/logwarts/internal/s3"
	"github.com/frederikmartin/logwarts/internal/session"
	"github.com/spf13/cobra"
)

var (
	bucket             string
	prefix             string
	downloadDir        string
	source             string
	statsRequestFilter string
)

var rootCmd = &cobra.Command{
	Use:   "logwarts",
	Short: "Logwarts is a CLI tool designed for efficient and magical processing of AWS Application Load Balancer (ALB) log files. Inspired by the wizarding world, Logwarts aims to bring a bit of magic to your log analysis tasks",
}

func main() {
	if err := session.Init(); err != nil {
		fmt.Println("Failed to initialize session management:", err)
		os.Exit(1)
	}
	defer session.Close()

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	importCmd.Flags().StringVarP(&source, "source", "s", "s3", "Source of the logs: 's3' or 'local' (local files are read from stdin)")

	importCmd.Flags().StringVarP(&bucket, "bucket", "b", "", "S3 bucket name")
	importCmd.Flags().StringVarP(&prefix, "prefix", "p", "", "S3 prefix (folder path) for ALB logs")
	importCmd.Flags().StringVarP(&downloadDir, "download-dir", "d", "./logs", "Local directory to store downloaded logs")

	statsCmd.Flags().StringVarP(&statsRequestFilter, "filter", "f", "", "Regex pattern to filter requests")

	rootCmd.AddCommand(sessionCmd, importCmd, queryCmd, statsCmd)
}

var sessionCmd = &cobra.Command{
	Use:   "session [create|attach|list|kill] name",
	Short: "Manage sessions (create, attach, list, kill)",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		action := args[0]
		switch action {
		case "create":
			if len(args) < 2 {
				fmt.Println("Session name is required for 'create'")
				return
			}
			if err := session.CreateSession(args[1]); err != nil {
				fmt.Println("Error creating session:", err)
			}

			dbConn, err := db.Connect("logwarts.duckdb")
			if err != nil {
				fmt.Printf("Failed to connect to db: %v\n", err)
				return
			}
			defer dbConn.Close()
			err = db.InitializeLogTable(dbConn)
			if err != nil {
				fmt.Printf("Failed to initialize log table: %v\n", err)
				return
			}
		case "attach":
			if len(args) < 2 {
				fmt.Println("Session name is required for 'attach'")
				return
			}
			if err := session.AttachSession(args[1]); err != nil {
				fmt.Println("Error attaching to session:", err)
			}
		case "list":
			sessions, err := session.ListSessions()
			if err != nil {
				fmt.Println("Error listing sessions:", err)
				return
			}
			if len(sessions) < 1 {
				fmt.Println("No sessions available")
				return
			}
			fmt.Println("Sessions:")
			for _, session := range sessions {
				if session.State == "active" {
					fmt.Printf("- %s (active)\n", session.Name)
				} else {
					fmt.Printf("- %s\n", session.Name)
				}
			}
		case "kill":
			dbConn, err := db.Connect("logwarts.duckdb")
			if err != nil {
				fmt.Printf("Failed to connect to db: %v\n", err)
				return
			}
			defer dbConn.Close()

			err = db.DeleteLogs(dbConn)
			if err != nil {
				fmt.Println("Error killing session's logs:", err)
				return
			}
			err = session.KillSession()
			if err != nil {
				fmt.Println("Error killing current session:", err)
				return
			}
		default:
			fmt.Println("Unknown session command. Use 'create', 'attach', 'list', or 'kill'")
		}
	},
}

var importCmd = &cobra.Command{
	Use:   "import [log file]",
	Short: "Import ALB logs",
	Run: func(cmd *cobra.Command, args []string) {
		if source == "s3" {
			if bucket == "" || prefix == "" || downloadDir == "" {
				fmt.Println("Bucket, prefix, and download-dir are required flags for importing from S3")
				return
			}

			s3Client, err := s3.NewS3Client()
			if err != nil {
				fmt.Printf("Failed to create S3 client: %v\n", err)
				return
			}

			err = s3Client.DownloadLogs(bucket, prefix, downloadDir)
			if err != nil {
				fmt.Printf("Failed to download logs: %v\n", err)
				return
			}

			dbConn, err := db.Connect("logwarts.duckdb")
			if err != nil {
				fmt.Printf("Failed to connect to db: %v\n", err)
				return
			}
			defer dbConn.Close()

			err = db.ImportDirectoryLogs(dbConn, downloadDir)
			if err != nil {
				fmt.Printf("Failed to import logs from directory: %v\n", err)
				return
			}
			fmt.Printf("Successfully imported logs from S3 to db\n")

		} else if source == "local" {
			var files []string
			s := bufio.NewScanner(os.Stdin)
			for s.Scan() {
				filename := strings.TrimSpace(s.Text())
				if filename != "" {
					files = append(files, filename)
				}
			}
			if err := s.Err(); err != nil {
				fmt.Fprintf(os.Stderr, "Error reading from stdin: %v\n", err)
				os.Exit(1)
			}

			dbConn, err := db.Connect("logwarts.duckdb")
			if err != nil {
				fmt.Printf("Failed to connect to db: %v\n", err)
				return
			}
			defer dbConn.Close()

			for _, filePath := range files {
				err := db.ImportLogFile(dbConn, filePath)
				if err != nil {
					fmt.Printf("Failed to import file '%s': %v\n", filePath, err)
					continue
				}
			}
			fmt.Printf("Successfully imported %d file(s)\n", len(files))

		} else {
			fmt.Println("Invalid source specified. Use 's3' or 'local'.")
		}
	},
}

var queryCmd = &cobra.Command{
	Use:   "query [SQL]",
	Short: "Run a SQL query against database",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		activeSessions, err := session.GetActiveSession()
		if err != nil {
			fmt.Errorf("Failed to get active session for import: %v", err)
			os.Exit(1)
		}
		tableName := fmt.Sprintf("alb_logs_%s", activeSessions.Name)

		sqlQuery := strings.Replace(args[0], "alb_logs", tableName, 1)

		dbConn, err := db.Connect("logwarts.duckdb")
		if err != nil {
			fmt.Printf("Failed to connect to db: %v\n", err)
			os.Exit(1)
		}
		defer dbConn.Close()

		rows, err := db.ExecuteQuery(dbConn, sqlQuery)
		if err != nil {
			fmt.Printf("Failed to execute query: %v\n", err)
			os.Exit(1)
		}
		defer rows.Close()

		err = displayResults(rows)
		if err != nil {
			fmt.Printf("Failed to display results: %v\n", err)
			os.Exit(1)
		}
	},
}

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show performance statistics",
	Run: func(cmd *cobra.Command, args []string) {
		dbConn, err := db.Connect("logwarts.duckdb")
		if err != nil {
			fmt.Printf("Failed to connect to db: %v\n", err)
			os.Exit(1)
		}
		defer dbConn.Close()

		sanitizedFilter, err := sanitizeRegex(statsRequestFilter)
		if err != nil {
			fmt.Printf("Filter is not a valid regex pattern: %v", err)
			os.Exit(1)
		}
		stats, err := db.GetFilteredStats(dbConn, sanitizedFilter)
		if err != nil {
			fmt.Printf("Failed to retrieve stats: %v\n", err)
			os.Exit(1)
		}

		err = displayResults(stats)
		if err != nil {
			fmt.Printf("Failed to display results: %v\n", err)
			os.Exit(1)
		}
	},
}

func sanitizeRegex(pattern string) (string, error) {
	_, err := regexp.Compile(pattern)
	if err != nil {
		return "", err
	}
	return pattern, nil
}

func displayResults(rows *sql.Rows) error {
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("Failed to get columns: %v", err)
	}

	tbl := output.NewTable(columns)

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return fmt.Errorf("Failed to scan row: %v", err)
		}

		row := make([]string, len(columns))
		for i, val := range values {
			if val == nil {
				row[i] = "NULL"
			} else {
				row[i] = fmt.Sprintf("%v", val)
			}
		}
		tbl.AddRow(row)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("Error during rows iteration: %v", err)
	}

	tbl.Render()

	return nil
}
