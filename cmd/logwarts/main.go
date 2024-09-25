package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/frederikmartin/logwarts"
)

func main() {
	input := flag.Bool("input", false, "Read filenames from stdin")
	limit := flag.Int("limit", 0, "Limit number of log entries for output (0 for all)")
	simple := flag.Bool("simple", false, "Show only the URL and the timestamp")

	startTimeStr := flag.String("start", "", "Start timestamp (inclusive) for filtering (RFC3339 format)")
	endTimeStr := flag.String("end", "", "End timestamp (inclusive) for filtering (RFC3339 format)")
	urlFilterStr := flag.String("url-filter", "", "Regex pattern to filter URLs")
	userAgentFilterStr := flag.String("user-agent-filter", "", "Regex pattern to filter user agents")
	elbStatusCodeFilter := flag.String("elb-status-code-filter", "", "ELB status code to filter")
	targetStatusCodeFilter := flag.String("target-status-code-filter", "", "Target status code to filter")
	targetProcessingTimeFilter := flag.String("target-processing-time-filter", "", "Min number of seconds target needed to process request")

	sortField := flag.String("sort-field", "", "Field to sort by (timestamp or target-processing-time)")
	sortOrder := flag.String("sort-order", "asc", "Sort order (asc or desc)")

	flag.Parse()

	filters := []logwarts.FilterFunc{}

	if *startTimeStr != "" || *endTimeStr != "" {
		var parsedStartTime, parsedEndTime time.Time
		var err error

		if *startTimeStr != "" {
			parsedStartTime, err = time.Parse(time.RFC3339, *startTimeStr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid start timestamp format: %v\n", err)
				os.Exit(1)
			}
		}

		if *endTimeStr != "" {
			parsedEndTime, err = time.Parse(time.RFC3339, *endTimeStr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid end timestamp format: %v\n", err)
				os.Exit(1)
			}
		}

		filters = append(filters, logwarts.FilterByTime(parsedStartTime, parsedEndTime))
	}

	if *urlFilterStr != "" {
		urlFilterRegex, err := regexp.Compile(*urlFilterStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid URL regex pattern: %v\n", err)
			os.Exit(1)
		}
		filters = append(filters, logwarts.FilterByURL(urlFilterRegex))
	}

	if *userAgentFilterStr != "" {
		userAgentFilterRegex, err := regexp.Compile(*userAgentFilterStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid user agent regex pattern: %v\n", err)
			os.Exit(1)
		}
		filters = append(filters, logwarts.FilterByUserAgent(userAgentFilterRegex))
	}

	if *elbStatusCodeFilter != "" {
		filters = append(filters, logwarts.FilterByELBStatusCode(*elbStatusCodeFilter))
	}

	if *targetStatusCodeFilter != "" {
		filters = append(filters, logwarts.FilterByTargetStatusCode(*targetStatusCodeFilter))
	}

	if *targetProcessingTimeFilter != "" {
		t, err := strconv.ParseFloat(*targetProcessingTimeFilter, 32)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid target processing time: %v\n", err)
			os.Exit(1)
		}
		filters = append(filters, logwarts.FilterByTargetProcessingTime(float32(t)))
	}

	var filenames []string
	if *input {
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			filename := strings.TrimSpace(s.Text())
			if filename != "" {
				filenames = append(filenames, filename)
			}
		}
		if err := s.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Error reading from stdin: %v\n", err)
			os.Exit(1)
		}
	} else {
		filenames = flag.Args()
	}

	if len(filenames) == 0 {
		fmt.Fprintln(os.Stderr, "No input files provided")
		os.Exit(1)
	}

	var (
		entries      = make(logwarts.Logs, 0)
		entriesMutex = &sync.Mutex{}
	)

	numWorkers := runtime.NumCPU()
	var (
		totalEntriesFound     int64
		totalEntriesProcessed int64
	)
	startTime := time.Now()

	processor := func(entry *logwarts.LogEntry) {
		entriesMutex.Lock()
		defer entriesMutex.Unlock()
		if *limit > 0 && len(entries) >= *limit {
			return
		}
		entries = append(entries, *entry)
		atomic.AddInt64(&totalEntriesFound, 1)
	}

	for _, filename := range filenames {
		err := logwarts.ParseLogs(filename, filters, processor, numWorkers, &totalEntriesProcessed)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing log file '%s': %v\n", filename, err)
			os.Exit(1)
		}
		entriesMutex.Lock()
		if *limit > 0 && len(entries) >= *limit {
			entriesMutex.Unlock()
			break
		}
		entriesMutex.Unlock()
	}

	if *sortField != "" {
		sortEntries(entries, *sortField, *sortOrder)
	}

	if len(entries) > 0 {
		var subLogs logwarts.Logs
		if *limit > 0 {
			subLogs = entries[:min(*limit, len(entries))]
		} else {
			subLogs = entries
		}

		if *simple {
			subLogs.PrintSimple()
		} else {
			subLogs.PrettyPrint()
		}
	} else {
		fmt.Println("No matching log entries found")
	}

	elapsedTime := time.Since(startTime)
	fmt.Printf("\nStatistics:\n")
	fmt.Printf("Total log entries found: %d\n", totalEntriesFound)
	fmt.Printf("Total log entries processed: %d\n", totalEntriesProcessed)
	fmt.Printf("Processing time: %s\n", elapsedTime)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func sortEntries(entries []logwarts.LogEntry, sortField, sortOrder string) {
	lessFunc := func(i, j int) bool {
		switch sortField {
		case "timestamp":
			if sortOrder == "asc" {
				return entries[i].Timestamp.Before(entries[j].Timestamp)
			}
			return entries[i].Timestamp.After(entries[j].Timestamp)
		case "target-processing-time":
			ti, err1 := strconv.ParseFloat(entries[i].TargetProcessingTime, 64)
			tj, err2 := strconv.ParseFloat(entries[j].TargetProcessingTime, 64)
			if err1 != nil || err2 != nil {
				ti, tj = 0.0, 0.0
			}
			if sortOrder == "asc" {
				return ti < tj
			}
			return ti > tj
		default:
			return entries[i].Timestamp.Before(entries[j].Timestamp)
		}
	}

	sort.Slice(entries, lessFunc)
}
