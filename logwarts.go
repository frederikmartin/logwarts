package logwarts

import (
	"bufio"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/frederikmartin/logwarts/table"
)

type LogEntry struct {
	Type                   string
	Timestamp              time.Time
	ELB                    string
	Client                 string
	Target                 string
	RequestProcessingTime  string
	TargetProcessingTime   string
	ResponseProcessingTime string
	ELBStatusCode          string
	TargetStatusCode       string
	ReceivedBytes          string
	SentBytes              string
	Request                string
	UserAgent              string
	SSLConnectionCipher    string
	SSLProtocol            string
	TargetGroupArn         string
	TraceID                string
	DomainName             string
	ChosenCertArn          string
	MatchedRulePriority    string
	RequestCreationTime    string
	ActionsExecuted        string
	RedirectURL            string
	LambdaErrorReason      string
	TargetPortList         string
	TargetStatusCodeList   string
	Classification         string
	ClassficationReason    string
}

type Logs []LogEntry

type FilterFunc func(*LogEntry) bool

func ParseLogs(filename string, filters []FilterFunc, processor func(*LogEntry), numWorkers int, totalEntriesProcessed *int64) error {
	file, err := os.Open(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()

	reader, err := getReader(file)
	if err != nil {
		return err
	}
	defer reader.Close()

	lineChan := make(chan string, 100)
	entryChan := make(chan *LogEntry, 100)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go logWorker(&wg, lineChan, entryChan, filters, totalEntriesProcessed)
	}

	go func() {
		s := bufio.NewScanner(reader)
		for s.Scan() {
			line := s.Text()
			lineChan <- line
		}
		close(lineChan)
	}()

	go func() {
		wg.Wait()
		close(entryChan)
	}()

	for entry := range entryChan {
		processor(entry)
	}

	if err := reader.Close(); err != nil {
		return err
	}

	return nil
}

func logWorker(wg *sync.WaitGroup, lineChan <-chan string, entryChan chan<- *LogEntry, filters []FilterFunc, totalEntriesProcessed *int64) {
	defer wg.Done()
	for line := range lineChan {
		atomic.AddInt64(totalEntriesProcessed, 1)
		fields := parseLogLine(line)
		if len(fields) >= 25 {
			timestamp, err := time.Parse(time.RFC3339Nano, fields[1])
			if err != nil {
				// TODO: Collect info about entries failed parsing and return feedback
				continue
			}
			entry := &LogEntry{
				Type:                   fields[0],
				Timestamp:              timestamp,
				ELB:                    fields[2],
				Client:                 fields[3],
				Target:                 fields[4],
				RequestProcessingTime:  fields[5],
				TargetProcessingTime:   fields[6],
				ResponseProcessingTime: fields[7],
				ELBStatusCode:          fields[8],
				TargetStatusCode:       fields[9],
				ReceivedBytes:          fields[10],
				SentBytes:              fields[11],
				Request:                strings.Trim(fields[12], `"`),
				UserAgent:              strings.Trim(fields[13], `"`),
				SSLConnectionCipher:    fields[14],
				SSLProtocol:            fields[15],
				TargetGroupArn:         fields[16],
				TraceID:                fields[17],
				DomainName:             fields[18],
				ChosenCertArn:          fields[19],
				MatchedRulePriority:    fields[20],
				RequestCreationTime:    fields[21],
				ActionsExecuted:        fields[22],
				RedirectURL:            fields[23],
				LambdaErrorReason:      fields[24],
			}

			if len(fields) > 25 {
				entry.TargetPortList = fields[25]
				entry.TargetStatusCodeList = fields[26]
				if len(fields) > 27 {
					entry.Classification = fields[27]
					if len(fields) > 28 {
						entry.ClassficationReason = fields[28]
					}
				}
			}

			include := true
			for _, filter := range filters {
				if !filter(entry) {
					include = false
					break
				}
			}

			if include {
				entryChan <- entry
			}
		}
	}
}

func parseLogLine(line string) []string {
	regex := regexp.MustCompile(`"([^"]*)"|(\S+)`)
	matches := regex.FindAllStringSubmatch(line, -1)

	var fields []string
	for _, match := range matches {
		if match[1] != "" {
			fields = append(fields, match[1])
		} else {
			fields = append(fields, match[2])
		}
	}
	return fields
}

func getReader(file *os.File) (io.ReadCloser, error) {
	buf := make([]byte, 2)
	if _, err := file.Read(buf); err != nil {
		return nil, err
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	if buf[0] == 0x1F && buf[1] == 0x8B {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, err
		}
		return gzReader, nil
	}
	return file, nil
}

func FilterByTime(start, end time.Time) FilterFunc {
	return func(entry *LogEntry) bool {
		if !start.IsZero() && entry.Timestamp.Before(start) {
			return false
		}
		if !end.IsZero() && entry.Timestamp.After(end) {
			return false
		}
		return true
	}
}

func FilterByURL(regex *regexp.Regexp) FilterFunc {
	return func(entry *LogEntry) bool {
		return regex.MatchString(entry.Request)
	}
}

func FilterByUserAgent(regex *regexp.Regexp) FilterFunc {
	return func(entry *LogEntry) bool {
		return regex.MatchString(entry.UserAgent)
	}
}

func FilterByELBStatusCode(statusCode string) FilterFunc {
	return func(entry *LogEntry) bool {
		return entry.ELBStatusCode == statusCode
	}
}

func FilterByTargetStatusCode(statusCode string) FilterFunc {
	return func(entry *LogEntry) bool {
		return entry.TargetStatusCode == statusCode
	}
}

func FilterByTargetProcessingTime(processingTime float32) FilterFunc {
	return func(entry *LogEntry) bool {
		t, err := strconv.ParseFloat(entry.TargetProcessingTime, 32)
		if err != nil {
			return false
		}
		return float32(t) >= processingTime
	}
}

func (l *Logs) PrettyPrint() {
	// TODO: Add row enumeration
	tbl := table.NewTable([]string{"Timestamp", "Client", "Target", "Request", "ELBStatusCode", "TargetStatusCode", "ReceivedBytes", "SentBytes", "TargetProcessingTime (sec)", "UserAgent", "SSLProtocol"})

	for _, entry := range *l {
		tbl.AddRow([]string{
			entry.Timestamp.Format(time.RFC3339Nano),
			entry.Client,
			entry.Target,
			entry.Request,
			entry.ELBStatusCode,
			entry.TargetStatusCode,
			entry.ReceivedBytes,
			entry.SentBytes,
			entry.TargetProcessingTime,
			entry.UserAgent,
			entry.SSLProtocol,
		})
	}

	tbl.Render()
}

func (l *Logs) PrintSimple() {
	// TODO: Add row enumeration
	tbl := table.NewTable([]string{"Timestamp", "URL", "UserAgent"})

	for _, entry := range *l {
		tbl.AddRow([]string{
			entry.Timestamp.Format(time.RFC3339Nano),
			entry.Request,
			entry.UserAgent,
		})
	}

	tbl.Render()
}
