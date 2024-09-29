package s3

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3Client struct {
	Client *s3.Client
}

func NewS3Client() (*S3Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("Unable to load AWS SDK config: %v", err)
	}

	client := s3.NewFromConfig(cfg)
	return &S3Client{Client: client}, nil
}

func (s *S3Client) ListLogs(bucket, prefix string) ([]types.Object, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	output, err := s.Client.ListObjectsV2(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("Failed to list objects in bucket '%s': %v", bucket, err)
	}

	return output.Contents, nil
}

func (s *S3Client) DownloadLog(bucket, key, downloadDir string) error {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	output, err := s.Client.GetObject(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("Failed to download object '%s': %v", key, err)
	}
	defer output.Body.Close()

	filePath := filepath.Join(downloadDir, filepath.Base(key))
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("Failed to create file '%s': %v", filePath, err)
	}
	defer file.Close()

	_, err = io.Copy(file, output.Body)
	if err != nil {
		return fmt.Errorf("Failed to copy content to file '%s': %v", filePath, err)
	}

	fmt.Printf("Downloaded '%s' to '%s'\n", key, filePath)
	return nil
}

func (s *S3Client) DownloadLogs(bucket, prefix, downloadDir string) error {
	logFiles, err := s.ListLogs(bucket, prefix)
	if err != nil {
		return fmt.Errorf("Failed to list log files: %v", err)
	}

	for _, logFile := range logFiles {
		err := s.DownloadLog(bucket, *logFile.Key, downloadDir)
		if err != nil {
			fmt.Printf("Failed to download log file '%s': %v\n", *logFile.Key, err)
			continue
		}
	}

	fmt.Printf("Downloaded %d log files to '%s'\n", len(logFiles), downloadDir)
	return nil
}
