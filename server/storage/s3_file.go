package storage

import (
	"bytes"
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3File struct {
	bytes       int64
	client      *s3.Client
	data        []byte
	name        string
	totalBytes  int64
	writeBuffer []byte
}

func (f *S3File) Close() error {
	f.data = nil

	if len(f.writeBuffer) > 0 {
		_, err := f.client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(os.Getenv("AWS_S3_BUCKET")),
			Key:    aws.String(preparePath(f.name)),
			Body:   bytes.NewReader(f.writeBuffer),
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func (f *S3File) Read(p []byte) (n int, err error) {
	if f.bytes == f.totalBytes {
		return copy(p, f.data), nil
	}

	if f.bytes < f.totalBytes {
		log.Fatal("TODO: download next chunk")
	}

	return 0, nil
}

func (f *S3File) ReadAt(p []byte, off int64) (n int, err error) {
	log.Println("****READ AT****", off, len(p))
	// response, err := f.client.GetObject(context.TODO(), &s3.GetObjectInput{
	// 	Bucket: aws.String(os.Getenv("AWS_S3_BUCKET")),
	// 	Key:    aws.String(f.name),
	// })

	// if err != nil {
	// 	return 0, err
	// }

	if len(p) > len(f.data) {
		return 0, nil
	}

	return copy(p, f.data[off:]), nil
}

func (f *S3File) Write(p []byte) (n int, err error) {
	if len(f.writeBuffer) == 0 {
		f.writeBuffer = []byte{}
	}

	// append data to the buffer
	f.writeBuffer = append(f.writeBuffer, p...)

	return len(p), nil
}

func (f *S3File) WriteAt(p []byte, off int64) (n int, err error) {
	if int(off)+len(p) > cap(f.writeBuffer) {
		newBuffer := make([]byte, off+int64(len(p)))
		copy(newBuffer, f.writeBuffer)
		f.writeBuffer = newBuffer
	}

	// Copy data to the buffer.
	copy(f.writeBuffer[off:], p)

	log.Println("WRITE AT", off, len(p))

	return len(p), nil
}

func (f *S3File) WriteString(s string) (ret int, err error) {
	return f.Write([]byte(s))
}
