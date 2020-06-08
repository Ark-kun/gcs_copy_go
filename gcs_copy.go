//  Copyright 2020 Alexey Volkov
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

var (
	gcsRegexp = regexp.MustCompile(`^gs://([^/]*)/(.*)$`)
)

func copyFromGcsToLocal(ctx context.Context, srcBucket, srcPath, dstPath string) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	it := client.Bucket(srcBucket).Objects(ctx, &storage.Query{
		Prefix: srcPath,
	})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		// TODO: Gracefully handle the case when a file has the same name as directory
		srcFilePath := attrs.Name
		relPath := srcFilePath[len(srcPath):] // Sometimes the slashed for the root directory are reversed
		if relPath == "" {
			copyFileFromGcsToLocal(ctx, srcBucket, srcFilePath, dstPath)
		} else {
			dstFilePath := filepath.Join(dstPath, relPath)
			copyFileFromGcsToLocal(ctx, srcBucket, srcFilePath, dstFilePath)
		}

	}
	return nil
}

func copyFileFromGcsToLocal(ctx context.Context, srcBucket, srcFilePath, dstFilePath string) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf(`Copying from "gs://%s/%s" to "%s"`, srcBucket, srcFilePath, dstFilePath)
	srcReader, err := client.Bucket(srcBucket).Object(srcFilePath).NewReader(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer srcReader.Close()

	dstFileDir := filepath.Dir(dstFilePath)
	err = os.MkdirAll(dstFileDir, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}

	dstWriter, err := os.OpenFile(dstFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer dstWriter.Close()

	_, err = io.Copy(dstWriter, srcReader)
	if err != nil {
		log.Fatal(err)
	}

	err = dstWriter.Sync()
	if err != nil {
		log.Fatal(err)
	}
}

func copyFromLocalToGcs(ctx context.Context, srcPath, dstBucket, dstPath string) {
	fi, err := os.Stat(srcPath)
	if err != nil {
		log.Fatal(err)
	}

	if !fi.IsDir() {
		copyFileFromLocalToGcs(ctx, srcPath, dstBucket, dstPath)
		return
	}
	err = filepath.Walk(srcPath,
		func(srcFilePath string, info os.FileInfo, err error) error {
			if err != nil {
				log.Fatal(err)
			}
			relFilePath, err := filepath.Rel(srcPath, srcFilePath)
			if err != nil {
				log.Fatal(err)
			}
			if relFilePath != "." && !info.IsDir() {
				dstFilePath := dstPath
				if !strings.HasSuffix(dstFilePath, "/") {
					dstFilePath += "/"
				}
				dstFilePath += relFilePath
				copyFileFromLocalToGcs(ctx, srcFilePath, dstBucket, dstFilePath)
			}
			return nil
		},
	)
	if err != nil {
		log.Println(err)
	}
}

func copyFileFromLocalToGcs(ctx context.Context, srcFilePath, dstBucket, dstFilePath string) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	src, err := os.Open(srcFilePath)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf(`Copying from "%s" to "gs://%s/%s"`, srcFilePath, dstBucket, dstFilePath)
	dst := client.Bucket(dstBucket).Object(dstFilePath).NewWriter(ctx)

	_, err = io.Copy(dst, src)
	if err != nil {
		log.Fatal(err)
	}

	err = dst.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func copyFromGcsToGcs(ctx context.Context, srcBucket, srcPath, dstBucket, dstPath string) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	it := client.Bucket(srcBucket).Objects(ctx, &storage.Query{
		Prefix: srcPath,
	})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		// TODO: Gracefully handle the case when a file has the same name as directory
		srcFilePath := attrs.Name
		relFilePath := srcFilePath[len(srcPath):] // Sometimes the slashed for the root directory are reversed
		if relFilePath == "" {
			copyFileFromGcsToGcs(ctx, srcBucket, srcFilePath, dstBucket, dstPath)
		} else {
			dstFilePath := dstPath
			if !strings.HasSuffix(dstFilePath, "/") {
				dstFilePath += "/"
			}
			dstFilePath += relFilePath
			copyFileFromGcsToGcs(ctx, srcBucket, srcFilePath, dstBucket, dstFilePath)
		}

	}
	return nil
}

func copyFileFromGcsToGcs(ctx context.Context, srcBucket, srcFilePath, dstBucket, dstFilePath string) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	log.Printf(`Copying from "gs://%s/%s" to "gs://%s/%s"`, srcBucket, srcFilePath, dstBucket, dstFilePath)
	dst := client.Bucket(srcBucket).Object(srcFilePath)
	if _, err := client.Bucket(dstBucket).Object(dstFilePath).CopierFrom(dst).Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func copyFromLocalToLocal(srcPath, dstPath string) {
	fi, err := os.Stat(srcPath)
	if err != nil {
		log.Fatal(err)
	}

	if !fi.IsDir() {
		copyFileFromLocalToLocal(srcPath, dstPath)
		return
	}
	err = filepath.Walk(srcPath,
		func(srcFilePath string, info os.FileInfo, err error) error {
			if err != nil {
				log.Fatal(err)
			}
			relFilePath, err := filepath.Rel(srcPath, srcFilePath)
			if err != nil {
				log.Fatal(err)
			}
			if relFilePath != "." && !info.IsDir() {
				dstFilePath := filepath.Join(dstPath, relFilePath)
				copyFileFromLocalToLocal(srcFilePath, dstFilePath)
			}
			return nil
		},
	)
	if err != nil {
		log.Println(err)
	}
}

func copyFileFromLocalToLocal(srcFilePath, dstFilePath string) {
	src, err := os.Open(srcFilePath)
	if err != nil {
		log.Fatal(err)
	}

	dstFileDir := filepath.Dir(dstFilePath)
	err = os.MkdirAll(dstFileDir, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	dst, err := os.OpenFile(dstFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf(`Copying from "%s" to "%s"`, srcFilePath, dstFilePath)
	_, err = io.Copy(dst, src)
	if err != nil {
		log.Fatal(err)
	}

	if err := dst.Close(); err != nil {
		log.Fatal(err)
	}
}

func splitGcsPath(path string) (string, string, error) {
	matches := gcsRegexp.FindStringSubmatch(path)
	if matches != nil {
		return matches[1], matches[2], nil
	}

	return "", "", fmt.Errorf(`"%s" is not a valid GCS path`, path)
}

func main() {
	flag.Parse()

	if len(flag.Args()) < 2 {
		log.Fatalf("Usage: %s <src> <dst>", os.Args[0])
	}
	from, to := flag.Arg(0), flag.Arg(1)

	log.Printf(`From: %s`, from)
	log.Printf(`To: %s`, to)

	var srcIsGcs, dstIsGcs bool

	fromBkt, fromObj, err := splitGcsPath(from)
	if err == nil {
		srcIsGcs = true
	}

	toBkt, toObj, err := splitGcsPath(to)
	if err == nil {
		dstIsGcs = true
	}

	ctx := context.Background()
	if srcIsGcs {
		if dstIsGcs {
			copyFromGcsToGcs(ctx, fromBkt, fromObj, toBkt, toObj)
		} else {
			copyFromGcsToLocal(ctx, fromBkt, fromObj, to)
		}
	} else {
		if dstIsGcs {
			copyFromLocalToGcs(ctx, from, toBkt, toObj)
		} else {
			copyFromLocalToLocal(from, to)
		}
	}
	log.Print("Finished copying")
}
