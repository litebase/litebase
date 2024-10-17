package storage_test

import (
	"fmt"
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/storage"
	"testing"
)

func TestNewS3Client(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewS3Client(
			app.Config,
			app.Config.StorageBucket,
			"region",
		)

		if client == nil {
			t.Error("NewS3Client() returned nil")
		}
	})
}

func TestS3CopyObject(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewS3Client(
			app.Config,
			app.Config.StorageBucket,
			"region",
		)

		_, err := client.PutObject("test", []byte("test"))

		if err != nil {
			t.Errorf("PutObject() returned an error: %v", err)
		}

		err = client.CopyObject("test", "test-copy")

		if err != nil {
			t.Errorf("CopyObject() returned an error: %v", err)
		}

		getObjectResp, err := client.GetObject("test-copy")

		if err != nil {
			t.Errorf("GetObject() returned an error: %v", err)
		}

		if string(getObjectResp.Data) != "test" {
			t.Errorf("GetObject() returned unexpected data: %v", string(getObjectResp.Data))
		}
	})
}

func TestS3CreateBucket(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewS3Client(
			app.Config,
			test.CreateHash(32),
			"region",
		)

		_, err := client.CreateBucket()

		if err != nil {
			t.Errorf("CreateBucket() returned an error: %v", err)
		}
	})
}

func TestS3DeleteObject(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewS3Client(
			app.Config,
			app.Config.StorageBucket,
			"region",
		)

		_, err := client.PutObject("test", []byte("test"))

		if err != nil {
			t.Errorf("PutObject() returned an error: %v", err)
		}

		err = client.DeleteObject("test")

		if err != nil {
			t.Errorf("DeleteObject() returned an error: %v", err)
		}

		resp, err := client.GetObject("test")

		if err == nil {
			t.Error("GetObject() did not return an error")
		}

		if err != nil && resp.StatusCode != 404 {
			t.Errorf("GetObject() returned unexpected error: %v", err)
		}
	})
}

func TestS3DeleteObjects(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewS3Client(
			app.Config,
			app.Config.StorageBucket,
			"region",
		)

		_, err := client.PutObject("test", []byte("test"))

		if err != nil {
			t.Errorf("PutObject() returned an error: %v", err)
		}

		err = client.DeleteObjects([]string{"test"})

		if err != nil {
			t.Errorf("DeleteObjects() returned an error: %v", err)
		}

		getBucketResp, err := client.GetObject("test")

		if err == nil {
			t.Error("GetObject() did not return an error")
		}

		if err != nil && getBucketResp.StatusCode != 404 {
			t.Errorf("GetObject() returned unexpected error: %v", err)
		}

		_, err = client.PutObject("test1", []byte("test1"))

		if err != nil {
			t.Errorf("PutObject() returned an error: %v", err)
		}

		_, err = client.PutObject("test2", []byte("test2"))

		if err != nil {
			t.Errorf("PutObject() returned an error: %v", err)
		}

		err = client.DeleteObjects([]string{"test1", "test2"})

		if err != nil {
			t.Errorf("DeleteObjects() returned an error: %v", err)
		}

		getBucketResp, err = client.GetObject("test1")

		if err == nil {
			t.Error("GetObject() did not return an error")
		}

		if err != nil && getBucketResp.StatusCode != 404 {
			t.Errorf("GetObject() returned unexpected error: %v", err)
		}

		getBucketResp, err = client.GetObject("test2")

		if err == nil {
			t.Error("GetObject() did not return an error")
		}

		if err != nil && getBucketResp.StatusCode != 404 {
			t.Errorf("GetObject() returned unexpected error: %v", err)
		}
	})
}

func TestS3HeadBucket(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewS3Client(
			app.Config,
			app.Config.StorageBucket,
			"region",
		)

		response, err := client.HeadBucket()

		if err != nil {
			t.Errorf("HeadBucket() returned an error: %v", err)
		}

		if response == (storage.HeadBucketResponse{}) {
			t.Error("HeadBucket() returned nil")
		}

		if response != (storage.HeadBucketResponse{}) && response.StatusCode != 200 {
			t.Errorf("HeadBucket() returned unexpected status code: %d", response.StatusCode)
		}
	})
}

func TestS3HeadObject(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewS3Client(
			app.Config,
			app.Config.StorageBucket,
			"region",
		)

		_, err := client.PutObject("test", []byte("test"))

		if err != nil {
			t.Errorf("PutObject() returned an error: %v", err)
		}

		resp, err := client.HeadObject("test")

		if err != nil {
			t.Errorf("HeadObject() returned an error: %v", err)
		}

		if resp == (storage.HeadObjectResponse{}) {
			t.Error("HeadObject() returned nil")
		}

		if resp != (storage.HeadObjectResponse{}) && resp.StatusCode != 200 {
			t.Errorf("HeadObject() returned unexpected status code: %d", resp.StatusCode)
		}
	})
}

func TestS3GetObjectAndPutObject(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewS3Client(
			app.Config,
			app.Config.StorageBucket,
			"region",
		)

		_, err := client.PutObject("test", []byte("test"))

		if err != nil {
			t.Errorf("PutObject() returned an error: %v", err)
		}

		resp, err := client.GetObject("test")

		if err != nil {
			t.Errorf("GetObject() returned an error: %v", err)
		}

		if string(resp.Data) != "test" {
			t.Errorf("GetObject() returned unexpected data: %v", string(resp.Data))
		}
	})
}

func TestS3ListObjectsV2(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewS3Client(
			app.Config,
			app.Config.StorageBucket,
			"region",
		)

		_, err := client.PutObject("test", []byte("test"))

		if err != nil {
			t.Errorf("PutObject() returned an error: %v", err)
		}

		resp, err := client.ListObjectsV2(storage.ListObjectsV2Input{
			Delimiter: "/",
			MaxKeys:   100,
			Prefix:    "/",
		})

		if err != nil {
			t.Errorf("ListObjectsV2() returned an error: %v", err)
		}

		if len(resp.ListBucketResult.Contents) == 0 {
			t.Errorf("ListObjectsV2() returned unexpected number of objects: %d", len(resp.ListBucketResult.Contents))
		}

		found := false

		for _, content := range resp.ListBucketResult.Contents {
			if content.Key == "test" {
				found = true
			}
		}

		if !found {
			t.Error("ListObjectsV2() did not return the expected object")
		}
	})
}

func TestS3ListObjectsV2WithPaginator(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewS3Client(
			app.Config,
			app.Config.StorageBucket,
			"region",
		)

		files := make(map[string]bool, 2000)

		for i := 0; i < 2000; i++ {
			files[fmt.Sprintf("test/file-%d", i)] = false
		}

		for key := range files {
			_, err := client.PutObject(key, []byte("test"))

			if err != nil {
				t.Errorf("PutObject() returned an error: %v", err)
			}
		}

		paginator := storage.NewListObjectsV2Paginator(client, storage.ListObjectsV2Input{
			Delimiter: "/",
			MaxKeys:   1,
			Prefix:    "test/",
		})

		for paginator.HasMorePages() {
			response, err := paginator.NextPage()

			if err != nil {
				t.Errorf("NextPage() returned an error: %v", err)
			}

			if len(response.ListBucketResult.Contents) == 0 {
				break
			}

			if len(response.ListBucketResult.Contents) > 10 {
				t.Errorf("NextPage() returned unexpected number of objects: %d", len(response.ListBucketResult.Contents))
			}

			for _, object := range response.ListBucketResult.Contents {
				if _, ok := files[object.Key]; ok {
					files[object.Key] = true
				}
			}
		}

		for key, found := range files {
			if !found {
				t.Errorf("ListObjectsV2() did not return the expected object: %s", key)
			}
		}
	})
}
