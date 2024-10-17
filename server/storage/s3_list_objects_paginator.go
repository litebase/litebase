package storage

import "time"

type ListObjectsV2Input struct {
	ContinuationToken string
	Delimiter         string
	MaxKeys           int
	Prefix            string
	StartAfter        string
}

type ListObjectsV2Paginator struct {
	ContinuationToken string
	Delimiter         string
	IsTruncated       bool
	MaxKeys           int
	Prefix            string
	S3Client          *S3Client
}

type ListItemContent struct {
	ETag         string              `xml:"ETag"`
	Key          string              `xml:"Key"`
	LastModified ListItemContentTime `xml:"LastModified"`
	Size         int64               `xml:"Size"`
	StorageClass string              `xml:"omitempty"`
}

type ListItemContentTime struct {
	time.Time
}

type ListBucketResult struct {
	CommonPrefixes        []string          `xml:"CommonPrefixes>Prefix"`
	Contents              []ListItemContent `xml:"Contents"`
	ContinuationToken     string            `xml:"ContinuationToken"`
	IsTruncated           bool              `xml:"IsTruncated"`
	Name                  string            `xml:"Name"`
	Prefix                string            `xml:"Prefix"`
	Delimiter             string            `xml:"Delimiter"`
	MaxKeys               int               `xml:"MaxKeys"`
	NextContinuationToken string            `xml:"NextContinuationToken"`
}

type ListObjectsV2Response struct {
	ListBucketResult ListBucketResult `xml:"ListBucketResult"`
	StatusCode       int              `xml:"-"`
}

func NewListObjectsV2Paginator(s3Client *S3Client, input ListObjectsV2Input) *ListObjectsV2Paginator {
	return &ListObjectsV2Paginator{
		Delimiter:   input.Delimiter,
		MaxKeys:     input.MaxKeys,
		Prefix:      input.Prefix,
		IsTruncated: true,
		S3Client:    s3Client,
	}
}

func (p *ListObjectsV2Paginator) HasMorePages() bool {
	return p.IsTruncated
}

func (p *ListObjectsV2Paginator) NextPage() (ListObjectsV2Response, error) {
	if !p.HasMorePages() {
		return ListObjectsV2Response{}, nil
	}

	resp, err := p.S3Client.ListObjectsV2(
		ListObjectsV2Input{
			ContinuationToken: p.ContinuationToken,
			Delimiter:         p.Delimiter,
			MaxKeys:           p.MaxKeys,
			Prefix:            p.Prefix,
			StartAfter:        p.ContinuationToken,
		},
	)

	if err != nil {
		return resp, err
	}

	p.ContinuationToken = resp.ListBucketResult.NextContinuationToken

	if !resp.ListBucketResult.IsTruncated {
		p.IsTruncated = false
	}

	return resp, nil
}
