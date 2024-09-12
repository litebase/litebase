package storage

type ListObjectsV2Input struct {
	ContinuationToken string
	Delimiter         string
	MaxKeys           int
	Prefix            string
	StartAfter        string
}

type ListObjectsV2Paginator struct {
	delimiter         string
	continuationToken string
	s3Client          *S3Client
}

type ListItemContent struct {
	Key string `xml:"Key"`
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
		s3Client: s3Client,
	}
}

func (p *ListObjectsV2Paginator) HasMorePages() bool {
	return p.continuationToken != ""
}

func (p *ListObjectsV2Paginator) NextPage() (ListBucketResult, error) {
	if !p.HasMorePages() {
		return ListBucketResult{}, nil
	}

	resp, err := p.s3Client.ListObjectsV2(
		ListObjectsV2Input{
			ContinuationToken: p.continuationToken,
			Delimiter:         p.delimiter,
		},
	)

	if err != nil {
		return ListBucketResult{}, err
	}

	p.continuationToken = resp.ListBucketResult.NextContinuationToken

	return resp.ListBucketResult, nil

}
