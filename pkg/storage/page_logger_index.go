package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/internal/utils"
)

// This index is used to track the versions of pages in the distributed log.
// On disk, the format is as follows:
// 1. PageLoggerIndex Version (uint32)
// 2. Number of PageGroups (uint32)
// 3. For each PageGroup:
//    1. PageGroup (uint64)
//    2. The length of each page group (uint32)
//    3. For each page in the group:
//       1. PageNumber (uint64)
//       2. The length of each version list (uint32)
//       3. For each version (uint64)

const PageLoggerIndexVersion uint32 = 1

type PageLoggerIndex struct {
	boundary   PageGroupVersion
	mutex      *sync.Mutex
	file       storage.File
	tieredFS   *FileSystem
	path       string
	pageGroups map[PageGroup]map[PageGroupVersion][]PageNumber
}

type PageGroupVersionByTimestamp struct {
	pageGroup        PageGroup
	pageGroupVersion PageGroupVersion
}

func NewPageLoggerIndex(tieredFS *FileSystem, path string) (*PageLoggerIndex, error) {
	pli := &PageLoggerIndex{
		boundary:   PageGroupVersion(0),
		mutex:      &sync.Mutex{},
		tieredFS:   tieredFS,
		path:       path,
		pageGroups: make(map[PageGroup]map[PageGroupVersion][]PageNumber),
	}

	// Load the index data from the file
	err := pli.load()

	if err != nil {
		slog.Error("Error loading page logger index", "error", err)
		return nil, err
	}

	return pli, nil
}

func (pli *PageLoggerIndex) Close() error {
	pli.mutex.Lock()
	defer pli.mutex.Unlock()

	if pli.file != nil {
		defer func() {
			pli.file = nil
		}()

		return pli.file.Close()
	}

	return nil
}

func (pli *PageLoggerIndex) File() storage.File {
	if pli.file == nil {
		var err error

	tryOpen:
		pli.file, err = pli.tieredFS.OpenFile(pli.path, os.O_RDWR|os.O_CREATE, 0600)

		if err != nil {
			if os.IsNotExist(err) {
				err = pli.tieredFS.MkdirAll(filepath.Dir(pli.path), 0750)

				if err != nil {
					return nil
				}

				goto tryOpen
			}

			return nil
		}
	}

	return pli.file
}

// Find the closest version for the given log group and version.
func (pli *PageLoggerIndex) Find(pageGroup PageGroup, pageNumber PageNumber, version PageVersion) (int64, bool, error) {
	pli.mutex.Lock()
	defer pli.mutex.Unlock()

	if _, ok := pli.pageGroups[pageGroup]; ok {
		pageGroupVersions := make([]PageGroupVersion, 0)

		for pageGroupVersion := range pli.pageGroups[pageGroup] {
			pageGroupVersions = append(pageGroupVersions, pageGroupVersion)
		}

		slices.Sort(pageGroupVersions)

		for i := len(pageGroupVersions) - 1; i >= 0; i-- {
			v := pageGroupVersions[i]

			if int64(v) <= int64(version) || version == 0 {
				if slices.Contains(pli.pageGroups[pageGroup][v], pageNumber) {
					return int64(v), true, nil
				}
				// Continue searching in older versions instead of returning false immediately
			}
		}
	}

	return 0, false, nil
}

func (pli *PageLoggerIndex) getPageGroupByTimestamp(pageVersion PageVersion) []PageGroupVersionByTimestamp {
	versions := []PageGroupVersionByTimestamp{}

	for pageGroupNumber, pageGroup := range pli.pageGroups {
		for pageGroupVersion := range pageGroup {
			if pageGroupVersion <= PageGroupVersion(pageVersion) {
				versions = append(versions, PageGroupVersionByTimestamp{
					pageGroup:        pageGroupNumber,
					pageGroupVersion: pageGroupVersion,
				})
			}
		}
	}

	return versions
}

// Load the index data from binary data.
func (pli *PageLoggerIndex) load() error {
	file := pli.File()

	if file == nil {
		return errors.New("failed to get file")
	}

	pli.pageGroups = make(map[PageGroup]map[PageGroupVersion][]PageNumber)

	// Read total length, page group count, and metadata for first page group
	buffer := make([]byte, 24)

	_, err := file.Seek(0, io.SeekStart)

	if err != nil {
		log.Println("Error seeking to page logger index:", err)
		return err
	}

	_, err = file.Read(buffer)

	if err != nil {
		if err == io.EOF {
			return nil
		}

		return err
	}

	pageGroupSize := binary.LittleEndian.Uint32(buffer[4:8])

	if pageGroupSize == 0 {
		return nil
	}

	pageGroupCount := binary.LittleEndian.Uint32(buffer[8:12])

	nextPageGroupIdInt64, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(buffer[12:20]))
	if err != nil {
		slog.Error("Error reading next page group ID from index", "error", err)
		return err
	}
	nextPageGroupId := PageGroup(nextPageGroupIdInt64)

	nextPageGroupLength := binary.LittleEndian.Uint32(buffer[20:24])

	for i := uint32(0); i < pageGroupCount; i++ {
		if pli.pageGroups[nextPageGroupId] == nil {
			pli.pageGroups[nextPageGroupId] = make(map[PageGroupVersion][]PageNumber)
		}

		// Create a buffer for the current page group data
		data := make([]byte, nextPageGroupLength)

		// Read the pageGroup
		n, err := file.Read(data)

		if err != nil && err != io.EOF {
			log.Println("Error reading page group data:", err)
			return err
		}

		if n == 0 {
			break
		}

		// Ensure we only process the bytes that were actually read
		actualDataLength := n
		if actualDataLength > int(nextPageGroupLength) {
			actualDataLength = int(nextPageGroupLength)
		}

		pageGroupBytesProcessed := 0

		for pageGroupBytesProcessed < actualDataLength {
			// Ensure we have enough bytes for page group version
			if pageGroupBytesProcessed+8 > actualDataLength {
				break
			}

			pageGroupVersionNumberInt64, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[pageGroupBytesProcessed : pageGroupBytesProcessed+8]))
			if err != nil {
				slog.Error("Error reading next page group version from index", "error", err)
				return err
			}
			pageGroupVersionNumber := PageGroupVersion(pageGroupVersionNumberInt64)
			pageGroupBytesProcessed += 8

			// Ensure we have enough bytes for version data length
			if pageGroupBytesProcessed+4 > actualDataLength {
				break
			}

			versionDataLength := binary.LittleEndian.Uint32(data[pageGroupBytesProcessed : pageGroupBytesProcessed+4])
			pageGroupBytesProcessed += 4

			if pli.pageGroups[nextPageGroupId][pageGroupVersionNumber] == nil {
				pli.pageGroups[nextPageGroupId][pageGroupVersionNumber] = []PageNumber{}
			}

			for versionBytesProcessed := uint32(0); versionBytesProcessed < versionDataLength; versionBytesProcessed += 8 {
				// Ensure we have enough bytes for page number
				if pageGroupBytesProcessed+8 > actualDataLength {
					break
				}

				pageNumberInt64, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[pageGroupBytesProcessed : pageGroupBytesProcessed+8]))
				if err != nil {
					slog.Error("Error reading page number from index", "error", err)
					return err
				}
				pageNumber := PageNumber(pageNumberInt64)
				pageGroupBytesProcessed += 8
				pli.pageGroups[nextPageGroupId][pageGroupVersionNumber] = append(pli.pageGroups[nextPageGroupId][pageGroupVersionNumber], pageNumber)
			}

			slices.Sort(pli.pageGroups[nextPageGroupId][pageGroupVersionNumber])
		}

		// Check if we need to read more page groups
		if i < pageGroupCount-1 {
			// Read the next page group metadata (ID and length)
			nextPageGroupMetadata := make([]byte, 12) // 8 bytes for ID + 4 bytes for length
			n, err := file.Read(nextPageGroupMetadata)

			if err != nil && err != io.EOF {
				log.Println("Error reading next page group metadata:", err)
				return err
			}

			if n < 12 {
				break // Not enough data for next page group
			}

			pageGroupInt64, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(nextPageGroupMetadata[0:8]))
			if err != nil {
				slog.Error("Error reading page group ID from index", "error", err)
				return err
			}

			nextPageGroupId = PageGroup(pageGroupInt64)

			nextPageGroupLengthInt32, err := utils.SafeUint32ToInt32(binary.LittleEndian.Uint32(nextPageGroupMetadata[8:12]))
			if err != nil {
				slog.Error("Error reading next page group length from index", "error", err)
				return err
			}

			nextPageGroupLength = uint32(nextPageGroupLengthInt32)
		}
	}

	// Set the boundary to the latest page group version
	if len(pli.pageGroups) > 0 {
		for _, pageGroup := range pli.pageGroups {
			for pageGroupVersion := range pageGroup {
				if pageGroupVersion > pli.boundary {
					pli.boundary = pageGroupVersion
				}
			}
		}
	}

	return nil
}

func (pli *PageLoggerIndex) Push(pageGroup PageGroup, pageNumber PageNumber, version PageGroupVersion) error {
	pli.mutex.Lock()
	defer pli.mutex.Unlock()

	if pli.pageGroups[pageGroup] == nil {
		pli.pageGroups[pageGroup] = make(map[PageGroupVersion][]PageNumber)
	}

	if pli.pageGroups[pageGroup][version] == nil {
		pli.pageGroups[pageGroup][version] = []PageNumber{}
	}

	// Check if version already exists
	if slices.Contains(pli.pageGroups[pageGroup][version], pageNumber) {
		return nil
	}

	pli.pageGroups[pageGroup][version] = append(pli.pageGroups[pageGroup][version], pageNumber)

	return pli.store()
}

func (pli *PageLoggerIndex) removePageLogs(pageLogEntries []PageLogEntry) error {
	for _, entry := range pageLogEntries {
		if pli.pageGroups[entry.pageGroup] == nil {
			continue
		}

		if pli.pageGroups[entry.pageGroup][entry.pageGroupVersion] == nil {
			continue
		}

		delete(pli.pageGroups[entry.pageGroup], entry.pageGroupVersion)
	}

	return pli.store()
}

// Store the index data as binary data. Each version is uint64. We need to store
// the length of the version list as uint64 followed by the versions.
func (pli *PageLoggerIndex) store() error {
	pageGroupCount := len(pli.pageGroups)

	// Calculate total size
	totalSize := 4 // Version field
	totalSize += 4 // Total length field
	totalSize += 4 // Page group count field

	for _, pages := range pli.pageGroups {
		totalSize += 8 + 4 // Page group ID + length of page group data

		for _, versions := range pages {
			totalSize += 8 + 4             // Page number + length of version data
			totalSize += len(versions) * 8 // Versions
		}
	}

	// Allocate binary data
	binaryData := make([]byte, totalSize)
	offset := 0

	// Write version
	binary.LittleEndian.PutUint32(binaryData[offset:offset+4], PageLoggerIndexVersion)
	offset += 4

	// Write total length
	uint32TotalSize, err := utils.SafeIntToUint32(totalSize)

	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint32(binaryData[offset:offset+4], uint32TotalSize)
	offset += 4

	// Write page group count
	uint32PageGroupCount, err := utils.SafeIntToUint32(pageGroupCount)

	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint32(binaryData[offset:offset+4], uint32PageGroupCount)
	offset += 4

	// Write page groups
	for pageGroupNumber, pageGroupVersions := range pli.pageGroups {
		uint64PageGroupNumber, err := utils.SafeInt64ToUint64(int64(pageGroupNumber))

		if err != nil {
			return err
		}

		binary.LittleEndian.PutUint64(binaryData[offset:offset+8], uint64PageGroupNumber)
		offset += 8

		// Calculate and write length of page group data
		pageGroupDataLength := 0

		for _, pageNumbers := range pageGroupVersions {
			pageGroupDataLength += 8 + 4 + len(pageNumbers)*8
		}

		uint32PageGroupDataLength, err := utils.SafeIntToUint32(pageGroupDataLength)

		if err != nil {
			return err
		}

		binary.LittleEndian.PutUint32(binaryData[offset:offset+4], uint32PageGroupDataLength)
		offset += 4

		// Write page groups
		for pageGroupVersionNumber, pages := range pageGroupVersions {
			uint64PageGroupVersionNumber, err := utils.SafeInt64ToUint64(int64(pageGroupVersionNumber))

			if err != nil {
				return err
			}

			binary.LittleEndian.PutUint64(binaryData[offset:offset+8], uint64PageGroupVersionNumber)
			offset += 8

			// Write length of version data
			versionDataLength := len(pages) * 8
			uint32VersionDataLength, err := utils.SafeIntToUint32(versionDataLength)

			if err != nil {
				return err
			}

			binary.LittleEndian.PutUint32(binaryData[offset:offset+4], uint32VersionDataLength)
			offset += 4

			// Write versions
			for _, pageNumber := range pages {
				uint64PageNumber, err := utils.SafeInt64ToUint64(int64(pageNumber))

				if err != nil {
					return err
				}

				binary.LittleEndian.PutUint64(binaryData[offset:offset+8], uint64PageNumber)

				offset += 8
			}
		}
	}

	// Write to file
	file := pli.File()

	if file == nil {
		return errors.New("failed to get file")
	}

	err = file.Truncate(0)

	if err != nil {
		return err
	}

	_, err = file.Seek(0, io.SeekStart)

	if err != nil {
		return err
	}

	_, err = file.Write(binaryData)

	return err
}
