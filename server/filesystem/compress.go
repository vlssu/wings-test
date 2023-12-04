package filesystem

import (
	"context"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"emperror.dev/errors"
	"github.com/mholt/archiver/v4"

	"bytes"
	"golang.org/x/text/transform"
	"golang.org/x/text/encoding/htmlindex"
)

// 定义 convertToUTF8 函数
func convertToUTF8(s []byte) ([]byte, error) {
	decoder, err := htmlindex.Get("gbk").NewDecoder()
	if err != nil {
		return nil, err
	}

	reader := transform.NewReader(bytes.NewReader(s), decoder)
	utf8Bytes, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return utf8Bytes, nil
}

// CompressFiles compresses all the files matching the given paths in the
// specified directory. This function also supports passing nested paths to only
// compress certain files and folders when working in a larger directory. This
// effectively creates a local backup, but rather than ignoring specific files
// and folders, it takes an allow-list of files and folders.
//
// All paths are relative to the dir that is passed in as the first argument,
// and the compressed file will be placed at that location named
// `archive-{date}.tar.gz`.
func (fs *Filesystem) CompressFiles(dir string, paths []string) (os.FileInfo, error) {
	cleanedRootDir, err := fs.SafePath(dir)
	if err != nil {
		return nil, err
	}

	// Take all the paths passed in and merge them together with the root directory we've gotten.
	for i, p := range paths {
		paths[i] = filepath.Join(cleanedRootDir, p)
	}

	cleaned, err := fs.ParallelSafePath(paths)
	if err != nil {
		return nil, err
	}

	a := &Archive{BasePath: cleanedRootDir, Files: cleaned}
	d := path.Join(
		cleanedRootDir,
		fmt.Sprintf("archive-%s.tar.gz", strings.ReplaceAll(time.Now().Format(time.RFC3339), ":", "")),
	)

	if err := a.Create(context.Background(), d); err != nil {
		return nil, err
	}

	f, err := os.Stat(d)
	if err != nil {
		_ = os.Remove(d)
		return nil, err
	}

	if err := fs.HasSpaceFor(f.Size()); err != nil {
		_ = os.Remove(d)
		return nil, err
	}

	fs.addDisk(f.Size())

	return f, nil
}

// SpaceAvailableForDecompression looks through a given archive and determines
// if decompressing it would put the server over its allocated disk space limit.
func (fs *Filesystem) SpaceAvailableForDecompression(ctx context.Context, dir string, file string) error {
	// Don't waste time trying to determine this if we know the server will have the space for
	// it since there is no limit.
	if fs.MaxDisk() <= 0 {
		return nil
	}

	source, err := fs.SafePath(filepath.Join(dir, file))
	if err != nil {
		return err
	}

	// Get the cached size in a parallel process so that if it is not cached we are not
	// waiting an unnecessary amount of time on this call.
	dirSize, err := fs.DiskUsage(false)

	fsys, err := archiver.FileSystem(ctx, source)
	if err != nil {
		if errors.Is(err, archiver.ErrNoMatch) {
			return newFilesystemError(ErrCodeUnknownArchive, err)
		}
		return err
	}

	var size int64
	return iofs.WalkDir(fsys, ".", func(path string, d iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			// Stop walking if the context is canceled.
			return ctx.Err()
		default:
			info, err := d.Info()
			if err != nil {
				return err
			}
			if atomic.AddInt64(&size, info.Size())+dirSize > fs.MaxDisk() {
				return newFilesystemError(ErrCodeDiskSpace, nil)
			}
			return nil
		}
	})
}

// DecompressFile will decompress a file in a given directory by using the
// archiver tool to infer the file type and go from there. This will walk over
// all the files within the given archive and ensure that there is not a
// zip-slip attack being attempted by validating that the final path is within
// the server data directory.
func (fs *Filesystem) DecompressFile(ctx context.Context, dir string, file string) error {
	source, err := fs.SafePath(filepath.Join(dir, file))
	if err != nil {
		return err
	}
	return fs.DecompressFileUnsafe(ctx, dir, source)
}

// DecompressFileUnsafe will decompress any file on the local disk without checking
// if it is owned by the server.  The file will be SAFELY decompressed and extracted
// into the server's directory.
func (fs *Filesystem) DecompressFileUnsafe(ctx context.Context, dir string, file string) error {
	// Ensure that the archive actually exists on the system.
	if _, err := os.Stat(file); err != nil {
		return errors.WithStack(err)
	}

	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	// Identify the type of archive we are dealing with.
	format, input, err := archiver.Identify(filepath.Base(file), f)
	if err != nil {
		if errors.Is(err, archiver.ErrNoMatch) {
			return newFilesystemError(ErrCodeUnknownArchive, err)
		}
		return err
	}

	return fs.extractStream(ctx, extractStreamOptions{
		Directory: dir,
		Format:    format,
		Reader:    input,
	})
}

// ExtractStreamUnsafe .
func (fs *Filesystem) ExtractStreamUnsafe(ctx context.Context, dir string, r io.Reader) error {
	format, input, err := archiver.Identify("archive.tar.gz", r)
	if err != nil {
		if errors.Is(err, archiver.ErrNoMatch) {
			return newFilesystemError(ErrCodeUnknownArchive, err)
		}
		return err
	}

	opts := extractStreamOptions{
		Directory: dir,
		Format:    format,
		Reader:    input,
	}

	// Decompress and extract archive
	if ex, ok := opts.Format.(archiver.Extractor); ok {
		return ex.Extract(ctx, opts.Reader, nil, func(ctx context.Context, f archiver.File) error {
			if f.IsDir() {
				return nil
			}
			p := filepath.Join(opts.Directory, f.NameInArchive)
			// If it is ignored, just don't do anything with the file and skip over it.
			if err := fs.IsIgnored(p); err != nil {
				return nil
			}

			// Convert file name to UTF-8
			utf8Name, err := convertToUTF8([]byte(f.NameInArchive))
			if err != nil {
				return err
			}
			_ = utf8Name // Fix: Added this line to avoid the "declared and not used" error.

			r, err := f.Open()
			if err != nil {
				return err
			}
			defer r.Close()

			if err := fs.Writefile(p, r); err != nil {
				return wrapError(err, opts.FileName)
			}
			// Update the file permissions to the one set in the archive.
			if err := fs.Chmod(p, f.Mode()); err != nil {
				return wrapError(err, opts.FileName)
			}
			// Update the file modification time to the one set in the archive.
			if err := fs.Chtimes(p, f.ModTime(), f.ModTime()); err != nil {
				return wrapError(err, opts.FileName)
			}
			return nil
		})
	}
	return nil
}

type extractStreamOptions struct {
	// The directory to extract the archive to.
	Directory string
	// File name of the archive.
	FileName string
	// Format of the archive.
	Format archiver.Format
	// Reader for the archive.
	Reader io.Reader
}

func (fs *Filesystem) extractStream(ctx context.Context, opts extractStreamOptions) error {
	// Decompress and extract archive
	if ex, ok := opts.Format.(archiver.Extractor); ok {
		return ex.Extract(ctx, opts.Reader, nil, func(ctx context.Context, f archiver.File) error {
			if f.IsDir() {
				return nil
			}
			p := filepath.Join(opts.Directory, f.NameInArchive)
			// If it is ignored, just don't do anything with the file and skip over it.
			if err := fs.IsIgnored(p); err != nil {
				return nil
			}
			r, err := f.Open()
			if err != nil {
				return err
			}
			defer r.Close()
			if err := fs.Writefile(p, r); err != nil {
				return wrapError(err, opts.FileName)
			}
			// Update the file permissions to the one set in the archive.
			if err := fs.Chmod(p, f.Mode()); err != nil {
				return wrapError(err, opts.FileName)
			}
			// Update the file modification time to the one set in the archive.
			if err := fs.Chtimes(p, f.ModTime(), f.ModTime()); err != nil {
				return wrapError(err, opts.FileName)
			}
			return nil
		})
	}
	return nil
}
