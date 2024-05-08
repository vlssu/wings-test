package filesystem

import (
	"context"
	"fmt"
	"io"
	iofs "io/fs"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"emperror.dev/errors"
	"github.com/klauspost/compress/zip"
	"github.com/mholt/archiver/v4"

	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"

	"github.com/pterodactyl/wings/internal/ufs"
	"github.com/pterodactyl/wings/server/filesystem/archiverext"
)

// CompressFiles compresses all the files matching the given paths in the
// specified directory. This function also supports passing nested paths to only
// compress certain files and folders when working in a larger directory. This
// effectively creates a local backup, but rather than ignoring specific files
// and folders, it takes an allow-list of files and folders.
//
// All paths are relative to the dir that is passed in as the first argument,
// and the compressed file will be placed at that location named
// `archive-{date}.tar.gz`.
func (fs *Filesystem) CompressFiles(dir string, paths []string) (ufs.FileInfo, error) {
	a := &Archive{Filesystem: fs, BaseDirectory: dir, Files: paths}
	d := path.Join(
		dir,
		fmt.Sprintf("archive-%s.tar.gz", strings.ReplaceAll(time.Now().Format(time.RFC3339), ":", "")),
	)
	f, err := fs.unixFS.OpenFile(d, ufs.O_WRONLY|ufs.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	cw := ufs.NewCountedWriter(f)
	if err := a.Stream(context.Background(), cw); err != nil {
		return nil, err
	}
	if !fs.unixFS.CanFit(cw.BytesWritten()) {
		_ = fs.unixFS.Remove(d)
		return nil, newFilesystemError(ErrCodeDiskSpace, nil)
	}
	fs.unixFS.Add(cw.BytesWritten())
	return f.Stat()
}

func (fs *Filesystem) archiverFileSystem(ctx context.Context, p string) (iofs.FS, error) {
	f, err := fs.unixFS.Open(p)
	if err != nil {
		return nil, err
	}
	// Do not use defer to close `f`, it will likely be used later.

	format, _, err := archiver.Identify(filepath.Base(p), f)
	if err != nil && !errors.Is(err, archiver.ErrNoMatch) {
		_ = f.Close()
		return nil, err
	}

	// Reset the file reader.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	if format != nil {
		switch ff := format.(type) {
		case archiver.Zip:
			// zip.Reader is more performant than ArchiveFS, because zip.Reader caches content information
			// and zip.Reader can open several content files concurrently because of io.ReaderAt requirement
			// while ArchiveFS can't.
			// zip.Reader doesn't suffer from issue #330 and #310 according to local test (but they should be fixed anyway)
			return zip.NewReader(f, info.Size())
		case archiver.Archival:
			return archiver.ArchiveFS{Stream: io.NewSectionReader(f, 0, info.Size()), Format: ff, Context: ctx}, nil
		case archiver.Compression:
			return archiverext.FileFS{File: f, Compression: ff}, nil
		}
	}
	_ = f.Close()
	return nil, archiver.ErrNoMatch
}

// SpaceAvailableForDecompression looks through a given archive and determines
// if decompressing it would put the server over its allocated disk space limit.
func (fs *Filesystem) SpaceAvailableForDecompression(ctx context.Context, dir string, file string) error {
	// Don't waste time trying to determine this if we know the server will have the space for
	// it since there is no limit.
	if fs.MaxDisk() <= 0 {
		return nil
	}

	fsys, err := fs.archiverFileSystem(ctx, filepath.Join(dir, file))
	if err != nil {
		if errors.Is(err, archiver.ErrNoMatch) {
			return newFilesystemError(ErrCodeUnknownArchive, err)
		}
		return err
	}

	var size atomic.Int64
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
			if !fs.unixFS.CanFit(size.Add(info.Size())) {
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
	f, err := fs.unixFS.Open(filepath.Join(dir, file))
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
		FileName:  file,
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
	return fs.extractStream(ctx, extractStreamOptions{
		Directory: dir,
		Format:    format,
		Reader:    input,
	})
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
	// See if it's a compressed archive, such as TAR or a ZIP
	ex, ok := opts.Format.(archiver.Extractor)
	if !ok {
		// 如果不是压缩包，则检查是否是单个文件压缩，比如 .log.gz、.sql.gz 等
		de, ok := opts.Format.(archiver.Decompressor)
		if !ok {
			return nil
		}

		// 去掉压缩后缀
		p := filepath.Join(opts.Directory, strings.TrimSuffix(opts.FileName, opts.Format.Name()))

		// 确保不被忽略
		if err := fs.IsIgnored(p); err != nil {
			return nil
		}

		reader, err := de.OpenReader(opts.Reader)
		if err != nil {
			return err
		}
		defer reader.Close()

		// 打开文件进行创建/写入
		f, err := fs.unixFS.OpenFile(p, ufs.O_WRONLY|ufs.O_CREATE, 0o644)
		if err != nil {
			return err
		}
		defer f.Close()

		// 以 4KB 分块读取
		buf := make([]byte, 4096)
		for {
			n, err := reader.Read(buf)
			if n > 0 {
				// 写入前检查配额
				if quotaErr := fs.HasSpaceFor(int64(n)); quotaErr != nil {
					return quotaErr
				}

				// 写入分块
				if _, writeErr := f.Write(buf[:n]); writeErr != nil {
					return writeErr
				}

				// 添加到配额
				fs.addDisk(int64(n))
			}

			if err != nil {
				// 预期的 EOF
				if err == io.EOF {
					break
				}

				// 返回任何其他错误
				return err
			}
		}

		return nil
	}

	// 解压缩并提取归档文件
	return ex.Extract(ctx, opts.Reader, nil, func(ctx context.Context, f archiver.File) error {
		if f.IsDir() {
			return nil
		}
		p := filepath.Join(opts.Directory, f.NameInArchive)
		// 如果被忽略，不处理该文件并跳过
		if err := fs.IsIgnored(p); err != nil {
			return nil
		}
		r, err := f.Open()
		if err != nil {
			return err
		}
		defer r.Close()
		// 处理中文文件名
		filePath, err := decodeGBK(p)
		if err != nil {
			return err
		}
		if err := fs.Write(filePath, r, f.Size(), f.Mode()); err != nil {
			return wrapError(err, opts.FileName)
		}
		// 更新文件修改时间为归档中设置的时间
		if err := fs.Chtimes(filePath, f.ModTime(), f.ModTime()); err != nil {
			return wrapError(err, opts.FileName)
		}
		return nil
	})
}

// 解码GBK编码的文件名
func decodeGBK(input string) (string, error) {
	decoder := simplifiedchinese.GBK.NewDecoder()
	decoded, _, err := transform.String(decoder, input)
	if err != nil {
		return "", err
	}
	return decoded, nil
}
