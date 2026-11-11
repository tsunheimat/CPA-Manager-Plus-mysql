package managerdatasnapshot

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/database"
	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
)

const restoreTokenKey = "internal.mysql.snapshot_restore_token"

var mysqlSnapshotFiles = []string{"mysql-data", "data-key", "archives"}

type mysqlJournalItem struct {
	Target, Stage, Old string
	Existed            bool
}
type mysqlJournal struct {
	Fingerprint, Token string
	Items              []mysqlJournalItem
}

func pendingPath(cfg config.Config) string { return cfg.DataKeyPath + ".restore-pending.json" }

func runMySQL(ctx context.Context, opts options, cfg config.Config, stdout io.Writer) error {
	if opts.DataKeyPath != "" {
		cfg.DataKeyPath = opts.DataKeyPath
	}
	if opts.ArchiveDir != "" {
		cfg.UsageArchiveDir = opts.ArchiveDir
	}
	lock, err := database.Acquire(ctx, &cfg)
	if err != nil {
		return err
	}
	defer lock.Close()
	db, err := mysqlrepo.OpenNative(ctx, cfg.DBDSN)
	if err != nil {
		return err
	}
	defer db.Close()
	return dispatchMySQL(ctx, opts, cfg, db, stdout, func() error { return lock.Check(ctx) }, func() error {
		initialized, err := mysqlrepo.Open(ctx, cfg.DBDSN)
		if err != nil {
			return err
		}
		return initialized.Close()
	})
}

// Keep the destination on the non-migrating administrative pool until restore
// preparation succeeds. The injected upgrade boundary also permits offline tests
// of dispatcher ordering, not just tests of the inner row restore.
func dispatchMySQL(ctx context.Context, opts options, cfg config.Config, db *sql.DB, stdout io.Writer, checkLock, upgrade func() error) error {
	if opts.Action == "recover" {
		return recoverMySQLFiles(ctx, db, cfg)
	}
	if _, err := os.Lstat(pendingPath(cfg)); err == nil {
		return errors.New("an interrupted restore must be recovered first: manager-data-snapshot recover")
	} else if !os.IsNotExist(err) {
		return err
	}
	var err error
	switch opts.Action {
	case "create":
		err = createMySQL(ctx, db, cfg, opts.SnapshotDir, checkLock)
	case "restore":
		err = restoreMySQLWithUpgrade(ctx, db, cfg, opts.SnapshotDir, checkLock, upgrade)
	default:
		return fmt.Errorf("unsupported MySQL snapshot action %q", opts.Action)
	}
	if err == nil {
		fmt.Fprintf(stdout, "MySQL Manager data snapshot %s completed.\n", opts.Action)
	}
	return err
}

func writeSnapshotPayload(ctx context.Context, path string, fn func(io.Writer) error) (manifestEntry, error) {
	out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return manifestEntry{}, err
	}
	h := sha256.New()
	writer := io.MultiWriter(out, h)
	if err := fn(writer); err != nil {
		out.Close()
		return manifestEntry{}, err
	}
	if err := ctx.Err(); err != nil {
		out.Close()
		return manifestEntry{}, err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return manifestEntry{}, err
	}
	info, err := out.Stat()
	if err != nil {
		out.Close()
		return manifestEntry{}, err
	}
	if err := out.Close(); err != nil {
		return manifestEntry{}, err
	}
	return manifestEntry{Existed: true, Mode: 0600, Size: info.Size(), SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}
func createMySQL(ctx context.Context, db *sql.DB, cfg config.Config, dir string, checkLock func() error) (returnErr error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	if cfg.UsageArchiveDir != "" {
		archiveRoot, err := filepath.Abs(cfg.UsageArchiveDir)
		if err != nil {
			return err
		}
		if abs == archiveRoot || strings.HasPrefix(abs, archiveRoot+string(os.PathSeparator)) {
			return errors.New("snapshot directory must be outside the archive directory")
		}
	}
	if _, err := os.Lstat(abs); err == nil {
		return errors.New("snapshot directory already exists")
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0700); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(filepath.Dir(abs), ".cpamp-mysql-snapshot-")
	if err != nil {
		return err
	}
	defer func() {
		if returnErr != nil {
			os.RemoveAll(tmp)
		}
	}()
	m := manifest{Version: 2, Driver: "mysql", Files: map[string]manifestEntry{}}
	m.Files["mysql-data"], err = writeSnapshotPayload(ctx, filepath.Join(tmp, "mysql-data"), func(w io.Writer) error {
		gz := gzip.NewWriter(w)
		if err := mysqlrepo.WriteBackup(ctx, db, gz); err != nil {
			gz.Close()
			return err
		}
		return gz.Close()
	})
	if err != nil {
		return err
	}
	m.Files["data-key"], err = snapshotOne(ctx, cfg.DataKeyPath, filepath.Join(tmp, "data-key"))
	if err != nil {
		return err
	}
	m.Files["archives"], err = snapshotArchives(ctx, cfg.UsageArchiveDir, filepath.Join(tmp, "archives"))
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := writeNewFile(filepath.Join(tmp, "manifest.json"), data, 0600); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkLock(); err != nil {
		return err
	}
	if err := os.Rename(tmp, abs); err != nil {
		return err
	}
	syncTargetDirs([]string{abs})
	return nil
}
func snapshotArchives(ctx context.Context, root, path string) (manifestEntry, error) {
	if root == "" {
		return manifestEntry{}, nil
	}
	info, err := os.Lstat(root)
	if os.IsNotExist(err) {
		return manifestEntry{}, nil
	}
	if err != nil {
		return manifestEntry{}, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return manifestEntry{}, errors.New("archive root must be a real directory")
	}
	return writeSnapshotPayload(ctx, path, func(w io.Writer) error {
		gz := gzip.NewWriter(w)
		tw := tar.NewWriter(gz)
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if path == root {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			if !info.IsDir() && !info.Mode().IsRegular() {
				return fmt.Errorf("archive contains a non-regular entry: %s", path)
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			h, err := tar.FileInfoHeader(info, "")
			if err != nil {
				return err
			}
			h.Name = filepath.ToSlash(rel)
			if err := tw.WriteHeader(h); err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			_, err = copyWithContext(ctx, tw, f)
			f.Close()
			return err
		})
		if err != nil {
			tw.Close()
			gz.Close()
			return err
		}
		if err := tw.Close(); err != nil {
			gz.Close()
			return err
		}
		return gz.Close()
	})
}
func checkedPayload(ctx context.Context, dir, name string, entry manifestEntry) (*os.File, error) {
	path := filepath.Join(dir, name)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("snapshot payload is not a regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	n, err := copyWithContext(ctx, h, f)
	if err != nil || n != entry.Size || hex.EncodeToString(h.Sum(nil)) != entry.SHA256 {
		f.Close()
		return nil, fmt.Errorf("snapshot integrity check failed for %s", name)
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
func stageArchives(ctx context.Context, dir string, entry manifestEntry, target string) (string, error) {
	f, err := checkedPayload(ctx, dir, "archives", entry)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		return "", err
	}
	tmp, err := os.MkdirTemp(filepath.Dir(target), ".cpamp-restore-archives-")
	if err != nil {
		return "", err
	}
	ok := false
	defer func() {
		if !ok {
			os.RemoveAll(tmp)
		}
	}()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return "", err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", err
		}
		if !filepath.IsLocal(h.Name) || strings.Contains(h.Name, "\\") {
			return "", errors.New("unsafe archive entry")
		}
		path := filepath.Join(tmp, filepath.FromSlash(h.Name))
		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(path, 0700); err != nil {
				return "", err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				return "", err
			}
			out, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				return "", err
			}
			_, err = copyWithContext(ctx, out, tr)
			syncErr := out.Sync()
			closeErr := out.Close()
			if err != nil {
				return "", err
			}
			if syncErr != nil {
				return "", syncErr
			}
			if closeErr != nil {
				return "", closeErr
			}
		default:
			return "", errors.New("unsupported archive entry")
		}
	}
	// Drain the gzip trailer so CRC/truncation failures are not hidden by tar EOF.
	if _, err := io.Copy(io.Discard, gz); err != nil {
		return "", err
	}
	ok = true
	return tmp, nil
}
func mysqlFingerprint(ctx context.Context, db *sql.DB) (string, error) {
	var server, name string
	err := db.QueryRowContext(ctx, "select @@server_uuid,database()").Scan(&server, &name)
	sum := sha256.Sum256([]byte(server + "/" + name))
	return hex.EncodeToString(sum[:]), err
}
func restoreMySQL(ctx context.Context, db *sql.DB, cfg config.Config, dir string, checkLock func() error) error {
	return restoreMySQLWithUpgrade(ctx, db, cfg, dir, checkLock, func() error { return nil })
}

func restoreMySQLWithUpgrade(ctx context.Context, db *sql.DB, cfg config.Config, dir string, checkLock, upgrade func() error) error {
	abs, m, err := loadManifest(dir)
	if err != nil {
		return err
	}
	if m.Driver != "mysql" {
		return errors.New("snapshot is not a MySQL backup")
	}
	// All payloads are verified before changing MySQL or any live file.
	for name, entry := range m.Files {
		if entry.Existed {
			f, err := checkedPayload(ctx, abs, name, entry)
			if err != nil {
				return err
			}
			f.Close()
		}
	}
	j := mysqlJournal{}
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	j.Token = hex.EncodeToString(token[:])
	cleanupStages := true
	defer func() {
		if cleanupStages {
			for _, item := range j.Items {
				if item.Stage != "" {
					os.RemoveAll(item.Stage)
				}
			}
		}
	}()
	for _, spec := range []struct {
		name, target string
		directory    bool
	}{{"data-key", cfg.DataKeyPath, false}, {"archives", cfg.UsageArchiveDir, true}} {
		if spec.target == "" {
			return errors.New("restore requires data key and archive paths")
		}
		target, err := filepath.Abs(spec.target)
		if err != nil {
			return err
		}
		if target == filepath.Dir(target) {
			return errors.New("cannot restore over a filesystem root")
		}
		info, err := os.Lstat(target)
		existed := err == nil
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if existed && (info.Mode()&os.ModeSymlink != 0 || spec.directory != info.IsDir() || (!spec.directory && !info.Mode().IsRegular())) {
			return errors.New("restore target has an unexpected file type")
		}
		item := mysqlJournalItem{Target: target, Existed: existed}
		if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
			return err
		}
		item.Old, err = reserveRollbackSlot(target)
		if err != nil {
			return err
		}
		entry := m.Files[spec.name]
		if entry.Existed {
			if spec.directory {
				item.Stage, err = stageArchives(ctx, abs, entry, target)
			} else {
				item.Stage, err = stageRestoreFile(ctx, filepath.Join(abs, spec.name), target, entry)
			}
			if err != nil {
				return err
			}
		}
		j.Items = append(j.Items, item)
	}
	// Prevent overlapping paths from making a file-set rollback ambiguous.
	if a, b := j.Items[0].Target, j.Items[1].Target; a == b || strings.HasPrefix(a, b+string(os.PathSeparator)) || strings.HasPrefix(b, a+string(os.PathSeparator)) {
		return errors.New("data key and archive targets overlap")
	}
	f, err := checkedPayload(ctx, abs, "mysql-data", m.Files["mysql-data"])
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	prepared, err := mysqlrepo.PrepareRestore(ctx, gz)
	if err != nil {
		return err
	}
	defer prepared.Close()
	// All payload integrity, archive extraction and row-stream structure checks
	// precede upgrade DDL. Only private stages exist; no journal/live file has
	// been published. Restore consumes this same validated spool after upgrading.
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkLock(); err != nil {
		return err
	}
	j.Fingerprint, err = mysqlFingerprint(ctx, db)
	if err != nil {
		return err
	}
	if err := upgrade(); err != nil {
		return err
	}
	restore, err := prepared.BeginRestore(ctx, db)
	if err != nil {
		return err
	}
	defer restore.Close()
	if _, err := restore.Tx.ExecContext(ctx, "insert into settings(`key`,value,updated_at_ms) values(?,?,0) on duplicate key update value=values(value)", restoreTokenKey, j.Token); err != nil {
		return err
	}
	if err := checkLock(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	if err := writeNewFile(pendingPath(cfg), data, 0600); err != nil {
		return err
	}
	syncTargetDirs([]string{pendingPath(cfg)})
	cleanupStages = false
	for _, item := range j.Items {
		if item.Existed {
			if err := os.Rename(item.Target, item.Old); err != nil {
				_ = restore.Tx.Rollback()
				return errors.Join(err, recoverMySQLFiles(context.Background(), db, cfg))
			}
		}
		if item.Stage != "" {
			if err := os.Rename(item.Stage, item.Target); err != nil {
				_ = restore.Tx.Rollback()
				return errors.Join(err, recoverMySQLFiles(context.Background(), db, cfg))
			}
		}
	}
	var targets []string
	for _, item := range j.Items {
		targets = append(targets, item.Target)
	}
	syncTargetDirs(targets)
	if err := checkLock(); err != nil {
		_ = restore.Tx.Rollback()
		return errors.Join(err, recoverMySQLFiles(context.Background(), db, cfg))
	}
	err = restore.Tx.Commit()
	committedToken, tokenErr := readRestoreToken(context.Background(), db)
	if tokenErr != nil {
		return fmt.Errorf("restore outcome is unresolved; run manager-data-snapshot recover: %w", errors.Join(err, tokenErr))
	}
	recoveryErr := recoverMySQLFiles(context.Background(), db, cfg)
	if recoveryErr != nil {
		return fmt.Errorf("restore outcome needs recovery before startup: %w", errors.Join(err, recoveryErr))
	}
	if committedToken == j.Token {
		return nil
	}
	return err
}

// A locking read waits for an uncertain COMMIT/rollback to finish. A plain
// snapshot read could misclassify an in-flight commit after connection loss.
func readRestoreToken(ctx context.Context, db *sql.DB) (string, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var token string
	err = tx.QueryRowContext(ctx, "select value from settings where `key`=? for update", restoreTokenKey).Scan(&token)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	return token, tx.Commit()
}
func recoverMySQLFiles(ctx context.Context, db *sql.DB, cfg config.Config) error {
	path := pendingPath(cfg)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var j mysqlJournal
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	fingerprint, err := mysqlFingerprint(ctx, db)
	if err != nil {
		return err
	}
	if fingerprint != j.Fingerprint {
		return errors.New("pending restore belongs to a different MySQL database")
	}
	token, err := readRestoreToken(ctx, db)
	if err != nil {
		return err
	}
	committed := token == j.Token
	for _, item := range j.Items {
		if !committed {
			if _, err := os.Lstat(item.Old); err == nil {
				if err := os.RemoveAll(item.Target); err != nil {
					return err
				}
				if err := os.Rename(item.Old, item.Target); err != nil {
					return err
				}
			} else if !os.IsNotExist(err) {
				return err
			} else if !item.Existed {
				if err := os.RemoveAll(item.Target); err != nil {
					return err
				}
			}
		} else {
			if err := os.RemoveAll(item.Old); err != nil {
				return err
			}
		}
		if item.Stage != "" {
			if err := os.RemoveAll(item.Stage); err != nil {
				return err
			}
		}
	}
	var targets []string
	for _, item := range j.Items {
		targets = append(targets, item.Target)
	}
	syncTargetDirs(targets)
	if err := os.Remove(path); err != nil {
		return err
	}
	syncTargetDirs([]string{path})
	return nil
}
