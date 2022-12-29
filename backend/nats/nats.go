// Package nats provides an interface to the Nats Jestream object storage system.
package nats

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/hash"

	"github.com/tidwall/btree"
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "Nats",
		Description: "Nats Jetstream Object Storage",
		NewFs:       NewFs,
		Options:     []fs.Option{},
	})
}

type Options struct{}

// Fs represents a remote nats server
type Fs struct {
	name string  // name of this remote
	root string  // the path we are working on if any
	opt  Options // parsed config options

	features *fs.Features
	ci       *fs.ConfigInfo

	store nats.ObjectStore

	objects btree.Map[string, *Object]
}

// Object describes a nats object
type Object struct {
	fs   *Fs // what this object is part of
	info *nats.ObjectInfo
}

func (o *Object) String() string {
	return o.Remote()
}

func (o *Object) Remote() string {
	if o.info.Name == o.fs.root {
		return path.Base(o.info.Name)
	}

	return strings.TrimPrefix(strings.TrimPrefix(o.info.Name, o.fs.root), "/")
}

func (o *Object) oremote() string {
	return o.info.Name
}

func (o *Object) ModTime(_ context.Context) time.Time {
	return o.info.ModTime
}

func (o *Object) Size() int64 {
	return int64(o.info.Size)
}

// ------------------------------------------------------------

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("Nats bucket %s", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// NewFs constructs an Fs from the path, bucket:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	ci := fs.GetConfig(ctx)

	//nc, err := nats.Connect(nats.DefaultURL)
	nc, err := nats.Connect("nats://raspberrypi4-keller:4222")
	if err != nil {
		return nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	obs, err := js.ObjectStore("data")
	if err != nil {
		return nil, err
	}

	f := &Fs{
		name:  name,
		opt:   *opt,
		ci:    ci,
		store: obs,
		root:  root,
	}

	done := make(chan bool)
	ow, err := obs.Watch()
	if err != nil {
		return nil, err
	}
	defer ow.Stop()

	go func() {
		for i := range ow.Updates() {
			if i == nil {
				done <- true
				continue
			}

			if i.Deleted {
				f.objects.Delete(i.Name)
				continue
			}

			f.objects.Set(i.Name, &Object{
				fs:   f,
				info: i,
			})
		}
	}()

	<-done

	f.features = (&fs.Features{
		BucketBased:       true,
		BucketBasedRootOK: true,
	}).Fill(ctx, f)

	return f, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	fmt.Println("NewObject")
	or, err := f.store.GetInfo(remote, nats.Context(ctx))
	if err != nil {
		if errors.Is(err, nats.ErrObjectNotFound) {
			return nil, fs.ErrorObjectNotFound
		}

		return nil, err
	}

	return &Object{
		fs:   f,
		info: or,
	}, nil
}

// List the objects and directories in dir into entries. The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	set := make(map[string]struct{})
	dir = path.Join(f.root, dir)

	if dir != "" {
		value, ok := f.objects.Get(dir)
		if ok {
			entries = append(entries, value)
			return entries, nil
		}

		dir += "/"
	}

	f.objects.Ascend(dir, func(key string, value *Object) bool {
		if dir != "" && !strings.HasPrefix(key, dir) {
			return false
		}

		pd, _, found := strings.Cut(strings.TrimPrefix(key, dir), "/")
		if found {
			_, ok := set[pd]
			if !ok {
				d := fs.NewDir(strings.TrimPrefix(path.Join(dir, pd), f.root+"/"), time.Now())
				entries = append(entries, d)
				set[pd] = struct{}{}
			}
			return true
		}

		entries = append(entries, value)
		return true
	})

	return entries, nil
}

// Put the object into the bucket
//
// Copy the reader in to the new object which is returned.
//
// The new object may have been created if an error is returned
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	info := &nats.ObjectMeta{
		Name: path.Join(f.root, src.Remote()),
	}

	oi, err := f.store.Put(info, in, nats.Context(ctx))
	if err != nil {
		return nil, err
	}

	// Temporary Object under construction
	fs := &Object{
		fs:   f,
		info: oi,
	}
	return fs, nil
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}

// Mkdir creates the bucket if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return nil
}

// Rmdir deletes the bucket if the fs is at the root
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return nil
}

// Precision of the remote
func (f *Fs) Precision() time.Duration {
	return time.Millisecond
}

// Copy src to this remote using server-side copy operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	return nil, fs.ErrorCantCopy
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.SHA256)
}

// ------------------------------------------------------------

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Hash returns the Sha256 of an object returning a lowercase hex string
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t != hash.SHA256 {
		return "", hash.ErrUnsupported
	}

	digest, err := nats.DecodeObjectDigest(o.info.Digest)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(digest), nil
}

// SetModTime sets the modification time of the Object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}

// Storable returns if this object is storable
func (o *Object) Storable() bool {
	return true
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	for _, option := range options {
		switch x := option.(type) {
		case *fs.SeekOption:
			fmt.Println("OFFSET", x.Offset)
		case *fs.RangeOption:
			offset, limit := x.Decode(o.Size())
			fmt.Println("OFFSET LIMIT", offset, limit)
		default:
			if option.Mandatory() {
				fs.Logf(o, "Unsupported mandatory option: %v", option)
			}
		}
	}

	return o.fs.store.Get(o.oremote(), nats.Context(ctx))
}

// Update the object with the contents of the io.Reader, modTime and size
//
// The new object may have been created if an error is returned
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	info := &nats.ObjectMeta{
		Name: o.oremote(),
	}

	oi, err := o.fs.store.Put(info, in, nats.Context(ctx))
	if err != nil {
		return err
	}

	o.info = oi
	return nil
}

// Remove an object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.store.Delete(o.oremote())
}

// MimeType of an Object if known, "" otherwise
func (o *Object) MimeType(ctx context.Context) string {
	return ""
}

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string {
	return o.info.NUID
}

// Check the interfaces are satisfied
var (
	_ fs.Fs          = &Fs{}
	_ fs.Copier      = &Fs{}
	_ fs.PutStreamer = &Fs{}
	_ fs.Object      = &Object{}
	_ fs.MimeTyper   = &Object{}
	_ fs.IDer        = &Object{}
)
