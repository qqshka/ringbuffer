package ringbuffer

import (
	"context"
	"errors"
	"io"
	"log"
	"reflect"
	"runtime"
	"unsafe"
)

var (
	ErrTooManyDataToWrite = errors.New("too many data to write")
	ErrIsEmpty            = errors.New("ringbuffer is empty")
	ErrAcquireLock        = errors.New("no lock to acquire")
	ErrNotEnoughData      = errors.New("not enough data")
	ErrReadFromInProgress = errors.New("ReadFrom in progress")
	ErrWriteToInProgress  = errors.New("WriteTo in progress")
)

type RingBuffer struct {
	buf  []byte
	size int // Capacity of underlying buffer.
	l    int // Available bytes to read.
	r    int // Next position to read.
	w    int // Next position to write.

	isRF             bool             // Is ReadFrom() in progress.
	isWT             bool             // Is WriteTo() in progress.
	isRCNotifyBlocks bool             // is WriteTo notify blocking, useful if you want to controll how frequently data is written to supplied io.Writer.
	isWCNotifyBlocks bool             // is ReadFrom() notify blocking, useful if you want to controll how frequently data is read from supplied io.Reader.
	WC               chan interface{} // ReadFrom() notify about new data with this channel. (Public)
	RC               chan interface{} // WriteTo() notify about releasing space with this channel. (Public)
	rC               chan interface{} // Readers notify about releasing space with this channel.
	wC               chan interface{} // Writers notify about new data with this channel.

	mu Mutex
}

// New returns a new RingBuffer whose buffer has the given size.
func New(size int) *RingBuffer {
	return NewExtended(size, false, false)
}

// New returns a new RingBuffer whose buffer has the given size.
// Also you can specify if notify is blocking for ReadFrom and WriteTo.
func NewExtended(size int, isRCNotifyBlocks bool, isWCNotifyBlocks bool) *RingBuffer {
	return &RingBuffer{
		buf:              make([]byte, size*2), // Double the size.
		size:             size,
		isRCNotifyBlocks: isRCNotifyBlocks,
		isWCNotifyBlocks: isWCNotifyBlocks,
		WC:               make(chan interface{}, 1),
		RC:               make(chan interface{}, 1),
		rC:               make(chan interface{}, 1),
		wC:               make(chan interface{}, 1),
	}
}

// Reset the read pointer and writer pointer to zero.
func (r *RingBuffer) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isRF {
		log.Panic("reseting buffer while incomplete ReadFrom()")
	}

	if r.isWT {
		log.Panic("reseting buffer while incomplete WriteTo()")
	}

	r.l = 0
	r.r = 0
	r.w = 0

	// Clear pending channels data, if any.
	for {
		select {
		case <-r.WC:
		case <-r.RC:
		case <-r.rC:
		case <-r.wC:
		default:
			return
		}
	}
}

func (r *RingBuffer) noticeFromRead() {
	// Non blocking.
	select {
	case r.rC <- nil:
	default:
	}
}

func (r *RingBuffer) noticeFromWriteTo(ctx context.Context) error {
	// If we could notify without blocking - do it.
	select {
	case r.RC <- nil:
	default:
		if r.isRCNotifyBlocks {
			select {
			case r.RC <- nil:
			case <-ctx.Done():
			}
		}
	}

	return ctx.Err()
}

func (r *RingBuffer) noticeFromWrite() {
	// Non blocking.
	select {
	case r.wC <- nil:
	default:
	}
}

func (r *RingBuffer) noticeFromReadFrom(ctx context.Context) error {
	// If we could notify without blocking - do it.
	select {
	case r.WC <- nil:
	default:
		if r.isWCNotifyBlocks {
			select {
			case r.WC <- nil:
			case <-ctx.Done():
			}
		}
	}

	return ctx.Err()
}

// Lock locks mutex.
// Should ONLY be used with functions like BytesNoLock().
func (r *RingBuffer) Lock() {
	r.mu.Lock()
}

// TryLock attempts to lock mutex, returns true if succeed.
// Should ONLY be used with functions like BytesNoLock().
func (r *RingBuffer) TryLock() bool {
	return r.mu.TryLock()
}

// Unlock unlocks mutex.
// Should ONLY be used with functions like BytesNoLock().
func (r *RingBuffer) Unlock() {
	r.mu.Unlock()
}

// Capacity returns the size of the underlying buffer.
func (r *RingBuffer) Capacity() int {
	return r.size
}

// Length returns the length of available read bytes.
func (r *RingBuffer) Length() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.l
}

// IsEmpty returns true if there is nothing to read.
func (r *RingBuffer) IsEmpty() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.l == 0
}

// Available returns the length of available bytes to write.
func (r *RingBuffer) Available() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.available()
}

func (r *RingBuffer) available() int {
	return r.size - r.l
}

// IsFull returns true if there is no more space for write.
func (r *RingBuffer) IsFull() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.available() == 0
}

// Converting string to bytes without allocating memory.
func stringToBytes(s string) []byte {
	// create an actual slice
	bytes := make([]byte, 0, 0)

	// create the string and slice headers by casting. Obtain pointers to the
	// headers to be able to change the slice header properties in the next step
	stringHeader := (*reflect.StringHeader)(unsafe.Pointer(&s))

	sliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))

	// set the slice's length and capacity temporarily to zero (this is actually
	// unnecessary here because the slice is already initialized as zero, but if
	// you are reusing a different slice this is important
	sliceHeader.Len = 0
	sliceHeader.Cap = 0

	// change the slice header data address
	sliceHeader.Data = stringHeader.Data

	// set the slice capacity and length to the string length
	sliceHeader.Cap = stringHeader.Len
	sliceHeader.Len = stringHeader.Len

	// use the keep alive dummy function to make sure the original string s is not
	// freed up until this point
	runtime.KeepAlive(s)

	return bytes
}

// WriteString writes the contents of the string s to buffer, which accepts a slice of bytes.
func (r *RingBuffer) WriteString(s string) (n int, err error) {
	return r.Write(stringToBytes(s))
}

// TryWriteString writes the contents of the string s to buffer, which accepts a slice of bytes.
// It is not blocking.
// If it has not succeeded to acquire the lock, it return 0 as n and ErrAcquireLock.
func (r *RingBuffer) TryWriteString(s string) (n int, err error) {
	return r.TryWrite(stringToBytes(s))
}

// Write writes len(p) bytes from p to the underlying buf.
// It returns the number of bytes written from p (0 <= n <= len(p)) and any error encountered that caused the write to stop early.
// Write returns a non-nil error if it returns n < len(p).
// Write must not modify the slice data, even temporarily.
func (r *RingBuffer) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.write(p, false)
}

// Same as Write but we do not perform short write, we write all or nothing.
func (r *RingBuffer) WriteFull(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.write(p, true)
}

// TryWrite writes len(p) bytes from p to the underlying buf like Write, but it is not blocking.
// If it has not succeeded to acquire the lock, it return 0 as n and ErrAcquireLock.
func (r *RingBuffer) TryWrite(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	ok := r.mu.TryLock()
	if !ok {
		return 0, ErrAcquireLock
	}
	defer r.mu.Unlock()

	return r.write(p, false)
}

func (r *RingBuffer) write(p []byte, full bool) (n int, err error) {
	if r.isRF {
		return 0, ErrReadFromInProgress
	}

	avail := r.available()

	if avail == 0 {
		return 0, ErrTooManyDataToWrite
	}

	n = len(p) // How much bytes we want to write.

	// Check if we have enough space for write.
	if n > avail {
		err = ErrTooManyDataToWrite
		// If full is true then we do not perform short write, we write all or nothing.
		if full {
			return 0, err
		}
		n = avail
		p = p[:n]
	}

	for n_tmp := n; n_tmp > 0; {
		bytesToWrite := r.size - r.w
		if bytesToWrite > n_tmp {
			bytesToWrite = n_tmp
		}

		copy(r.buf[r.w:], p[:bytesToWrite])
		copy(r.buf[r.w+r.size:], p[:bytesToWrite])
		r.w = (r.w + bytesToWrite) % r.size
		r.l += bytesToWrite

		p = p[bytesToWrite:]
		n_tmp -= bytesToWrite
	}

	r.noticeFromWrite() // Let WriteTo know what we got some data.

	return n, err
}

// ReadFrom reads data from rd until EOF or error.
// The return value n is the number of bytes read.
// Any error except EOF encountered during the read is also returned.
func (r *RingBuffer) ReadFromWithContext(ctx context.Context, rd io.Reader) (int64, error) {
	r.mu.Lock()

	if r.isRF {
		defer r.mu.Unlock()
		return 0, ErrReadFromInProgress
	}

	r.isRF = true // Let other writers know they can't do anything until we done.

	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		r.isRF = false // Release isRF when we finally leave function.
		r.mu.Unlock()
	}()

	var len int64

	for {
		avail := r.Available()
		// Check if we have space for writing.
		if avail == 0 {
			// We does not have space, wait for reader to release some space or context cancelation.
			select {
			case <-r.rC:
				continue // Reader possibly released space, lets check it.
			case <-ctx.Done():
				return len, ctx.Err()
			}
		}

		// We access 'r.w' without lock since other writers should not touch it because r.isRF is still true.
		wrap := r.size - r.w // How much bytes we can write before wrapping.
		if wrap > avail {
			wrap = avail // Cap wrap by available bytes.
		}

		b := r.buf[r.w : r.w+wrap] // Buffer for read.
		n, rErr := rd.Read(b)
		copy(r.buf[r.w+r.size:], b[:n]) // Copy what we read to the second part of underlying buffer.

		len += int64(n)

		r.mu.Lock()

		r.w = (r.w + n) % r.size
		r.l += n

		r.noticeFromWrite() // Let WriteTo know what we got some data.

		r.mu.Unlock()

		// May block.
		if err := r.noticeFromReadFrom(ctx); err != nil {
			return len, err
		}

		if rErr == io.EOF {
			return len, nil
		} else if rErr != nil {
			return len, rErr
		}
	}
}

// ReadFrom reads data from rd until EOF or error.
// The return value n is the number of bytes read.
// Any error except EOF encountered during the read is also returned.
func (r *RingBuffer) ReadFrom(rd io.Reader) (int64, error) {
	return r.ReadFromWithContext(context.Background(), rd)
}

// Read reads up to len(p) bytes into p. It returns the number of bytes read (0 <= n <= len(p))
// and any error encountered. Even if Read returns n < len(p), it may use all of p as scratch space during the call.
// If some data is available but not len(p) bytes, Read conventionally returns what is available instead of waiting for more.
// When Read encounters an error or end-of-file condition after successfully reading n > 0 bytes,
// it returns the number of bytes read. It may return the (non-nil) error from the same call or
// return the error (and n == 0) from a subsequent call.
// Callers should always process the n > 0 bytes returned before considering the error err.
// Doing so correctly handles I/O errors that happen after reading some bytes and also both of the allowed EOF behaviors.
func (r *RingBuffer) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.read(p)
}

// TryRead read up to len(p) bytes into p like Read but it is not blocking.
// If it has not succeeded to acquire the lock, it return 0 as n and ErrAcquireLock.
func (r *RingBuffer) TryRead(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	ok := r.mu.TryLock()
	if !ok {
		return 0, ErrAcquireLock
	}
	defer r.mu.Unlock()

	return r.read(p)
}

func (r *RingBuffer) read(p []byte) (n int, err error) {
	if r.isWT {
		return 0, ErrWriteToInProgress
	}

	if r.l == 0 {
		return 0, ErrIsEmpty
	}

	n = r.l
	if n > len(p) {
		n = len(p) // Read no more than user requested with his buffer.
	}
	// We do not have to do wrapping for reading since we use two times more memory than usual ring buffer,
	// so it simple as this.
	copy(p, r.buf[r.r:r.r+n])
	r.r = (r.r + n) % r.size
	r.l -= n

	r.noticeFromRead() // Let ReadFrom know we release some space for writing.

	return n, err
}

// Discard skips the next n bytes for reading.
func (r *RingBuffer) Discard(n int) (err error) {
	if n == 0 {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isWT {
		return ErrWriteToInProgress
	}

	if r.l < n {
		return ErrNotEnoughData
	}

	r.r = (r.r + n) % r.size
	r.l -= n

	r.noticeFromRead() // Let ReadFrom know we release some space for writing.

	return nil
}

func (r *RingBuffer) WriteToWithContext(ctx context.Context, wr io.Writer) (int64, error) {
	r.mu.Lock()

	if r.isWT {
		defer r.mu.Unlock()
		return 0, ErrWriteToInProgress
	}

	r.isWT = true // Let other readers know they can't do anything until we done.

	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		r.isWT = false // Release isWT when we finally leave function.
		r.mu.Unlock()
	}()

	var len int64

	for {
		l := r.Length()
		// Check if we have data for reading.
		if l == 0 {
			// We does not have data, wait for writer or context cancelation.
			select {
			case <-r.wC:
				continue // Writer tell us what we have data, lets check it.
			case <-ctx.Done():
				return len, ctx.Err()
			}
		}

		// We access 'r.r' without lock since other readers should not touch it because r.isWT is still true.
		b := r.buf[r.r : r.r+l]
		n, wErr := wr.Write(b)

		len += int64(n)

		r.mu.Lock()

		r.r = (r.r + n) % r.size
		r.l -= n

		r.noticeFromRead() // Let ReadFrom know we release some space for writing.

		r.mu.Unlock()

		// May block.
		if err := r.noticeFromWriteTo(ctx); err != nil {
			return len, err
		}

		if wErr != nil {
			return len, wErr
		}
	}
}

func (r *RingBuffer) WriteTo(wr io.Writer) (n int64, err error) {
	return r.WriteToWithContext(context.Background(), wr)
}

// Bytes returns all available read bytes. It does not move the read pointer and only copy the available data.
func (r *RingBuffer) Bytes() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()

	buf := make([]byte, r.l)
	copy(buf, r.buf[r.r:r.r+r.l])
	return buf
}

// BytesNoLock returns all available read bytes. It does not move the read pointer.
// Unlike Bytes() it does not make a copy and returns underlying buffer.
// It does not use locking so you MUST lock ringbuffer yourself!!!
func (r *RingBuffer) BytesNoLock() []byte {
	return r.buf[r.r : r.r+r.l]
}

// BytesOneReader returns all available read bytes. It does not move the read pointer.
// Unlike Bytes() it does not make a copy and returns underlying buffer.
// BytesOneReader() expects what you do NOT USE ANY Read() or Discard() while you use returned buffer.
// It is safe to use Write() while you use returned buffer.
func (r *RingBuffer) BytesOneReader() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Minor protection.
	if r.isWT {
		log.Panic(ErrWriteToInProgress)
	}

	return r.buf[r.r : r.r+r.l]
}
